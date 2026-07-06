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
import signal
import shutil
import stat
import subprocess
import sys
import tarfile
import threading
import time
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


_DIRECT_OPERATION_LOCKS_GUARD = threading.Lock()
_DIRECT_OPERATION_LOCKS: dict[str, threading.Lock] = {}

_PRODUCTION_OPERATION_RE = re.compile(
    r"\A/opt/degen/backups/config/[0-9]{8}T[0-9]{6}Z\Z"
)
_PROBE_PREFIX_RE = re.compile(
    r"\Aonedrive:backups/degen-db-probe/"
    r"(?P<operation_id>[0-9]{8}T[0-9]{6}Z)-(?P<token>[0-9a-f]{32})/\Z",
    re.ASCII,
)
_PROBE_NAMESPACE_ROOT = "onedrive:backups/"
_PROBE_NAMESPACE_PARENT = "degen-db-probe"
_PROBE_LOCAL_SOURCE_PREFIX = ".task8-probe-source-"
_TASK8_RCLONE_GROUP_RE = re.compile(
    r"\Atask8:(?P<kind>probe|dry_run|policy|observe):"
    r"(?P<started_epoch>0|[1-9][0-9]*):"
    r"(?P<attempt_ordinal>0|[1-9][0-9]*):"
    r"(?P<group_ordinal>0|[1-9][0-9]*)\Z",
    re.ASCII,
)
_PROBE_OBJECT_NAMES = ("probe.dump", "probe.dump.sha256")
_TASK8_RCLONE_OUTCOMES = frozenset({"success", "indeterminate"})
_TASK8_NORMAL_PROBE_TAIL_PURPOSES = (
    "probe-create:probe.dump:strict-no-existing",
    "probe-create:probe.dump.sha256:strict-no-existing",
    "probe-owned-inventory",
    "probe-verify:probe.dump",
    "probe-verify:probe.dump.sha256",
    "probe-cleanup:probe.dump",
    "probe-cleanup:probe.dump.sha256",
    "probe-prefix-empty",
)
_TASK8_DRY_RUN_PURPOSES = (
    "dry-run-inventory-before",
    "dry-run-runtime",
    "dry-run-inventory-after",
)
_TASK8_POLICY_PURPOSES = ("enable-prune-inventory",)
_TASK8_OBSERVE_PURPOSES = (
    "observe-inventory-before",
    "observe-current-stat-before",
    "observe-current-hash-before",
    "observe-current-sidecar-before",
    "observe-inventory-after",
    "observe-current-hash-after",
    "observe-current-sidecar-after",
    "observe-current-stat-after",
)
_TASK8_ENTRY_PHASES = {
    "probe": "probing",
    "dry_run": "dry_run_recording",
    "policy": "policy_enabling",
    "observe": "observing",
}
_TASK8_PRIOR_PHASES = {
    "probe": "installed",
    "dry_run": "probed",
    "policy": "dry_run_recorded",
    "observe": "policy_enabled",
}
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
_BUSCTL_EXECUTABLE = "/usr/bin/busctl"
_JOURNALCTL_EXECUTABLE = "/usr/bin/journalctl"
_LOGINCTL_EXECUTABLE = "/usr/bin/loginctl"
_DATE_EXECUTABLE = "/usr/bin/date"
_RCLONE_EXECUTABLE = "/usr/bin/rclone"
_MAX_COMMAND_OUTPUT_BYTES = 4096
_DEFAULT_COMMAND_TIMEOUT_SECONDS = 15 * 60
_COMMAND_CLEANUP_GRACE_SECONDS = 2.0
_MAX_DRY_RUN_NAME_BYTES = 512
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
_SYSTEMD_INVOCATION_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z", re.ASCII)
_RCLONE_MODTIME_RE = re.compile(
    r"\A(?P<base>[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{1,9}))?"
    r"(?P<zone>Z|[+-][0-9]{2}:[0-9]{2})\Z",
    re.ASCII,
)
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
if mode == "stdin":
    os.dup2(fd, 0)
    if fd != 0:
        os.close(fd)
    os.execve(
        executable,
        argv,
        {
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        },
    )
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
    "/etc/systemd/system/degen-prod-db-backup-alert@.service",
    "/etc/systemd/system/degen-prod-db-backup.timer",
    "/etc/degen/prod-db-backup.env",
)
_SNAPSHOT_TARGET_NAMES = {
    "/usr/local/sbin/degen-prod-db-backup": "degen-prod-db-backup",
    "/usr/local/sbin/degen-prod-db-retention": "degen-prod-db-retention",
    "/usr/local/sbin/degen-prod-db-backup-env": "degen-prod-db-backup-env",
    "/usr/local/sbin/degen-prod-db-backup-ops": "degen-prod-db-backup-ops",
    "/etc/systemd/system/degen-prod-db-backup.service": "degen-prod-db-backup.service",
    "/etc/systemd/system/degen-prod-db-backup-alert@.service": "degen-prod-db-backup-alert@.service",
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
    ("deploy/systemd/degen-prod-db-backup-alert@.service", _TARGET_ORDER[5]),
    ("deploy/systemd/degen-prod-db-backup.timer", _TARGET_ORDER[6]),
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


class _Task7RcloneEvidencePersistenceError(OperationStateError):
    def __init__(
        self,
        group: dict[str, object],
        persistence_error: Exception,
        preflight_error: BaseException | None,
    ) -> None:
        super().__init__("rclone before/after audit could not be persisted durably")
        self.group = copy.deepcopy(group)
        self.persistence_error = persistence_error
        self.preflight_error = preflight_error


class _Task7RcloneAuditIncompleteError(OperationStateError):
    def __init__(
        self,
        primary_error: Exception,
        secondary_error: BaseException | None,
    ) -> None:
        super().__init__(sanitize_error_text(primary_error))
        self.primary_error = primary_error
        self.secondary_error = secondary_error


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


@dataclass(frozen=True)
class MigrationLocks:
    legacy_fd: int
    runtime_fd: int


@dataclass(frozen=True)
class _Task7LockReleaseIssue:
    stage: str
    error: Exception
    release_uncertain: bool

    def __iter__(self):
        yield self.stage
        yield self.error


class _Task8GuardReleaseError(OperationStateError):
    def __init__(self, issues: list[_Task7LockReleaseIssue]) -> None:
        if not issues:
            raise OperationStateError("guard release failure requires at least one issue")
        self.issues = list(issues)
        details = "; ".join(
            f"{issue.stage}: {sanitize_error_text(issue.error)}"
            for issue in self.issues
        )
        qualifier = (
            "release uncertainty"
            if any(issue.release_uncertain for issue in self.issues)
            else "release hook failure"
        )
        super().__init__(f"guard {qualifier}: {details}")


class _Task8ProbeSourceCleanupError(OperationStateError):
    def __init__(self, primary_error: Exception, cleanup_error: Exception) -> None:
        self.primary_error = primary_error
        self.cleanup_error = cleanup_error
        super().__init__("remote probe source cleanup failed after an operation error")


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
    operation_lock_held: bool = False,
    force_checkpoint: bool = False,
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
            operation_lock_held=operation_lock_held,
            force_checkpoint=force_checkpoint,
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
            operation_lock_held=operation_lock_held,
            force_checkpoint=force_checkpoint,
        )


def _atomic_write_with_operation_directory_binding(
    path: Path,
    state: dict[str, object],
    canonical: bytes,
    effective_uid: int,
    binding: _OperationDirectoryBinding,
    pre_replace_validator: PreReplaceValidator | None,
    *,
    operation_lock_held: bool = False,
    force_checkpoint: bool = False,
) -> None:
    if binding.path != path.parent:
        raise OperationStateError("operation directory binding path does not match state path")
    _revalidate_operation_dir_binding(
        binding.path,
        binding.descriptor,
        binding.metadata,
        effective_uid,
    )
    lock_context = (
        contextlib.nullcontext()
        if operation_lock_held
        else _exclusive_operation_state_lock(binding.descriptor)
    )
    with lock_context:
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
            force_checkpoint=force_checkpoint,
        )


@contextlib.contextmanager
def _open_operation_transaction(context: OperationsContext):
    with _open_validated_operation_dir(
        context.paths.operation_dir,
        context.effective_uid,
    ) as directory_fd:
        operation_metadata = (
            context.paths.operation_dir.lstat()
            if directory_fd is None
            else os.fstat(directory_fd)
        )
        binding = _OperationDirectoryBinding(
            context.paths.operation_dir,
            directory_fd,
            operation_metadata,
        )
        if directory_fd is None:
            key = str(context.paths.operation_dir)
            with _DIRECT_OPERATION_LOCKS_GUARD:
                direct_lock = _DIRECT_OPERATION_LOCKS.setdefault(
                    key,
                    threading.Lock(),
                )
            with direct_lock:
                _revalidate_operation_dir_binding(
                    binding.path,
                    binding.descriptor,
                    binding.metadata,
                    context.effective_uid,
                )
                yield binding
            return
        with _exclusive_operation_state_lock(directory_fd):
            _revalidate_operation_dir_binding(
                binding.path,
                binding.descriptor,
                binding.metadata,
                context.effective_uid,
            )
            yield binding


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
    *,
    force_checkpoint: bool = False,
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
        if state == old_state and not force_checkpoint:
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
    _validate_rclone_evidence_semantics(state)
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
        missing = sorted(keys - actual)
        extra = sorted(actual - keys)
        if missing:
            raise OperationStateError(
                f"{label} has invalid keys: missing required field {missing[0]}"
            )
        raise OperationStateError(
            f"{label} has invalid keys: unexpected field {extra[0]}"
        )
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
        frozenset(
            {
                "manifest_sha256",
                "asset_hashes",
                "environment_sha256",
                "enabled_environment_sha256",
            }
        ),
        "host_stage",
    )
    _require_hash(item["manifest_sha256"], "host_stage.manifest_sha256")
    _require_string_map(item["asset_hashes"], "host_stage.asset_hashes", hash_values=True)
    _require_hash(item["environment_sha256"], "host_stage.environment_sha256")
    _require_hash(
        item["enabled_environment_sha256"],
        "host_stage.enabled_environment_sha256",
    )
    if item["enabled_environment_sha256"] == item["environment_sha256"]:
        raise OperationStateError(
            "host_stage enabled environment digest must differ from disabled digest"
        )


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


def _validate_prior_runtime(value: object, label: str = "prior_runtime") -> None:
    item = _require_object(
        value,
        frozenset({"timer_enabled", "timer_active", "pids", "preinstall_trigger_epoch"}),
        label,
    )
    _require_bool(item["timer_enabled"], f"{label}.timer_enabled")
    _require_bool(item["timer_active"], f"{label}.timer_active")
    pids = item["pids"]
    if type(pids) is not dict:
        raise OperationStateError(f"{label}.pids must be an integer map")
    for key, pid in pids.items():
        _require_string(key, f"{label}.pids key", nonempty=True)
        _require_int(pid, f"{label}.pids value", minimum=1)
    fixed = {
        "system:degen-web.service",
        "system:degen-worker.service",
    }
    keys = set(pids)
    postgres_keys = [key for key in keys if key.startswith("postgresql:")]
    user_keys = [key for key in keys if key.startswith("user:")]
    if len(keys) != 4 or not fixed.issubset(keys) or len(postgres_keys) != 1 or len(user_keys) != 1:
        raise OperationStateError(f"{label}.pids identity keys are incomplete or ambiguous")
    postgres_unit = postgres_keys[0].split(":", 1)[1]
    if (
        _SYSTEMD_UNIT_RE.fullmatch(postgres_unit) is None
        or not postgres_unit.startswith("postgresql")
    ):
        raise OperationStateError(f"{label} PostgreSQL unit identity is invalid")
    user_parts = user_keys[0].split(":")
    if (
        len(user_parts) != 4
        or not user_parts[1].isdigit()
        or str(int(user_parts[1])) != user_parts[1]
        or int(user_parts[1]) < 0
        or _LOGIN_USER_RE.fullmatch(user_parts[2]) is None
        or user_parts[3] != "degen-ops-discord-bot.service"
    ):
        raise OperationStateError(f"{label} bot owner identity is invalid")
    if len(set(pids.values())) != len(pids):
        raise OperationStateError(f"{label}.pids values must be unique")
    _require_optional_int(
        item["preinstall_trigger_epoch"],
        f"{label}.preinstall_trigger_epoch",
    )


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
                "runtime_directory_created",
                "validated_epoch",
                "validation_evidence_sha256",
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
    _require_bool(
        item["runtime_directory_created"],
        "install.runtime_directory_created",
    )
    _require_optional_int(item["validated_epoch"], "install.validated_epoch")
    _require_optional_hash(
        item["validation_evidence_sha256"],
        "install.validation_evidence_sha256",
    )


def _validate_rclone_evidence_group(value: object, label: str) -> None:
    if type(value) is not dict:
        raise OperationStateError(f"{label} must be an object with exact keys")
    raw_group_id = value.get("group_id")
    is_task8 = isinstance(raw_group_id, str) and raw_group_id.startswith("task8:")
    is_dry_run = (
        isinstance(raw_group_id, str)
        and raw_group_id.startswith("task8:dry_run:")
    )
    is_policy = (
        isinstance(raw_group_id, str)
        and raw_group_id.startswith("task8:policy:")
    )
    is_observe = (
        isinstance(raw_group_id, str)
        and raw_group_id.startswith("task8:observe:")
    )
    is_result_bearing = is_dry_run or is_policy or is_observe
    keys = {"group_id", "purpose", "before", "after", "evidence_sha256"}
    if is_task8:
        keys.add("outcome")
    if is_result_bearing:
        keys.add("result_sha256")
    item = _require_object(
        value,
        frozenset(keys),
        label,
    )
    _require_string(item["group_id"], f"{label}.group_id")
    _require_string(item["purpose"], f"{label}.purpose")
    _validate_file_audit(item["before"], f"{label}.before")
    if item["after"] is None:
        if item["evidence_sha256"] is not None or (
            is_task8 and item["outcome"] is not None
        ):
            raise OperationStateError(
                f"{label} pending audit evidence and outcome must be null"
            )
        if is_result_bearing and item["result_sha256"] is not None:
            raise OperationStateError(
                f"{label} pending Task 8 result digest must be null"
            )
    else:
        _validate_file_audit(item["after"], f"{label}.after")
        _require_hash(item["evidence_sha256"], f"{label}.evidence_sha256")
        if is_task8 and item["outcome"] not in _TASK8_RCLONE_OUTCOMES:
            raise OperationStateError(
                f"{label}.outcome must be success or indeterminate"
            )
        if is_result_bearing:
            if item["outcome"] == "success":
                _require_hash(
                    item["result_sha256"],
                    f"{label}.result_sha256",
                )
            elif item["result_sha256"] is not None:
                raise OperationStateError(
                    f"{label} indeterminate Task 8 result digest must be null"
                )


def _task8_attempt_entries(
    state: dict[str, object],
    kind: str,
) -> list[dict[str, object]]:
    entry_phase = _TASK8_ENTRY_PHASES[kind]
    history = state["phase_history"]
    assert isinstance(history, list)
    return [
        entry
        for entry in history
        if isinstance(entry, dict) and entry["phase"] == entry_phase
    ]


def _task8_group_evidence_sha256(group: dict[str, object]) -> str:
    payload = {
        "group_id": group["group_id"],
        "purpose": group["purpose"],
        "before": group["before"],
        "after": group["after"],
    }
    if str(group["group_id"]).startswith("task8:"):
        payload["outcome"] = group["outcome"]
    if str(group["group_id"]).startswith(
        ("task8:dry_run:", "task8:policy:", "task8:observe:")
    ):
        payload["result_sha256"] = group["result_sha256"]
    return _task7_evidence_sha256(
        "rclone-audit",
        payload,
    )


def _task8_group_identity(
    group: dict[str, object],
) -> tuple[str, int, int, int]:
    group_id = str(group["group_id"])
    match = _TASK8_RCLONE_GROUP_RE.fullmatch(group_id)
    if match is None:
        raise OperationStateError("Task 8 rclone group identity is invalid")
    return (
        match.group("kind"),
        int(match.group("started_epoch")),
        int(match.group("attempt_ordinal")),
        int(match.group("group_ordinal")),
    )


def _validate_rclone_evidence_semantics(state: dict[str, object]) -> None:
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    history = state["phase_history"]
    assert isinstance(history, list)
    history_epochs = [entry["epoch"] for entry in history]
    if history_epochs != sorted(history_epochs):
        return
    pending = [
        index
        for index, group in enumerate(groups)
        if isinstance(group, dict) and group["after"] is None
    ]
    if pending and pending != [len(groups) - 1]:
        raise OperationStateError(
            "pending rclone audit must be the final and only pending group"
        )

    next_group_ordinal: dict[tuple[str, int], int] = {}
    pending_identity: tuple[str, int, int, int] | None = None
    last_attempt_history_position = -1
    for index, group in enumerate(groups):
        assert isinstance(group, dict)
        if group["group_id"] == "install":
            if index != 0 or group["purpose"] != "credential-refresh-audit":
                raise OperationStateError("install rclone audit identity is invalid")
            if (
                group["after"] is not None
                and group["evidence_sha256"]
                != _task8_group_evidence_sha256(group)
            ):
                raise OperationStateError(
                    "install rclone audit evidence digest is invalid"
                )
            continue
        kind, started_epoch, attempt_ordinal, group_ordinal = (
            _task8_group_identity(group)
        )
        if kind == "dry_run" and (
            group_ordinal >= len(_TASK8_DRY_RUN_PURPOSES)
            or group["purpose"] != _TASK8_DRY_RUN_PURPOSES[group_ordinal]
        ):
            raise OperationStateError(
                "dry_run rclone group purpose is not the exact ordered prefix"
            )
        if kind == "policy" and (
            group_ordinal >= len(_TASK8_POLICY_PURPOSES)
            or group["purpose"] != _TASK8_POLICY_PURPOSES[group_ordinal]
        ):
            raise OperationStateError(
                "policy rclone group purpose is not the exact ordered prefix"
            )
        if kind == "observe" and (
            group_ordinal >= len(_TASK8_OBSERVE_PURPOSES)
            or group["purpose"] != _TASK8_OBSERVE_PURPOSES[group_ordinal]
        ):
            raise OperationStateError(
                "observation rclone group purpose is not the exact ordered prefix"
            )
        entries = _task8_attempt_entries(state, kind)
        if (
            attempt_ordinal >= len(entries)
            or entries[attempt_ordinal]["epoch"] != started_epoch
        ):
            raise OperationStateError(
                "Task 8 rclone group is not bound to its history occurrence"
            )
        entry_phase = _TASK8_ENTRY_PHASES[kind]
        attempt_positions = [
            history_index
            for history_index, history_entry in enumerate(history)
            if history_entry["phase"] == entry_phase
        ]
        attempt_history_position = attempt_positions[attempt_ordinal]
        if attempt_history_position < last_attempt_history_position:
            raise OperationStateError(
                "Task 8 rclone groups violate global history occurrence order"
            )
        last_attempt_history_position = attempt_history_position
        key = (kind, attempt_ordinal)
        expected_group_ordinal = next_group_ordinal.get(key, 0)
        if group_ordinal != expected_group_ordinal:
            raise OperationStateError(
                "Task 8 rclone group ordinal is duplicate or out of sequence"
            )
        next_group_ordinal[key] = expected_group_ordinal + 1
        if group["after"] is None:
            pending_identity = (
                kind,
                started_epoch,
                attempt_ordinal,
                group_ordinal,
            )
        elif group["evidence_sha256"] != _task8_group_evidence_sha256(group):
            raise OperationStateError(
                "Task 8 rclone group evidence digest is invalid"
            )

    if not pending:
        return
    group = groups[-1]
    assert isinstance(group, dict)
    if group["group_id"] == "install":
        if state["phase"] not in {"installing", "recovering", "recovery_required"}:
            raise OperationStateError(
                "pending rclone audit is forbidden in a terminal or unrelated phase"
            )
        return
    transaction = state["active_transaction"]
    if pending_identity is None or not isinstance(transaction, dict):
        raise OperationStateError(
            "pending rclone audit identity is not bound to an active transaction"
        )
    kind, started_epoch, attempt_ordinal, _group_ordinal = pending_identity
    expected = _active_expected_for_state(state)
    entries = _task8_attempt_entries(state, kind)
    current_attempt_ordinal = len(entries) - 1
    if (
        expected is None
        or expected[0] != kind
        or transaction["kind"] != kind
        or transaction["started_epoch"] != started_epoch
        or attempt_ordinal != current_attempt_ordinal
    ):
        raise OperationStateError(
            "pending rclone audit identity does not match the active transaction"
        )


def _validate_probe_prefix(
    value: object,
    label: str,
    *,
    operation_id: str | None = None,
) -> str:
    prefix = _require_string(value, label, nonempty=True)
    match = _PROBE_PREFIX_RE.fullmatch(prefix)
    if match is None:
        raise OperationStateError(f"{label} is not a safe remote probe prefix")
    if operation_id is not None and match.group("operation_id") != operation_id:
        raise OperationStateError(f"{label} is not bound to this operation")
    return prefix


def _task8_evidence_sha256(label: str, payload: object) -> str:
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


def _task8_utf8_size(value: object, label: str) -> int:
    if type(value) is not str:
        raise OperationStateError(f"{label} is invalid")
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise OperationStateError(f"{label} encoding is invalid") from exc


def _task8_validate_remote_name(value: object, label: str) -> str:
    name = _require_string(value, label, nonempty=True)
    try:
        encoded = name.encode("ascii")
    except UnicodeEncodeError as exc:
        raise OperationStateError(f"{label} is not ASCII") from exc
    if (
        len(encoded) > _MAX_DRY_RUN_NAME_BYTES
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
        or any(byte < 0x20 or byte > 0x7E for byte in encoded)
    ):
        raise OperationStateError(f"{label} is unsafe")
    return name


def _task8_validate_canonical_remote_names(
    value: object,
    label: str,
    *,
    require_sorted: bool,
) -> list[str]:
    names = _require_string_list(value, label)
    if len(names) > _MAX_BACKUP_ENTRIES:
        raise OperationStateError(f"{label} exceeds the entry bound")
    folded: set[str] = set()
    seen: set[str] = set()
    for index, raw_name in enumerate(names):
        name = _task8_validate_remote_name(raw_name, f"{label}[{index}]")
        casefolded = name.casefold()
        if name in seen or casefolded in folded:
            raise OperationStateError(
                f"{label} contains duplicate or casefold-colliding names"
            )
        seen.add(name)
        folded.add(casefolded)
    if require_sorted and names != sorted(names):
        raise OperationStateError(f"{label} is not in canonical order")
    return names


def _task8_decode_remote_lsf_inventory(raw: str) -> tuple[list[str], list[str]]:
    if _task8_utf8_size(raw, "remote dry-run inventory output") > _MAX_STATE_BYTES:
        raise OperationStateError("remote dry-run inventory exceeds the size limit")
    if raw == "":
        return [], []
    if not raw.endswith("\n") or "\r" in raw or "\x00" in raw:
        raise OperationStateError("remote dry-run inventory encoding is invalid")
    records = raw[:-1].split("\n")
    if any(not record for record in records):
        raise OperationStateError("remote dry-run inventory contains an empty record")
    names = _task8_validate_canonical_remote_names(
        records,
        "remote dry-run inventory",
        require_sorted=False,
    )
    canonical = sorted(names)
    return canonical, [name.casefold() for name in canonical]


def _task8_decode_strict_canonical_json_line(raw: str, label: str) -> object:
    if (
        _task8_utf8_size(raw, label) > _MAX_STATE_BYTES
        or not raw
        or not raw.endswith("\n")
        or "\r" in raw
        or "\x00" in raw
    ):
        raise OperationStateError(f"{label} encoding is invalid")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise OperationStateError(f"{label} is invalid") from exc
    try:
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ) + "\n"
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise OperationStateError(f"{label} is invalid") from exc
    if canonical != raw:
        raise OperationStateError(f"{label} is not canonical")
    return value


def _task8_decode_remote_retention_plan(
    raw: str,
    *,
    expected_prefix: str,
    inventory_names: list[str],
    expected_now: datetime,
) -> dict[str, list[str]]:
    prefix = _require_string(expected_prefix, "remote retention prefix", nonempty=True)
    if _BACKUP_PREFIX_RE.fullmatch(prefix) is None:
        raise OperationStateError("remote retention prefix is invalid")
    inventory = _task8_validate_canonical_remote_names(
        inventory_names,
        "remote retention inventory",
        require_sorted=True,
    )
    if (
        not isinstance(expected_now, datetime)
        or expected_now.tzinfo is None
        or expected_now.utcoffset() is None
    ):
        raise OperationStateError(
            "remote retention expected_now must be timezone-aware"
        )
    inventory_set = set(inventory)
    value = _task8_decode_strict_canonical_json_line(
        raw,
        "remote retention planner output",
    )
    plan = _require_object(
        value,
        frozenset({"mode", "prefix", "keep", "delete", "protected"}),
        "remote retention plan",
    )
    if plan["mode"] != "remote" or plan["prefix"] != prefix:
        raise OperationStateError("remote retention plan identity is invalid")

    def flatten_pairs(value: object, label: str, *, deleting: bool) -> list[str]:
        if type(value) is not list or len(value) > _MAX_BACKUP_ENTRIES // 2:
            raise OperationStateError(f"{label} must be a bounded list")
        flattened: list[str] = []
        ordering: list[tuple[datetime, str]] = []
        allowed_reasons = (
            {"expired"}
            if deleting
            else {"newest", "daily", "weekly", "monthly"}
        )
        for index, record_value in enumerate(value):
            record = _require_object(
                record_value,
                frozenset({"dump", "checksum", "timestamp", "reasons"}),
                f"{label}[{index}]",
            )
            dump = _task8_validate_remote_name(
                record["dump"],
                f"{label}[{index}].dump",
            )
            checksum = _task8_validate_remote_name(
                record["checksum"],
                f"{label}[{index}].checksum",
            )
            timestamp = _require_string(
                record["timestamp"],
                f"{label}[{index}].timestamp",
                nonempty=True,
            )
            match = _BACKUP_NAME_RE.fullmatch(dump)
            if (
                match is None
                or match.group("sidecar") is not None
                or match.group("prefix") != prefix
                or match.group("stamp") != timestamp
                or checksum != f"{dump}.sha256"
            ):
                raise OperationStateError(f"{label}[{index}] pair is invalid")
            try:
                parsed_timestamp = datetime.strptime(
                    timestamp,
                    "%Y%m%dT%H%M%SZ",
                ).replace(tzinfo=timezone.utc)
            except ValueError as exc:
                raise OperationStateError(
                    f"{label}[{index}] timestamp is invalid"
                ) from exc
            if parsed_timestamp > expected_now:
                raise OperationStateError(
                    f"{label}[{index}] future timestamp may not be actionable"
                )
            reasons = _require_string_list(
                record["reasons"],
                f"{label}[{index}].reasons",
            )
            if (
                not reasons
                or reasons != sorted(set(reasons))
                or not set(reasons).issubset(allowed_reasons)
                or (deleting and reasons != ["expired"])
            ):
                raise OperationStateError(f"{label}[{index}] reasons are invalid")
            flattened.extend((dump, checksum))
            ordering.append((parsed_timestamp, dump))
        expected_ordering = sorted(ordering, reverse=not deleting)
        if ordering != expected_ordering:
            raise OperationStateError(f"{label} is not in planner order")
        return flattened

    keep_names = flatten_pairs(plan["keep"], "remote retention keep", deleting=False)
    delete_names = flatten_pairs(
        plan["delete"],
        "remote retention delete",
        deleting=True,
    )
    protected_value = plan["protected"]
    if type(protected_value) is not list or len(protected_value) > _MAX_BACKUP_ENTRIES:
        raise OperationStateError("remote retention protected must be a bounded list")
    protected_names: list[str] = []
    allowed_protected_reasons = {
        "unknown-name",
        "unparseable-timestamp",
        "future-timestamp",
        "incomplete-pair",
    }
    protected_records: list[tuple[str, str]] = []
    for index, record_value in enumerate(protected_value):
        record = _require_object(
            record_value,
            frozenset({"name", "reason"}),
            f"remote retention protected[{index}]",
        )
        name = _task8_validate_remote_name(
            record["name"],
            f"remote retention protected[{index}].name",
        )
        reason = record["reason"]
        if reason not in allowed_protected_reasons:
            raise OperationStateError("remote retention protected reason is invalid")
        base = name[:-7] if name.endswith(".sha256") else name
        match = _BACKUP_NAME_RE.fullmatch(base)
        if match is None or match.group("sidecar") is not None or match.group("prefix") != prefix:
            expected_reason = "unknown-name"
        else:
            try:
                parsed_protected = datetime.strptime(
                    match.group("stamp"),
                    "%Y%m%dT%H%M%SZ",
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                expected_reason = "unparseable-timestamp"
            else:
                counterpart = (
                    base
                    if name.endswith(".sha256")
                    else f"{name}.sha256"
                )
                if parsed_protected > expected_now:
                    expected_reason = "future-timestamp"
                elif counterpart in inventory_set:
                    raise OperationStateError(
                        "remote retention protected contains a complete actionable pair"
                    )
                else:
                    expected_reason = "incomplete-pair"
        if expected_reason is not None and reason != expected_reason:
            raise OperationStateError(
                "remote retention protected reason does not match its name"
            )
        protected_names.append(name)
        protected_records.append((name, reason))
    if protected_records != sorted(protected_records):
        raise OperationStateError("remote retention protected is not in planner order")

    partitions = (keep_names, protected_names, delete_names)
    partition_sets = [set(items) for items in partitions]
    if any(len(items) != len(set(items)) for items in partitions):
        raise OperationStateError("remote retention plan contains duplicate names")
    if any(
        partition_sets[left] & partition_sets[right]
        for left in range(len(partition_sets))
        for right in range(left + 1, len(partition_sets))
    ):
        raise OperationStateError("remote retention plan partitions overlap")
    if set().union(*partition_sets) != set(inventory):
        raise OperationStateError("remote retention plan does not partition inventory")
    return {
        "keep_names": keep_names,
        "protected_names": protected_names,
        "delete_names": delete_names,
    }


_TASK8_DRY_RUN_LOG_LINE_RE = re.compile(
    r"\A\[(?P<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z)\] (?P<body>.*)\Z",
    re.ASCII,
)

_TASK8_RCLONE_CONFIG_RECEIPT_RE = re.compile(
    r"\ARCLONE_CONFIG_RECEIPT phase=(?P<phase>before|final) status=ok "
    r"path=(?P<path>/[^ ]+) sha256=(?P<sha256>[0-9a-f]{64}) "
    r"device=(?P<device>[0-9]+) inode=(?P<inode>[0-9]+) "
    r"uid=(?P<uid>[0-9]+) gid=(?P<gid>[0-9]+) mode=(?P<mode>[0-7]{4}) "
    r"links=(?P<links>[0-9]+) size=(?P<size>[0-9]+) "
    r"mtime_ns=(?P<mtime_ns>[0-9]+) "
    r"change=(?P<change>baseline|unchanged|possible-oauth-refresh)\Z",
    re.ASCII,
)


def _task8_decode_rclone_config_receipt(
    body: str,
    *,
    expected_phase: str,
) -> dict[str, str]:
    match = _TASK8_RCLONE_CONFIG_RECEIPT_RE.fullmatch(body)
    if match is None:
        raise OperationStateError("remote dry-run rclone receipt is invalid")
    receipt = match.groupdict()
    if (
        receipt["phase"] != expected_phase
        or receipt["path"] != _RCLONE_CONFIG_PATH
        or receipt["uid"] != "0"
        or receipt["mode"] != "0600"
        or receipt["links"] != "1"
        or (
            expected_phase == "before"
            and receipt["change"] != "baseline"
        )
        or (
            expected_phase == "final"
            and receipt["change"] not in {"unchanged", "possible-oauth-refresh"}
        )
    ):
        raise OperationStateError("remote dry-run rclone receipt identity is invalid")
    return receipt


def _task8_decode_remote_dry_run_log(
    raw: str,
    *,
    expected_prefix: str,
    expected_delete_names: list[str],
) -> list[str]:
    prefix = _require_string(expected_prefix, "remote dry-run prefix", nonempty=True)
    if _BACKUP_PREFIX_RE.fullmatch(prefix) is None:
        raise OperationStateError("remote dry-run prefix is invalid")
    expected = _task8_validate_canonical_remote_names(
        expected_delete_names,
        "remote dry-run expected candidates",
        require_sorted=False,
    )
    if (
        _task8_utf8_size(raw, "remote dry-run log") > _MAX_STATE_BYTES
        or not raw
        or not raw.endswith("\n")
        or "\r" in raw
        or "\x00" in raw
    ):
        raise OperationStateError("remote dry-run log encoding is invalid")
    lines = raw[:-1].split("\n")
    if any(not line for line in lines):
        raise OperationStateError("remote dry-run log contains an empty line")
    bodies: list[str] = []
    timestamps: list[datetime] = []
    for line in lines:
        match = _TASK8_DRY_RUN_LOG_LINE_RE.fullmatch(line)
        if match is None:
            raise OperationStateError("remote dry-run log line is not timestamped")
        try:
            timestamp = datetime.strptime(
                match.group("timestamp"),
                "%Y-%m-%dT%H:%M:%SZ",
            )
        except ValueError as exc:
            raise OperationStateError("remote dry-run log timestamp is invalid") from exc
        timestamps.append(timestamp)
        bodies.append(match.group("body"))
    if timestamps != sorted(timestamps):
        raise OperationStateError("remote dry-run log timestamps are out of order")

    preflight = (
        "Preflight passed for mode=remote-retention-dry-run prefix=" + prefix
    )
    completion = "Remote retention dry run completed; no dump or deletion was performed"
    if len(bodies) < 4 or bodies[1] != preflight or bodies[-2] != completion:
        raise OperationStateError("remote dry-run log lifecycle is incomplete")
    before_receipt = _task8_decode_rclone_config_receipt(
        bodies[0],
        expected_phase="before",
    )
    final_receipt = _task8_decode_rclone_config_receipt(
        bodies[-1],
        expected_phase="final",
    )
    metadata_fields = (
        "path",
        "sha256",
        "device",
        "inode",
        "uid",
        "gid",
        "mode",
        "links",
        "size",
        "mtime_ns",
    )
    metadata_unchanged = all(
        before_receipt[field] == final_receipt[field]
        for field in metadata_fields
    )
    if (
        (final_receipt["change"] == "unchanged" and not metadata_unchanged)
        or (
            final_receipt["change"] == "possible-oauth-refresh"
            and metadata_unchanged
        )
    ):
        raise OperationStateError("remote dry-run rclone receipt change is inconsistent")
    middle = bodies[2:-2]
    candidates: list[str] = []
    would_delete: list[str] = []
    none_seen = False
    for body in middle:
        if body == "Remote retention candidates: none":
            if none_seen or candidates or would_delete:
                raise OperationStateError("remote dry-run zero-candidate receipt is invalid")
            none_seen = True
            continue
        candidate_prefix = "Remote retention candidate: "
        delete_prefix = "Remote retention dry run: would delete "
        if body.startswith(candidate_prefix):
            if none_seen or would_delete:
                raise OperationStateError("remote dry-run candidate order is invalid")
            candidates.append(
                _task8_validate_remote_name(
                    body[len(candidate_prefix) :],
                    "remote dry-run candidate",
                )
            )
            continue
        if body.startswith(delete_prefix):
            if none_seen:
                raise OperationStateError("remote dry-run delete receipt is invalid")
            would_delete.append(
                _task8_validate_remote_name(
                    body[len(delete_prefix) :],
                    "remote dry-run would-delete candidate",
                )
            )
            continue
        raise OperationStateError("remote dry-run log contains an unexpected record")
    if expected:
        if none_seen or candidates != expected or would_delete != expected:
            raise OperationStateError("remote dry-run candidates differ from the reviewed plan")
    elif not none_seen or candidates or would_delete:
        raise OperationStateError("remote dry-run zero-candidate receipt is missing")
    return candidates


_TASK8_OBSERVATION_SERVICE_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "MainPID",
    "Result",
    "ExecMainCode",
    "ExecMainStatus",
    "ExecMainStartTimestamp",
    "ExecMainExitTimestamp",
    "ExecMainStartTimestampMonotonic",
    "TriggeredBy",
    "RefuseManualStart",
    "InvocationID",
)

_TASK8_OBSERVATION_TIMER_PROPERTIES = (
    "LastTriggerUSec",
)


def _task8_decode_observation_service(raw: str) -> dict[str, str]:
    values = _parse_systemctl_show(
        raw,
        _TASK8_OBSERVATION_SERVICE_PROPERTIES,
        "scheduled backup service observation",
    )
    monotonic_raw = values["ExecMainStartTimestampMonotonic"]
    if (
        values["LoadState"] != "loaded"
        or values["ActiveState"] != "inactive"
        or values["SubState"] != "dead"
        or values["MainPID"] != "0"
        or values["Result"] != "success"
        or values["ExecMainCode"] != "1"
        or values["ExecMainStatus"] != "0"
        or not values["ExecMainStartTimestamp"]
        or not values["ExecMainExitTimestamp"]
        or re.fullmatch(r"[1-9][0-9]{0,19}", monotonic_raw, re.ASCII) is None
        or str(int(monotonic_raw)) != monotonic_raw
        or values["TriggeredBy"] != "degen-prod-db-backup.timer"
        or values["RefuseManualStart"] != "yes"
        or _SYSTEMD_INVOCATION_ID_RE.fullmatch(values["InvocationID"]) is None
    ):
        raise OperationStateError(
            "scheduled backup service invocation is not one successful inactive run"
        )
    return {
        "start_timestamp": values["ExecMainStartTimestamp"],
        "exit_timestamp": values["ExecMainExitTimestamp"],
        "start_monotonic_usec": int(monotonic_raw),
        "invocation_id": values["InvocationID"],
    }


def _task8_decode_observation_timer(raw: str) -> dict[str, object]:
    values = _parse_systemctl_show(
        raw,
        _TASK8_OBSERVATION_TIMER_PROPERTIES,
        "scheduled backup timer observation",
    )
    trigger = values["LastTriggerUSec"]
    if not trigger or trigger in {"n/a", "never"}:
        raise OperationStateError(
            "scheduled backup timer trigger evidence is invalid"
        )
    return {
        "trigger_timestamp": trigger,
    }


def _task8_decode_observation_timer_monotonic(raw: str) -> int:
    if type(raw) is not str:
        raise OperationStateError(
            "scheduled backup timer monotonic trigger evidence is invalid"
        )
    match = re.fullmatch(r"t ([1-9][0-9]{0,19})\n", raw, re.ASCII)
    if match is None:
        raise OperationStateError(
            "scheduled backup timer monotonic trigger evidence is invalid"
        )
    value = int(match.group(1))
    if value > (2**64 - 1):
        raise OperationStateError(
            "scheduled backup timer monotonic trigger evidence is invalid"
        )
    return value


def _task8_decode_observation_journal(
    raw: str,
    *,
    expected_invocation_id: str,
    expected_prefix: str,
) -> dict[str, object]:
    invocation_id = _require_string(
        expected_invocation_id,
        "scheduled backup invocation id",
        nonempty=True,
    )
    if _SYSTEMD_INVOCATION_ID_RE.fullmatch(invocation_id) is None:
        raise OperationStateError("scheduled backup invocation id is invalid")
    prefix = _require_string(
        expected_prefix,
        "scheduled backup prefix",
        nonempty=True,
    )
    if _BACKUP_PREFIX_RE.fullmatch(prefix) is None:
        raise OperationStateError("scheduled backup prefix is invalid")
    if (
        type(raw) is not str
        or _task8_utf8_size(raw, "scheduled backup journal") > _MAX_STATE_BYTES
        or not raw
        or not raw.endswith("\n")
        or "\r" in raw
        or "\x00" in raw
    ):
        raise OperationStateError("scheduled backup journal encoding is invalid")
    lines = raw[:-1].split("\n")
    if not lines or len(lines) > _MAX_BACKUP_ENTRIES or any(not line for line in lines):
        raise OperationStateError("scheduled backup journal record count is invalid")
    trusted_epochs: list[int] = []
    messages: list[str] = []
    bodies: list[str] = []
    message_epochs: list[int] = []
    for index, line in enumerate(lines):
        try:
            value = json.loads(
                line,
                object_pairs_hook=_reject_duplicate_pairs,
                parse_constant=_reject_nonfinite,
            )
        except (json.JSONDecodeError, ValueError) as exc:
            raise OperationStateError(
                "scheduled backup journal JSON is invalid"
            ) from exc
        if type(value) is not dict:
            raise OperationStateError("scheduled backup journal record is not an object")
        required = {
            "__REALTIME_TIMESTAMP",
            "MESSAGE",
            "_SYSTEMD_INVOCATION_ID",
            "_SYSTEMD_UNIT",
        }
        if not required.issubset(value):
            raise OperationStateError("scheduled backup journal identity is incomplete")
        timestamp = value["__REALTIME_TIMESTAMP"]
        message = value["MESSAGE"]
        if (
            type(timestamp) is not str
            or re.fullmatch(r"[1-9][0-9]{0,19}", timestamp, re.ASCII) is None
            or str(int(timestamp)) != timestamp
            or type(message) is not str
            or value["_SYSTEMD_INVOCATION_ID"] != invocation_id
            or value["_SYSTEMD_UNIT"] != "degen-prod-db-backup.service"
        ):
            raise OperationStateError(
                "scheduled backup journal invocation identity is invalid"
            )
        match = _TASK8_DRY_RUN_LOG_LINE_RE.fullmatch(message)
        if match is None:
            raise OperationStateError(
                f"scheduled backup journal message {index} is not timestamped"
            )
        try:
            datetime.strptime(match.group("timestamp"), "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as exc:
            raise OperationStateError(
                "scheduled backup journal message timestamp is invalid"
            ) from exc
        trusted_epoch = int(timestamp) // 1_000_000
        trusted_epochs.append(trusted_epoch)
        message_epochs.append(trusted_epoch)
        messages.append(message)
        bodies.append(match.group("body"))
    if trusted_epochs != sorted(trusted_epochs):
        raise OperationStateError("scheduled backup journal timestamps are out of order")

    before_indices = [
        index
        for index, body in enumerate(bodies)
        if body.startswith("RCLONE_CONFIG_RECEIPT phase=before ")
    ]
    final_indices = [
        index
        for index, body in enumerate(bodies)
        if body.startswith("RCLONE_CONFIG_RECEIPT phase=final ")
    ]
    create_prefix = "Creating PostgreSQL custom-format backup: "
    create_indices = [
        index for index, body in enumerate(bodies) if body.startswith(create_prefix)
    ]
    verified_prefix = "Remote backup pair verified: "
    verified_indices = [
        index for index, body in enumerate(bodies) if body.startswith(verified_prefix)
    ]
    success_indices = [
        index
        for index, body in enumerate(bodies)
        if body == "Backup completed successfully"
    ]
    if any(
        len(indices) != 1
        for indices in (
            before_indices,
            final_indices,
            create_indices,
            verified_indices,
            success_indices,
        )
    ):
        raise OperationStateError(
            "scheduled backup journal lifecycle is missing or ambiguous"
        )
    before_index = before_indices[0]
    create_index = create_indices[0]
    verified_index = verified_indices[0]
    success_index = success_indices[0]
    final_index = final_indices[0]
    if not (
        before_index < create_index < verified_index < success_index < final_index
    ):
        raise OperationStateError("scheduled backup journal lifecycle order is invalid")
    if any(
        body.startswith(("ERROR:", "WARNING:"))
        for body in bodies
    ):
        raise OperationStateError("scheduled backup journal contains a failure record")
    before_receipt = _task8_decode_rclone_config_receipt(
        bodies[before_index],
        expected_phase="before",
    )
    final_receipt = _task8_decode_rclone_config_receipt(
        bodies[final_index],
        expected_phase="final",
    )
    metadata_fields = (
        "path",
        "sha256",
        "device",
        "inode",
        "uid",
        "gid",
        "mode",
        "links",
        "size",
        "mtime_ns",
    )
    unchanged = all(
        before_receipt[field] == final_receipt[field]
        for field in metadata_fields
    )
    if (
        (final_receipt["change"] == "unchanged" and not unchanged)
        or (
            final_receipt["change"] == "possible-oauth-refresh"
            and unchanged
        )
    ):
        raise OperationStateError(
            "scheduled backup journal rclone receipt change is inconsistent"
        )
    dump_name = _task8_validate_remote_name(
        bodies[create_index][len(create_prefix) :],
        "scheduled backup dump name",
    )
    match = _BACKUP_NAME_RE.fullmatch(dump_name)
    if (
        match is None
        or match.group("sidecar") is not None
        or match.group("prefix") != prefix
    ):
        raise OperationStateError("scheduled backup dump name is invalid")
    try:
        datetime.strptime(match.group("stamp"), "%Y%m%dT%H%M%SZ")
    except ValueError as exc:
        raise OperationStateError("scheduled backup dump timestamp is invalid") from exc
    sidecar_name = f"{dump_name}.sha256"
    expected_verified = f"{verified_prefix}{dump_name} and {sidecar_name}"
    if bodies[verified_index] != expected_verified:
        raise OperationStateError(
            "scheduled backup remote verification does not match the created pair"
        )
    return {
        "dump_name": dump_name,
        "sidecar_name": sidecar_name,
        "stamp": match.group("stamp"),
        "success_epoch": message_epochs[success_index],
        "final_epoch": message_epochs[final_index],
        "before_receipt": before_receipt,
        "final_receipt": final_receipt,
        "messages": messages,
        "trusted_epochs": trusted_epochs,
        "log_suffix": "".join(f"{message}\n" for message in messages),
    }


def _task8_decode_observation_remote_inventory(
    raw: str,
) -> tuple[list[str], list[str]]:
    if (
        type(raw) is not str
        or _task8_utf8_size(raw, "observation remote inventory") > _MAX_STATE_BYTES
        or not raw
        or not raw.endswith("\n")
        or "\r" in raw
        or "\x00" in raw
    ):
        raise OperationStateError("observation remote inventory encoding is invalid")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise OperationStateError("observation remote inventory is invalid") from exc
    if type(value) is not list or len(value) > _MAX_BACKUP_ENTRIES:
        raise OperationStateError("observation remote inventory is not a bounded list")
    names: list[str] = []
    for index, item in enumerate(value):
        if type(item) is not dict:
            raise OperationStateError(
                "observation remote inventory entry is not an object"
            )
        path = item.get("Path")
        name = item.get("Name")
        size = item.get("Size")
        is_dir = item.get("IsDir")
        if (
            type(path) is not str
            or type(name) is not str
            or path != name
            or is_dir is not False
            or type(size) is not int
            or size < 0
        ):
            raise OperationStateError(
                "observation remote inventory contains a directory or nested object"
            )
        names.append(
            _task8_validate_remote_name(
                name,
                f"observation remote inventory[{index}]",
            )
        )
    validated = _task8_validate_canonical_remote_names(
        names,
        "observation remote inventory",
        require_sorted=False,
    )
    canonical = sorted(validated)
    return canonical, [name.casefold() for name in canonical]


def _task8_decode_observation_remote_stat(
    raw: str,
    *,
    expected_name: str,
    expected_size: int,
) -> dict[str, object]:
    name = _task8_validate_remote_name(
        expected_name,
        "observation remote dump name",
    )
    if type(expected_size) is not int or expected_size < 0:
        raise OperationStateError("observation remote dump size is invalid")
    if (
        type(raw) is not str
        or _task8_utf8_size(raw, "observation remote stat") > _MAX_COMMAND_OUTPUT_BYTES
        or not raw
        or not raw.endswith("\n")
        or "\r" in raw
        or "\x00" in raw
    ):
        raise OperationStateError("observation remote stat encoding is invalid")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise OperationStateError("observation remote stat is invalid") from exc
    mod_time = value.get("ModTime") if isinstance(value, dict) else None
    if (
        type(value) is not dict
        or value.get("Path") != name
        or value.get("Name") != name
        or type(value.get("Size")) is not int
        or value.get("Size") != expected_size
        or value.get("IsDir") is not False
        or type(mod_time) is not str
        or not mod_time
    ):
        raise OperationStateError("observation remote stat identity is invalid")
    mod_time_match = _RCLONE_MODTIME_RE.fullmatch(mod_time)
    if mod_time_match is None:
        raise OperationStateError("observation remote stat ModTime is invalid")
    fraction = mod_time_match.group("fraction")
    normalized_fraction = (
        "" if fraction is None else "." + fraction[:6].ljust(6, "0")
    )
    zone = mod_time_match.group("zone")
    if zone != "Z" and (
        int(zone[1:3]) > 23
        or int(zone[4:6]) > 59
    ):
        raise OperationStateError("observation remote stat ModTime is invalid")
    normalized_zone = "+00:00" if zone == "Z" else zone
    try:
        parsed_mod_time = datetime.fromisoformat(
            mod_time_match.group("base")
            + normalized_fraction
            + normalized_zone
        )
    except ValueError as exc:
        raise OperationStateError("observation remote stat ModTime is invalid") from exc
    if parsed_mod_time.tzinfo is None or parsed_mod_time.utcoffset() is None:
        raise OperationStateError("observation remote stat ModTime is not aware")
    object_id = value.get("ID")
    if object_id is not None and (
        type(object_id) is not str
        or not object_id
        or len(object_id.encode("utf-8")) > _MAX_DRY_RUN_NAME_BYTES
        or any(ord(character) < 0x20 for character in object_id)
    ):
        raise OperationStateError("observation remote stat object ID is invalid")
    return value


def _task8_decode_observation_remote_hash(
    raw: str,
    *,
    expected_name: str,
) -> str:
    name = _task8_validate_remote_name(
        expected_name,
        "observation remote hash name",
    )
    match = re.fullmatch(
        rf"(?P<digest>[0-9a-f]{{64}})  {re.escape(name)}\n",
        raw,
        re.ASCII,
    )
    if match is None:
        raise OperationStateError("observation remote hash output is invalid")
    return match.group("digest")


def _task8_decode_observation_remote_sidecar(
    raw: str,
    *,
    expected_name: str,
    expected_sha256: str,
) -> str:
    name = _task8_validate_remote_name(
        expected_name,
        "observation remote sidecar dump name",
    )
    _require_hash(expected_sha256, "observation remote sidecar digest")
    expected = f"{expected_sha256}  {name}\n"
    if raw != expected:
        raise OperationStateError("observation remote sidecar output is invalid")
    return raw


def _task8_observation_result_sha256(
    purpose: str,
    result_domain: object,
) -> str:
    if purpose not in _TASK8_OBSERVE_PURPOSES:
        raise OperationStateError("observation rclone result purpose is invalid")
    return _task8_evidence_sha256(
        "observation-rclone-result",
        {"purpose": purpose, "result_domain": result_domain},
    )


def _task8_validate_observation_correlation(
    state: dict[str, object],
    *,
    service: dict[str, object],
    journal: dict[str, object],
    local: dict[str, object],
) -> dict[str, object]:
    if state.get("phase") != "observing":
        raise OperationStateError("observation correlation requires observing state")
    install = state.get("install")
    policy = state.get("policy")
    prior_runtime = state.get("prior_runtime")
    if (
        not isinstance(install, dict)
        or not isinstance(policy, dict)
        or not isinstance(prior_runtime, dict)
        or not isinstance(service, dict)
        or not isinstance(journal, dict)
        or not isinstance(local, dict)
    ):
        raise OperationStateError("observation correlation evidence is incomplete")
    completed_epoch = install.get("completed_epoch")
    enabled_epoch = policy.get("enabled_epoch")
    if type(completed_epoch) is not int or type(enabled_epoch) is not int:
        raise OperationStateError("observation cutoff provenance is invalid")
    cutoffs = [completed_epoch, enabled_epoch]
    original_trigger = prior_runtime.get("preinstall_trigger_epoch")
    if original_trigger is not None:
        if type(original_trigger) is not int:
            raise OperationStateError("observation prior trigger is invalid")
        cutoffs.append(original_trigger)
    cutoff = max(cutoffs)
    runtime_baseline = _task8_observation_runtime_baseline(state)
    run_epoch = runtime_baseline["preinstall_trigger_epoch"]
    assert isinstance(run_epoch, int)
    entry_epoch = _phase_epoch(state, "observing")
    try:
        start_epoch = int(service["start_epoch"])
        exit_epoch = int(service["exit_epoch"])
        success_epoch = int(journal["success_epoch"])
        final_epoch = int(journal["final_epoch"])
    except (KeyError, TypeError, ValueError) as exc:
        raise OperationStateError(
            "observation service or journal timestamp is invalid"
        ) from exc
    if any(
        type(value) is not int
        for value in (
            service.get("start_epoch"),
            service.get("exit_epoch"),
            journal.get("success_epoch"),
            journal.get("final_epoch"),
        )
    ):
        raise OperationStateError("observation timestamps must be exact integers")
    invocation_id = service.get("invocation_id")
    start_monotonic_usec = service.get("start_monotonic_usec")
    timer_trigger_epoch = service.get("timer_trigger_epoch")
    timer_trigger_monotonic_usec = service.get("timer_trigger_monotonic_usec")
    if (
        type(invocation_id) is not str
        or _SYSTEMD_INVOCATION_ID_RE.fullmatch(invocation_id) is None
        or type(start_monotonic_usec) is not int
        or type(timer_trigger_epoch) is not int
        or type(timer_trigger_monotonic_usec) is not int
        or timer_trigger_epoch != run_epoch
        or start_monotonic_usec < timer_trigger_monotonic_usec
        or start_monotonic_usec - timer_trigger_monotonic_usec > 30_000_000
    ):
        raise OperationStateError(
            "observation service invocation is not bound to the timer trigger"
        )
    pairs = local.get("pairs")
    if type(pairs) is not list or len(pairs) != 2:
        raise OperationStateError("observation requires exactly two local pairs")
    newest = pairs[0]
    if not isinstance(newest, dict):
        raise OperationStateError("observation newest local pair is invalid")
    dump_name = journal.get("dump_name")
    sidecar_name = journal.get("sidecar_name")
    stamp = journal.get("stamp")
    if (
        type(dump_name) is not str
        or type(sidecar_name) is not str
        or type(stamp) is not str
        or newest.get("dump_name") != dump_name
        or newest.get("sidecar_name") != sidecar_name
        or newest.get("stamp") != stamp
        or local.get("newest_dump") != dump_name
    ):
        raise OperationStateError(
            "observation journal and newest local pair do not correlate"
        )
    match = _BACKUP_NAME_RE.fullmatch(dump_name)
    if (
        match is None
        or match.group("sidecar") is not None
        or match.group("stamp") != stamp
        or sidecar_name != f"{dump_name}.sha256"
    ):
        raise OperationStateError("observation scheduled dump identity is invalid")
    try:
        filename_epoch = int(
            datetime.strptime(stamp, "%Y%m%dT%H%M%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
    except ValueError as exc:
        raise OperationStateError(
            "observation scheduled dump timestamp is invalid"
        ) from exc
    if newest.get("stamp_epoch") != filename_epoch:
        raise OperationStateError(
            "observation local filename timestamp commitment is invalid"
        )
    dump_mtime_ns = newest.get("dump_mtime_ns")
    sidecar_mtime_ns = newest.get("sidecar_mtime_ns")
    if (
        type(dump_mtime_ns) is not int
        or type(sidecar_mtime_ns) is not int
        or dump_mtime_ns < 0
        or sidecar_mtime_ns < 0
    ):
        raise OperationStateError("observation local artifact mtime is invalid")
    dump_mtime = dump_mtime_ns // 1_000_000_000
    sidecar_mtime = sidecar_mtime_ns // 1_000_000_000
    if min(
        run_epoch,
        start_epoch,
        success_epoch,
        filename_epoch,
        dump_mtime,
        sidecar_mtime,
    ) <= cutoff:
        raise OperationStateError(
            "observation scheduled run evidence is not strictly newer than the cutoff"
        )
    if not (
        run_epoch <= start_epoch <= run_epoch + 30
        and start_epoch <= filename_epoch <= success_epoch
        and start_epoch <= dump_mtime <= entry_epoch
        and start_epoch <= sidecar_mtime <= entry_epoch
        and success_epoch <= final_epoch <= exit_epoch <= entry_epoch
    ):
        raise OperationStateError(
            "observation trigger, service, journal, and artifact timestamp order is invalid"
        )
    trusted_epochs = journal.get("trusted_epochs")
    if (
        type(trusted_epochs) is not list
        or not trusted_epochs
        or any(type(value) is not int for value in trusted_epochs)
        or trusted_epochs != sorted(trusted_epochs)
        or trusted_epochs[-1] > entry_epoch
    ):
        raise OperationStateError("observation journal trusted timestamp order is invalid")
    log_suffix = journal.get("log_suffix")
    if (
        type(log_suffix) is not str
        or local.get("log_suffix_sha256")
        != hashlib.sha256(log_suffix.encode("utf-8")).hexdigest()
    ):
        raise OperationStateError(
            "observation local log does not match the journal invocation"
        )
    journal_summary = {
        "service": service,
        "journal": {
            key: value
            for key, value in journal.items()
            if key != "log_suffix"
        },
        "log_suffix_sha256": local["log_suffix_sha256"],
        "run_epoch": run_epoch,
        "cutoff": cutoff,
    }
    local_summary = {
        key: value
        for key, value in local.items()
        if key != "pairs"
    }
    local_summary["pairs"] = [
        {
            key: value
            for key, value in pair.items()
            if key != "sidecar_text"
        }
        for pair in pairs
        if isinstance(pair, dict)
    ]
    return {
        "run_epoch": run_epoch,
        "journal_sha256": _task8_evidence_sha256(
            "observation-journal",
            journal_summary,
        ),
        "local_sha256": _task8_evidence_sha256(
            "observation-local",
            local_summary,
        ),
    }


def _task8_require_observation_runtime_entry(
    state: dict[str, object],
    runtime: dict[str, object],
) -> None:
    prior_runtime = state.get("prior_runtime")
    install = state.get("install")
    policy = state.get("policy")
    if (
        not isinstance(prior_runtime, dict)
        or not isinstance(install, dict)
        or not isinstance(policy, dict)
    ):
        raise OperationStateError("observation runtime provenance is incomplete")
    _validate_prior_runtime(runtime, "observation runtime baseline")
    if (
        runtime.get("timer_enabled") is not prior_runtime.get("timer_enabled")
        or runtime.get("timer_active") is not prior_runtime.get("timer_active")
        or runtime.get("pids") != prior_runtime.get("pids")
    ):
        raise OperationStateError(
            "observation protected timer or process identities changed"
        )
    trigger = runtime.get("preinstall_trigger_epoch")
    completed = install.get("completed_epoch")
    enabled = policy.get("enabled_epoch")
    if type(trigger) is not int or type(completed) is not int or type(enabled) is not int:
        raise OperationStateError("observation scheduled trigger is missing")
    cutoffs = [completed, enabled]
    old_trigger = prior_runtime.get("preinstall_trigger_epoch")
    if old_trigger is not None:
        if type(old_trigger) is not int:
            raise OperationStateError("observation prior trigger is invalid")
        cutoffs.append(old_trigger)
    if trigger <= max(cutoffs):
        raise OperationStateError(
            "observation scheduled trigger is not newer than the rollout cutoff"
        )


def _task8_run_observation_readonly_command(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    argv: tuple[str, ...],
    label: str,
    runtime_lock_fd: int,
    max_output_bytes: int = _MAX_COMMAND_OUTPUT_BYTES,
    revalidate: Callable[[], None] | None = None,
) -> str:
    if type(runtime_lock_fd) is not int or runtime_lock_fd < 0:
        raise OperationStateError("observation runtime lock descriptor is invalid")
    with _task8_open_exact_guard_recovery_material(
        context,
        state,
        helper_route="installed",
    ) as material:
        _revalidate_transaction_material(material)
        if revalidate is not None:
            revalidate()
        _task8_force_checkpoint(
            context,
            binding,
            state,
            "observation-readonly:" + label,
        )
        if revalidate is not None:
            revalidate()
        completed = _checked_command(
            context,
            argv,
            (runtime_lock_fd,),
            label,
            max_output_bytes=max_output_bytes,
        )
        try:
            if completed.stderr:
                raise OperationStateError(f"{label} returned unexpected stderr")
            output = completed.stdout
        finally:
            _scrub_completed_process(completed)
        if revalidate is not None:
            revalidate()
        _revalidate_transaction_material(material)
        return output


def _task8_parse_observation_epoch(raw: str, label: str) -> int:
    if (
        type(raw) is not str
        or not raw.endswith("\n")
        or re.fullmatch(r"[1-9][0-9]{0,19}\n", raw, re.ASCII) is None
    ):
        raise OperationStateError(f"{label} epoch is invalid")
    return int(raw[:-1])


def _task8_capture_observation_service(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    runtime_lock_fd: int,
    revalidate: Callable[[], None] | None = None,
) -> dict[str, object]:
    raw = _task8_run_observation_readonly_command(
        context,
        binding,
        state,
        argv=(
            _SYSTEMCTL_EXECUTABLE,
            "show",
            "degen-prod-db-backup.service",
            *(
                f"--property={name}"
                for name in _TASK8_OBSERVATION_SERVICE_PROPERTIES
            ),
            "--no-pager",
        ),
        label="scheduled backup service observation",
        runtime_lock_fd=runtime_lock_fd,
        revalidate=revalidate,
    )
    decoded = _task8_decode_observation_service(raw)
    start_raw = _task8_run_observation_readonly_command(
        context,
        binding,
        state,
        argv=(
            _DATE_EXECUTABLE,
            "--date",
            decoded["start_timestamp"],
            "+%s",
        ),
        label="scheduled backup start conversion",
        runtime_lock_fd=runtime_lock_fd,
        revalidate=revalidate,
    )
    exit_raw = _task8_run_observation_readonly_command(
        context,
        binding,
        state,
        argv=(
            _DATE_EXECUTABLE,
            "--date",
            decoded["exit_timestamp"],
            "+%s",
        ),
        label="scheduled backup exit conversion",
        runtime_lock_fd=runtime_lock_fd,
        revalidate=revalidate,
    )
    return {
        "invocation_id": decoded["invocation_id"],
        "start_monotonic_usec": decoded["start_monotonic_usec"],
        "start_epoch": _task8_parse_observation_epoch(
            start_raw,
            "scheduled backup start",
        ),
        "exit_epoch": _task8_parse_observation_epoch(
            exit_raw,
            "scheduled backup exit",
        ),
    }


def _task8_capture_observation_timer(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    runtime_lock_fd: int,
    revalidate: Callable[[], None] | None = None,
) -> dict[str, object]:
    raw = _task8_run_observation_readonly_command(
        context,
        binding,
        state,
        argv=(
            _SYSTEMCTL_EXECUTABLE,
            "show",
            "degen-prod-db-backup.timer",
            *(
                f"--property={name}"
                for name in _TASK8_OBSERVATION_TIMER_PROPERTIES
            ),
            "--no-pager",
        ),
        label="scheduled backup timer observation",
        runtime_lock_fd=runtime_lock_fd,
        revalidate=revalidate,
    )
    decoded = _task8_decode_observation_timer(raw)
    monotonic_raw = _task8_run_observation_readonly_command(
        context,
        binding,
        state,
        argv=(
            _BUSCTL_EXECUTABLE,
            "get-property",
            "org.freedesktop.systemd1",
            "/org/freedesktop/systemd1/unit/degen_2dprod_2ddb_2dbackup_2etimer",
            "org.freedesktop.systemd1.Timer",
            "LastTriggerUSecMonotonic",
        ),
        label="scheduled backup timer monotonic observation",
        runtime_lock_fd=runtime_lock_fd,
        revalidate=revalidate,
    )
    trigger_monotonic_usec = _task8_decode_observation_timer_monotonic(
        monotonic_raw
    )
    epoch_raw = _task8_run_observation_readonly_command(
        context,
        binding,
        state,
        argv=(
            _DATE_EXECUTABLE,
            "--date",
            str(decoded["trigger_timestamp"]),
            "+%s",
        ),
        label="scheduled backup timer trigger conversion",
        runtime_lock_fd=runtime_lock_fd,
        revalidate=revalidate,
    )
    return {
        "trigger_epoch": _task8_parse_observation_epoch(
            epoch_raw,
            "scheduled backup timer trigger",
        ),
        "trigger_monotonic_usec": trigger_monotonic_usec,
    }


def _task8_capture_observation_journal(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    runtime_lock_fd: int,
    invocation_id: str,
    revalidate: Callable[[], None] | None = None,
) -> dict[str, object]:
    effective = state.get("effective_config")
    if not isinstance(effective, dict):
        raise OperationStateError("observation journal configuration is missing")
    raw = _task8_run_observation_readonly_command(
        context,
        binding,
        state,
        argv=(
            _JOURNALCTL_EXECUTABLE,
            "--unit=degen-prod-db-backup.service",
            f"_SYSTEMD_INVOCATION_ID={invocation_id}",
            "--output=json",
            "--no-pager",
        ),
        label="scheduled backup journal observation",
        runtime_lock_fd=runtime_lock_fd,
        max_output_bytes=_MAX_STATE_BYTES,
        revalidate=revalidate,
    )
    return _task8_decode_observation_journal(
        raw,
        expected_invocation_id=invocation_id,
        expected_prefix=str(effective["BACKUP_PREFIX"]),
    )


def _task8_remote_dry_run_candidate_sha256(delete_names: list[str]) -> str:
    names = _task8_validate_canonical_remote_names(
        delete_names,
        "remote dry-run delete names",
        require_sorted=False,
    )
    return _task8_evidence_sha256(
        "dry-run-candidates",
        {"delete_names": names},
    )


def _task8_plan_remote_inventory(
    inventory_names: list[str],
    *,
    prefix: str,
    now: datetime,
    daily: int,
    weekly: int,
    monthly: int,
) -> dict[str, list[str]]:
    validated_prefix = _require_string(
        prefix,
        "remote retention prefix",
        nonempty=True,
    )
    if _BACKUP_PREFIX_RE.fullmatch(validated_prefix) is None:
        raise OperationStateError("remote retention prefix is invalid")
    inventory = _task8_validate_canonical_remote_names(
        inventory_names,
        "remote retention inventory",
        require_sorted=True,
    )
    if (
        not isinstance(now, datetime)
        or now.tzinfo is None
        or now.utcoffset() is None
    ):
        raise OperationStateError("remote retention now must be timezone-aware")
    for label, count in (
        ("daily", daily),
        ("weekly", weekly),
        ("monthly", monthly),
    ):
        if type(count) is not int or count < 0:
            raise OperationStateError(
                f"remote retention {label} count must be nonnegative"
            )

    dumps: dict[str, datetime] = {}
    checksums: set[str] = set()
    recognized: dict[str, datetime] = {}
    protected: list[tuple[str, str]] = []
    for name in inventory:
        base = name[:-7] if name.endswith(".sha256") else name
        match = _BACKUP_NAME_RE.fullmatch(base)
        if (
            match is None
            or match.group("sidecar") is not None
            or match.group("prefix") != validated_prefix
        ):
            protected.append((name, "unknown-name"))
            continue
        try:
            parsed = datetime.strptime(
                match.group("stamp"),
                "%Y%m%dT%H%M%SZ",
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            protected.append((name, "unparseable-timestamp"))
            continue
        recognized[name] = parsed
        if name.endswith(".sha256"):
            checksums.add(name)
        else:
            dumps[name] = parsed

    future_names = {
        name for name, timestamp in recognized.items() if timestamp > now
    }
    complete: list[tuple[str, datetime]] = []
    for dump, timestamp in dumps.items():
        checksum = f"{dump}.sha256"
        if dump not in future_names and checksum in checksums:
            complete.append((dump, timestamp))
    for name in sorted(future_names):
        protected.append((name, "future-timestamp"))

    complete_names = {
        name
        for dump, _timestamp in complete
        for name in (dump, f"{dump}.sha256")
    }
    for name in sorted(set(recognized) - complete_names - future_names):
        protected.append((name, "incomplete-pair"))

    complete.sort(key=lambda item: (item[1], item[0]), reverse=True)
    reasons: dict[str, set[str]] = {}
    if complete:
        reasons.setdefault(complete[0][0], set()).add("newest")
    group_specs: tuple[
        tuple[str, int, Callable[[datetime], object]],
        ...,
    ] = (
        ("daily", daily, lambda value: value.date()),
        ("weekly", weekly, lambda value: value.isocalendar()[:2]),
        ("monthly", monthly, lambda value: (value.year, value.month)),
    )
    for reason, count, bucket_key in group_specs:
        seen: set[object] = set()
        for dump, timestamp in complete:
            bucket = bucket_key(timestamp)
            if bucket in seen:
                continue
            if len(seen) >= count:
                break
            seen.add(bucket)
            reasons.setdefault(dump, set()).add(reason)

    keep_names = [
        name
        for dump, _timestamp in complete
        if dump in reasons
        for name in (dump, f"{dump}.sha256")
    ]
    delete_names = [
        name
        for dump, _timestamp in reversed(complete)
        if dump not in reasons
        for name in (dump, f"{dump}.sha256")
    ]
    return {
        "keep_names": keep_names,
        "protected_names": [
            name for name, _reason in sorted(protected)
        ],
        "delete_names": delete_names,
    }


def _task8_run_remote_retention_planner(
    context: OperationsContext,
    state: dict[str, object],
    inventory_names: list[str],
    *,
    runtime_lock_fd: int,
    expected_now_override: datetime | None = None,
) -> dict[str, list[str]]:
    inventory = _task8_validate_canonical_remote_names(
        inventory_names,
        "remote retention inventory",
        require_sorted=True,
    )
    transaction = state.get("active_transaction")
    transaction_kind = transaction.get("kind") if isinstance(transaction, dict) else None
    if transaction_kind not in {"dry_run", "policy", "observe"}:
        raise OperationStateError(
            "remote retention planner requires an active Task 8 transaction"
        )
    assert isinstance(transaction_kind, str)
    entries = _task8_attempt_entries(state, transaction_kind)
    if (
        not entries
        or entries[-1]["epoch"] != transaction.get("started_epoch")
    ):
        raise OperationStateError("remote retention planner entry identity is invalid")
    if transaction_kind == "observe":
        expected_now = expected_now_override
        if (
            not isinstance(expected_now, datetime)
            or expected_now.tzinfo is None
            or expected_now.utcoffset() is None
        ):
            raise OperationStateError(
                "observation retention planner requires the scheduled run timestamp"
            )
        expected_now = expected_now.astimezone(timezone.utc)
    else:
        if expected_now_override is not None:
            raise OperationStateError(
                "remote retention planner timestamp override is observation-only"
            )
        try:
            expected_now = datetime.fromtimestamp(
                int(entries[-1]["epoch"]),
                tz=timezone.utc,
            )
        except (OverflowError, OSError, ValueError) as exc:
            raise OperationStateError(
                "remote retention planner entry epoch is invalid"
            ) from exc
    effective = state.get("effective_config")
    if not isinstance(effective, dict):
        raise OperationStateError("remote retention planner configuration is missing")
    if effective.get("RETENTION_PLANNER") != "/usr/local/sbin/degen-prod-db-retention":
        raise OperationStateError("remote retention planner path is not fixed")
    prefix = _require_string(
        effective.get("BACKUP_PREFIX"),
        "remote retention planner prefix",
        nonempty=True,
    )
    if _BACKUP_PREFIX_RE.fullmatch(prefix) is None:
        raise OperationStateError("remote retention planner prefix is invalid")

    def count(key: str) -> int:
        raw = effective.get(key)
        if (
            type(raw) is not str
            or len(raw) > 20
            or re.fullmatch(r"[0-9]+", raw, re.ASCII) is None
        ):
            raise OperationStateError(f"remote retention planner {key} is invalid")
        return int(raw)

    daily = count("KEEP_REMOTE_DAILY")
    weekly = count("KEEP_REMOTE_WEEKLY")
    monthly = count("KEEP_REMOTE_MONTHLY")
    payload = (
        "" if not inventory else "\n".join(inventory) + "\n"
    ).encode("ascii")
    if len(payload) > _MAX_STATE_BYTES:
        raise OperationStateError("remote retention planner inventory exceeds the size limit")
    if type(runtime_lock_fd) is not int or runtime_lock_fd < 0:
        raise OperationStateError("remote retention planner runtime lock is invalid")
    try:
        os.fstat(runtime_lock_fd)
    except OSError as exc:
        raise OperationStateError("remote retention planner runtime lock is invalid") from exc

    read_fd: int | None = None
    write_fd: int | None = None
    writer: threading.Thread | None = None
    writer_started = False
    writer_errors: list[BaseException] = []
    completed: subprocess.CompletedProcess[str] | None = None

    def write_payload(descriptor: int) -> None:
        try:
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise OperationStateError(
                        "remote retention planner transport made no progress"
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
        if hasattr(os, "pipe2") and os.name == "posix":
            read_fd, write_fd = os.pipe2(getattr(os, "O_CLOEXEC", 0))
        else:
            read_fd, write_fd = os.pipe()
            os.set_inheritable(read_fd, False)
            os.set_inheritable(write_fd, False)
        writer = threading.Thread(
            target=write_payload,
            args=(write_fd,),
            name="degen-retention-inventory-writer",
            daemon=True,
        )
        writer.start()
        writer_started = True
        write_fd = None
        argv = (
            sys.executable,
            "-c",
            _INHERITED_FD_EXEC_SHIM,
            "stdin",
            str(read_fd),
            "/usr/local/sbin/degen-prod-db-retention",
            "degen-prod-db-retention",
            "--mode",
            "remote",
            "--prefix",
            prefix,
            "--now",
            expected_now.strftime("%Y%m%dT%H%M%SZ"),
            "--daily",
            str(daily),
            "--weekly",
            str(weekly),
            "--monthly",
            str(monthly),
            "--format",
            "json",
        )
        completed = _checked_command(
            context,
            argv,
            (read_fd, runtime_lock_fd),
            "remote retention planner",
            max_output_bytes=_MAX_STATE_BYTES,
        )
    except OperationStateError:
        raise
    except Exception:
        raise OperationStateError("remote retention planner failed") from None
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
                raise OperationStateError(
                    "remote retention planner transport did not terminate"
                )
    if writer_errors:
        if completed is not None:
            _scrub_completed_process(completed)
        raise OperationStateError("remote retention planner transport failed")
    if completed is None:
        raise OperationStateError("remote retention planner returned no result")
    try:
        if completed.stderr:
            raise OperationStateError(
                "remote retention planner returned unexpected stderr"
            )
        installed_plan = _task8_decode_remote_retention_plan(
            completed.stdout,
            expected_prefix=prefix,
            inventory_names=inventory,
            expected_now=expected_now,
        )
        independent_plan = _task8_plan_remote_inventory(
            inventory,
            prefix=prefix,
            now=expected_now,
            daily=daily,
            weekly=weekly,
            monthly=monthly,
        )
        if installed_plan != independent_plan:
            raise OperationStateError(
                "installed remote retention planner differs from independent policy"
            )
        return installed_plan
    finally:
        _scrub_completed_process(completed)


def _task8_plan_remote_inventory_for_state(
    state: dict[str, object],
    inventory_names: list[str],
) -> dict[str, list[str]]:
    transaction = state.get("active_transaction")
    transaction_kind = transaction.get("kind") if isinstance(transaction, dict) else None
    if transaction_kind not in {"dry_run", "policy"}:
        raise OperationStateError(
            "remote retention plan requires an active dry_run or policy transaction"
        )
    assert isinstance(transaction_kind, str)
    entries = _task8_attempt_entries(state, transaction_kind)
    if not entries or entries[-1]["epoch"] != transaction.get("started_epoch"):
        raise OperationStateError("remote dry-run plan entry identity is invalid")
    try:
        now = datetime.fromtimestamp(int(entries[-1]["epoch"]), tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise OperationStateError("remote dry-run plan entry epoch is invalid") from exc
    effective = state.get("effective_config")
    if not isinstance(effective, dict):
        raise OperationStateError("remote dry-run configuration is missing")

    def count(key: str) -> int:
        raw = effective.get(key)
        if (
            type(raw) is not str
            or len(raw) > 20
            or re.fullmatch(r"[0-9]+", raw, re.ASCII) is None
        ):
            raise OperationStateError(f"remote dry-run {key} is invalid")
        return int(raw)

    return _task8_plan_remote_inventory(
        inventory_names,
        prefix=str(effective["BACKUP_PREFIX"]),
        now=now,
        daily=count("KEEP_REMOTE_DAILY"),
        weekly=count("KEEP_REMOTE_WEEKLY"),
        monthly=count("KEEP_REMOTE_MONTHLY"),
    )


def _task8_run_normal_dry_run_audited_command(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    purpose: str,
    runtime_lock_fd: int,
    expected_delete_names: list[str] | None = None,
) -> tuple[list[str], list[str]] | list[str]:
    effective = state.get("effective_config")
    if not isinstance(effective, dict):
        raise OperationStateError("normal dry-run configuration is missing")
    inventory_argv = (
        _RCLONE_EXECUTABLE,
        "--config",
        _RCLONE_CONFIG_PATH,
        "lsf",
        str(effective["RCLONE_REMOTE_PATH"]),
        "--files-only",
        "--max-depth",
        "1",
    )
    if purpose in {
        "dry-run-inventory-before",
        "dry-run-inventory-after",
    }:
        argv = inventory_argv
        label = purpose.replace("-", " ")
        max_output_bytes = _MAX_STATE_BYTES
    elif purpose == "dry-run-runtime":
        if expected_delete_names is None:
            raise OperationStateError("normal dry-run runtime plan is missing")
        transaction = state.get("active_transaction")
        if not isinstance(transaction, dict) or transaction.get("kind") != "dry_run":
            raise OperationStateError("normal dry-run runtime transaction is invalid")
        entries = _task8_attempt_entries(state, "dry_run")
        if (
            not entries
            or entries[-1]["epoch"] != transaction.get("started_epoch")
        ):
            raise OperationStateError("normal dry-run runtime entry identity is invalid")
        try:
            frozen_now = datetime.fromtimestamp(
                int(entries[-1]["epoch"]),
                tz=timezone.utc,
            ).strftime("%Y%m%dT%H%M%SZ")
        except (OverflowError, OSError, ValueError) as exc:
            raise OperationStateError(
                "normal dry-run runtime entry epoch is invalid"
            ) from exc
        argv = (
            str(_host_path(context, "/usr/local/sbin/degen-prod-db-backup")),
            "remote-retention-dry-run",
            "--lock-fd",
            str(runtime_lock_fd),
            "--now",
            frozen_now,
        )
        label = "remote retention dry-run runtime"
        max_output_bytes = _MAX_STATE_BYTES
    else:
        raise OperationStateError("normal dry-run purpose is invalid")

    completed: subprocess.CompletedProcess[str] | None = None
    command_error: Exception | None = None
    decoded: tuple[list[str], list[str]] | list[str] | None = None
    result_sha256: str | None = None
    with _task8_open_exact_guard_recovery_material(
        context,
        state,
        helper_route="installed",
    ) as material:
        _revalidate_transaction_material(material)
        _task8_begin_rclone_audit(
            context,
            binding,
            state,
            purpose,
        )
        _revalidate_transaction_material(material)
        try:
            completed = _checked_command(
                context,
                argv,
                (runtime_lock_fd,),
                label,
                max_output_bytes=max_output_bytes,
            )
            if completed.stderr:
                raise OperationStateError(f"{label} returned unexpected stderr")
            if purpose in {
                "dry-run-inventory-before",
                "dry-run-inventory-after",
            }:
                names, folded = _task8_decode_remote_lsf_inventory(completed.stdout)
                decoded = (names, folded)
                result_sha256 = _task8_dry_run_result_sha256(
                    purpose,
                    {
                        "inventory_names": names,
                        "casefold_names": folded,
                    },
                )
            else:
                assert expected_delete_names is not None
                candidates = _task8_decode_remote_dry_run_log(
                    completed.stdout,
                    expected_prefix=str(effective["BACKUP_PREFIX"]),
                    expected_delete_names=expected_delete_names,
                )
                decoded = candidates
                result_sha256 = _task8_dry_run_result_sha256(
                    purpose,
                    {
                        "delete_names": candidates,
                        "candidate_sha256": _task8_remote_dry_run_candidate_sha256(
                            candidates
                        ),
                    },
                )
        except Exception as exc:
            command_error = exc
        except BaseException:
            if completed is not None:
                _scrub_completed_process(completed)
            raise
        _revalidate_transaction_material(material)
        try:
            _task8_complete_rclone_audit(
                context,
                binding,
                state,
                outcome="success" if command_error is None else "indeterminate",
                result_sha256=result_sha256 if command_error is None else None,
            )
        except BaseException as audit_error:
            if completed is not None:
                _scrub_completed_process(completed)
            if not isinstance(audit_error, Exception):
                raise
            if command_error is not None:
                raise _Task7RcloneAuditIncompleteError(
                    command_error,
                    audit_error,
                ) from audit_error
            raise
        _revalidate_transaction_material(material)
    if completed is not None:
        _scrub_completed_process(completed)
    if command_error is not None:
        raise command_error
    if decoded is None:
        raise OperationStateError("normal dry-run command returned no result")
    return decoded


def _task8_run_normal_policy_inventory(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    runtime_lock_fd: int,
) -> tuple[list[str], list[str], dict[str, list[str]]]:
    transaction = state.get("active_transaction")
    effective = state.get("effective_config")
    dry_run = state.get("dry_run")
    if (
        not isinstance(transaction, dict)
        or transaction.get("kind") != "policy"
        or not isinstance(effective, dict)
        or not isinstance(dry_run, dict)
    ):
        raise OperationStateError("normal policy inventory provenance is incomplete")
    argv = (
        _RCLONE_EXECUTABLE,
        "--config",
        _RCLONE_CONFIG_PATH,
        "lsf",
        str(effective["RCLONE_REMOTE_PATH"]),
        "--files-only",
        "--max-depth",
        "1",
    )
    purpose = _TASK8_POLICY_PURPOSES[0]
    completed: subprocess.CompletedProcess[str] | None = None
    command_error: Exception | None = None
    inventory_names: list[str] | None = None
    casefold_names: list[str] | None = None
    plan: dict[str, list[str]] | None = None
    result_sha256: str | None = None
    with _task8_open_exact_guard_recovery_material(
        context,
        state,
        helper_route="installed",
    ) as material:
        _revalidate_transaction_material(material)
        _task8_begin_rclone_audit(
            context,
            binding,
            state,
            purpose,
        )
        _revalidate_transaction_material(material)
        try:
            completed = _checked_command(
                context,
                argv,
                (runtime_lock_fd,),
                "policy enable inventory",
                max_output_bytes=_MAX_STATE_BYTES,
            )
            if completed.stderr:
                raise OperationStateError(
                    "policy enable inventory returned unexpected stderr"
                )
            inventory_names, casefold_names = _task8_decode_remote_lsf_inventory(
                completed.stdout
            )
            plan = _task8_run_remote_retention_planner(
                context,
                state,
                inventory_names,
                runtime_lock_fd=runtime_lock_fd,
            )
            candidate_sha256 = _task8_remote_dry_run_candidate_sha256(
                plan["delete_names"]
            )
            observed = {
                "inventory_names": inventory_names,
                "casefold_names": casefold_names,
                "keep_names": plan["keep_names"],
                "protected_names": plan["protected_names"],
                "delete_names": plan["delete_names"],
                "candidate_sha256": candidate_sha256,
            }
            frozen = {
                field: dry_run[field]
                for field in (
                    "inventory_names",
                    "casefold_names",
                    "keep_names",
                    "protected_names",
                    "delete_names",
                    "candidate_sha256",
                )
            }
            if observed != frozen:
                raise OperationStateError(
                    "policy enable inventory or retention plan changed after dry-run approval"
                )
            result_sha256 = _task8_policy_inventory_result_sha256(dry_run)
        except Exception as exc:
            command_error = exc
        except BaseException:
            if completed is not None:
                _scrub_completed_process(completed)
            raise
        _revalidate_transaction_material(material)
        try:
            _task8_complete_rclone_audit(
                context,
                binding,
                state,
                outcome="success" if command_error is None else "indeterminate",
                result_sha256=result_sha256 if command_error is None else None,
            )
        except BaseException as audit_error:
            if completed is not None:
                _scrub_completed_process(completed)
            if not isinstance(audit_error, Exception):
                raise
            if command_error is not None:
                raise _Task7RcloneAuditIncompleteError(
                    command_error,
                    audit_error,
                ) from audit_error
            raise
        _revalidate_transaction_material(material)
    if completed is not None:
        _scrub_completed_process(completed)
    if command_error is not None:
        raise command_error
    if inventory_names is None or casefold_names is None or plan is None:
        raise OperationStateError("normal policy inventory returned no result")
    return inventory_names, casefold_names, plan


def _task8_observation_receipt_file_audit(
    receipt: dict[str, str],
) -> dict[str, object]:
    try:
        return {
            "sha256": receipt["sha256"],
            "inode": int(receipt["inode"]),
            "uid": int(receipt["uid"]),
            "gid": int(receipt["gid"]),
            "mode": int(receipt["mode"], 8),
            "size": int(receipt["size"]),
            "mtime_ns": int(receipt["mtime_ns"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise OperationStateError(
            "scheduled rclone receipt cannot bind to local audit evidence"
        ) from exc


def _task8_run_normal_observation_audited_command(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    purpose: str,
    runtime_lock_fd: int,
    newest_pair: dict[str, object],
) -> object:
    transaction = state.get("active_transaction")
    effective = state.get("effective_config")
    if (
        not isinstance(transaction, dict)
        or transaction.get("kind") != "observe"
        or not isinstance(effective, dict)
        or not isinstance(newest_pair, dict)
        or purpose not in _TASK8_OBSERVE_PURPOSES
    ):
        raise OperationStateError("normal observation command provenance is incomplete")
    remote_path = _require_string(
        effective.get("RCLONE_REMOTE_PATH"),
        "observation remote path",
        nonempty=True,
    )
    dump_name = _task8_validate_remote_name(
        newest_pair.get("dump_name"),
        "observation current dump name",
    )
    sidecar_name = _task8_validate_remote_name(
        newest_pair.get("sidecar_name"),
        "observation current sidecar name",
    )
    if sidecar_name != f"{dump_name}.sha256":
        raise OperationStateError("observation current pair names are invalid")
    dump_size = newest_pair.get("dump_size")
    dump_sha256 = newest_pair.get("dump_sha256")
    sidecar_text = newest_pair.get("sidecar_text")
    if type(dump_size) is not int or dump_size < 0:
        raise OperationStateError("observation current dump size is invalid")
    _require_hash(dump_sha256, "observation current dump digest")
    if type(sidecar_text) is not str:
        raise OperationStateError("observation current sidecar text is invalid")
    inventory_argv = (
        _RCLONE_EXECUTABLE,
        "--config",
        _RCLONE_CONFIG_PATH,
        "lsjson",
        remote_path,
        "--recursive",
    )
    remote_dump = f"{remote_path.rstrip('/')}/{dump_name}"
    remote_sidecar = f"{remote_path.rstrip('/')}/{sidecar_name}"
    if purpose in {"observe-inventory-before", "observe-inventory-after"}:
        argv = inventory_argv
        max_output_bytes = _MAX_STATE_BYTES
    elif purpose.startswith("observe-current-stat-"):
        argv = (
            _RCLONE_EXECUTABLE,
            "--config",
            _RCLONE_CONFIG_PATH,
            "lsjson",
            remote_dump,
            "--stat",
        )
        max_output_bytes = _MAX_COMMAND_OUTPUT_BYTES
    elif purpose.startswith("observe-current-hash-"):
        argv = (
            _RCLONE_EXECUTABLE,
            "--config",
            _RCLONE_CONFIG_PATH,
            "hashsum",
            "SHA-256",
            remote_dump,
            "--download",
        )
        max_output_bytes = _MAX_COMMAND_OUTPUT_BYTES
    else:
        argv = (
            _RCLONE_EXECUTABLE,
            "--config",
            _RCLONE_CONFIG_PATH,
            "cat",
            remote_sidecar,
        )
        max_output_bytes = 256
    completed: subprocess.CompletedProcess[str] | None = None
    command_error: Exception | None = None
    decoded: object = None
    result_sha256: str | None = None
    with _task8_open_exact_guard_recovery_material(
        context,
        state,
        helper_route="installed",
    ) as material:
        _revalidate_transaction_material(material)
        _task8_begin_rclone_audit(context, binding, state, purpose)
        _revalidate_transaction_material(material)
        try:
            completed = _checked_command(
                context,
                argv,
                (runtime_lock_fd,),
                purpose.replace("-", " "),
                max_output_bytes=max_output_bytes,
            )
            if completed.stderr:
                raise OperationStateError(
                    f"{purpose} returned unexpected stderr"
                )
            if purpose in {"observe-inventory-before", "observe-inventory-after"}:
                names, folded = _task8_decode_observation_remote_inventory(
                    completed.stdout
                )
                decoded = (names, folded)
                domain: object = {
                    "inventory_names": names,
                    "casefold_names": folded,
                }
            elif purpose.startswith("observe-current-stat-"):
                decoded = _task8_decode_observation_remote_stat(
                    completed.stdout,
                    expected_name=dump_name,
                    expected_size=dump_size,
                )
                domain = {"stat": decoded}
            elif purpose.startswith("observe-current-hash-"):
                observed_hash = _task8_decode_observation_remote_hash(
                    completed.stdout,
                    expected_name=dump_name,
                )
                if observed_hash != dump_sha256:
                    raise OperationStateError(
                        "observation remote dump hash differs from the local archive"
                    )
                decoded = observed_hash
                domain = {"name": dump_name, "sha256": observed_hash}
            else:
                observed_sidecar = _task8_decode_observation_remote_sidecar(
                    completed.stdout,
                    expected_name=dump_name,
                    expected_sha256=str(dump_sha256),
                )
                if observed_sidecar != sidecar_text:
                    raise OperationStateError(
                        "observation remote sidecar differs from the local sidecar"
                    )
                decoded = observed_sidecar
                domain = {
                    "name": sidecar_name,
                    "sha256": hashlib.sha256(
                        observed_sidecar.encode("ascii")
                    ).hexdigest(),
                }
            result_sha256 = _task8_observation_result_sha256(purpose, domain)
        except Exception as exc:
            command_error = exc
        except BaseException:
            if completed is not None:
                _scrub_completed_process(completed)
            raise
        _revalidate_transaction_material(material)
        try:
            _task8_complete_rclone_audit(
                context,
                binding,
                state,
                outcome="success" if command_error is None else "indeterminate",
                result_sha256=result_sha256 if command_error is None else None,
            )
        except BaseException as audit_error:
            if completed is not None:
                _scrub_completed_process(completed)
            if not isinstance(audit_error, Exception):
                raise
            if command_error is not None:
                raise _Task7RcloneAuditIncompleteError(
                    command_error,
                    audit_error,
                ) from audit_error
            raise
        _revalidate_transaction_material(material)
    if completed is not None:
        _scrub_completed_process(completed)
    if command_error is not None:
        raise command_error
    if decoded is None:
        raise OperationStateError("normal observation command returned no result")
    return decoded


def _task8_capture_observation_remote_evidence(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    runtime_lock_fd: int,
    newest_pair: dict[str, object],
    journal: dict[str, object],
    revalidate: Callable[[], None] | None = None,
) -> dict[str, object]:
    if revalidate is not None:
        revalidate()
    before = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-inventory-before",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    if not isinstance(before, tuple):
        raise OperationStateError("observation remote inventory is invalid")
    before_names, before_casefold = before
    dump_name = str(newest_pair["dump_name"])
    sidecar_name = str(newest_pair["sidecar_name"])
    if dump_name not in before_names or sidecar_name not in before_names:
        raise OperationStateError(
            "observation current local pair is missing from remote inventory"
        )
    try:
        scheduled_now = datetime.strptime(
            str(newest_pair["stamp"]),
            "%Y%m%dT%H%M%SZ",
        ).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise OperationStateError(
            "observation scheduled planner timestamp is invalid"
        ) from exc
    plan = _task8_run_remote_retention_planner(
        context,
        state,
        before_names,
        runtime_lock_fd=runtime_lock_fd,
        expected_now_override=scheduled_now,
    )
    if plan["delete_names"]:
        raise OperationStateError(
            "observation remote retention still has deletion candidates"
        )
    before_stat = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-current-stat-before",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    before_hash = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-current-hash-before",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    before_sidecar = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-current-sidecar-before",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    after = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-inventory-after",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    if (
        not isinstance(after, tuple)
        or after[0] != before_names
        or after[1] != before_casefold
    ):
        raise OperationStateError(
            "observation remote inventory changed during verification"
        )
    after_hash = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-current-hash-after",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    after_sidecar = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-current-sidecar-after",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    after_stat = _task8_run_normal_observation_audited_command(
        context,
        binding,
        state,
        purpose="observe-current-stat-after",
        runtime_lock_fd=runtime_lock_fd,
        newest_pair=newest_pair,
    )
    if revalidate is not None:
        revalidate()
    if (
        after_stat != before_stat
        or after_hash != before_hash
        or after_sidecar != before_sidecar
    ):
        raise OperationStateError(
            "observation remote object identity or content changed during verification"
        )
    entries = _task8_attempt_entries(state, "observe")
    if not entries:
        raise OperationStateError("observation remote evidence has no entry")
    groups = _task8_require_completed_observation_groups(state, entries[-1])
    final_receipt = journal.get("final_receipt")
    if (
        not isinstance(final_receipt, dict)
        or groups[0]["before"]
        != _task8_observation_receipt_file_audit(final_receipt)
    ):
        raise OperationStateError(
            "scheduled and observation rclone configuration receipts do not connect"
        )
    summary = {
        "remote_path": state["effective_config"]["RCLONE_REMOTE_PATH"],
        "inventory_names": before_names,
        "casefold_names": before_casefold,
        "keep_names": plan["keep_names"],
        "protected_names": plan["protected_names"],
        "delete_names": plan["delete_names"],
        "current_pair": {
            "dump_name": dump_name,
            "sidecar_name": sidecar_name,
            "dump_size": newest_pair["dump_size"],
            "dump_sha256": newest_pair["dump_sha256"],
            "sidecar_sha256": newest_pair["sidecar_sha256"],
            "stat_sha256": _task8_evidence_sha256(
                "observation-remote-stat",
                before_stat,
            ),
        },
        "rclone_groups": [
            {
                "group_id": group["group_id"],
                "purpose": group["purpose"],
                "result_sha256": group["result_sha256"],
            }
            for group in groups
        ],
    }
    return {
        "remote_sha256": _task8_evidence_sha256(
            "observation-remote",
            summary,
        ),
        "summary": summary,
    }


def _task8_derive_probe_material(
    operation_id: str,
    prefix: str,
) -> list[dict[str, object]]:
    validated_prefix = _validate_probe_prefix(
        prefix,
        "probe prefix",
        operation_id=operation_id,
    )
    match = _PROBE_PREFIX_RE.fullmatch(validated_prefix)
    assert match is not None
    token = match.group("token")
    dump = (
        "degen-db-remote-probe-v1\n"
        f"operation_id={operation_id}\n"
        f"token={token}\n"
    ).encode("ascii")
    dump_sha256 = hashlib.sha256(dump).hexdigest()
    sidecar = f"{dump_sha256}  probe.dump\n".encode("ascii")
    return [
        {
            "name": "probe.dump",
            "contents": dump,
            "expected_sha256": dump_sha256,
            "expected_size": len(dump),
        },
        {
            "name": "probe.dump.sha256",
            "contents": sidecar,
            "expected_sha256": hashlib.sha256(sidecar).hexdigest(),
            "expected_size": len(sidecar),
        },
    ]


def _task8_probe_source_filename(name: str, expected_sha256: str) -> str:
    if name not in _PROBE_OBJECT_NAMES:
        raise OperationStateError("remote probe source name is not tracked")
    _require_hash(expected_sha256, "remote probe source expected SHA-256")
    return _PROBE_LOCAL_SOURCE_PREFIX + name + "-" + expected_sha256


def _task8_probe_source_path(state: dict[str, object], name: str) -> Path:
    operation_dir = Path(str(state["operation_dir"]))
    objects = _task8_probe_objects(state)
    matches = [item for item in objects if item.get("name") == name]
    if len(matches) != 1:
        raise OperationStateError("remote probe source object is not unique")
    expected_sha256 = str(matches[0]["expected_sha256"])
    return operation_dir / _task8_probe_source_filename(name, expected_sha256)


def _task8_read_probe_source(
    descriptor: int,
    expected: bytes,
    *,
    effective_uid: int,
) -> os.stat_result:
    if type(descriptor) is not int or descriptor < 0 or type(expected) is not bytes:
        raise OperationStateError("remote probe source descriptor is invalid")
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != len(expected):
            raise OperationStateError("remote probe source metadata is invalid")
        if os.name == "posix":
            if stat.S_IMODE(metadata.st_mode) != 0o600:
                raise OperationStateError("remote probe source mode is unsafe")
            if metadata.st_uid != effective_uid:
                raise OperationStateError("remote probe source owner is unsafe")
            if metadata.st_nlink != 1:
                raise OperationStateError("remote probe source link count is unsafe")
        position = os.lseek(descriptor, 0, os.SEEK_CUR)
        os.lseek(descriptor, 0, os.SEEK_SET)
        observed = _read_exact_descriptor(
            descriptor,
            len(expected),
            "remote probe source",
        )
        if os.read(descriptor, 1):
            raise OperationStateError("remote probe source has trailing bytes")
        os.lseek(descriptor, position, os.SEEK_SET)
    except OperationStateError:
        raise
    except OSError as exc:
        raise OperationStateError("remote probe source validation failed") from exc
    if observed != expected:
        raise OperationStateError("remote probe source bytes differ from state")
    return metadata


@contextlib.contextmanager
def _task8_open_probe_source(
    context: OperationsContext,
    state: dict[str, object],
    name: str,
    contents: bytes,
):
    if type(contents) is not bytes or not contents:
        raise OperationStateError("remote probe source bytes are invalid")
    source_path = _task8_probe_source_path(state, name)
    if source_path.parent != context.paths.operation_dir:
        raise OperationStateError("remote probe source escaped the operation directory")
    descriptor: int | None = None
    directory_fd: int | None = None
    source_metadata: os.stat_result | None = None
    active_error: BaseException | None = None
    try:
        with _open_validated_operation_dir(
            context.paths.operation_dir,
            context.effective_uid,
        ) as opened_directory_fd:
            directory_fd = opened_directory_fd
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0)
            if os.name == "posix":
                flags |= os.O_CLOEXEC | os.O_NOFOLLOW
            source_name = source_path.name
            try:
                descriptor = (
                    os.open(source_name, flags, 0o600, dir_fd=directory_fd)
                    if directory_fd is not None
                    else os.open(source_path, flags, 0o600)
                )
            except FileExistsError as exc:
                raise OperationStateError(
                    "remote probe source already exists; recover the operation"
                ) from exc
            if os.name == "posix":
                os.fchmod(descriptor, 0o600)
            view = memoryview(contents)
            written = 0
            while written < len(view):
                count = os.write(descriptor, view[written:])
                if count <= 0:
                    raise OperationStateError(
                        "remote probe source write made no progress"
                    )
                written += count
            os.fsync(descriptor)
            source_metadata = _task8_read_probe_source(
                descriptor,
                contents,
                effective_uid=context.effective_uid,
            )
            named = (
                os.stat(source_name, dir_fd=directory_fd, follow_symlinks=False)
                if directory_fd is not None
                else source_path.lstat()
            )
            if not _same_identity(source_metadata, named):
                raise OperationStateError(
                    "remote probe source path/descriptor binding changed"
                )
            if directory_fd is not None:
                os.fsync(directory_fd)
            elif os.name == "posix":
                _fsync_parent_directory(source_path)
            os.lseek(descriptor, 0, os.SEEK_SET)
            yield source_path
            _task8_read_probe_source(
                descriptor,
                contents,
                effective_uid=context.effective_uid,
            )
    except OperationStateError as exc:
        active_error = exc
        raise
    except OSError as exc:
        wrapped = OperationStateError("remote probe source preparation failed")
        active_error = wrapped
        raise wrapped from exc
    except BaseException as exc:
        active_error = exc
        raise
    finally:
        cleanup_error: Exception | None = None
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if source_metadata is not None:
            try:
                with _open_validated_operation_dir(
                    context.paths.operation_dir,
                    context.effective_uid,
                ) as cleanup_directory_fd:
                    try:
                        source_name = source_path.name
                        named = (
                            os.stat(
                                source_name,
                                dir_fd=cleanup_directory_fd,
                                follow_symlinks=False,
                            )
                            if cleanup_directory_fd is not None
                            else source_path.lstat()
                        )
                        if not _same_identity(source_metadata, named):
                            raise OperationStateError(
                                "remote probe source binding changed before cleanup"
                            )
                        if cleanup_directory_fd is not None:
                            os.unlink(source_name, dir_fd=cleanup_directory_fd)
                            os.fsync(cleanup_directory_fd)
                        else:
                            source_path.unlink()
                            if os.name == "posix":
                                _fsync_parent_directory(source_path)
                    except OperationStateError:
                        raise
                    except OSError as exc:
                        raise OperationStateError(
                            "remote probe source cleanup failed: "
                            + sanitize_error_text(exc)
                        ) from exc
            except Exception as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            if isinstance(active_error, Exception):
                raise _Task8ProbeSourceCleanupError(
                    active_error,
                    cleanup_error,
                ) from cleanup_error
            if active_error is None:
                raise cleanup_error


def _task8_cleanup_interrupted_probe_sources(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
) -> None:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        raise OperationStateError(
            "interrupted probe source cleanup requires an active probe"
        )
    probe = transaction.get("probe")
    assert isinstance(probe, dict)
    prefix = _validate_probe_prefix(
        probe["prefix"],
        "active_transaction.probe.prefix",
        operation_id=context.operation_id,
    )
    material = {
        str(item["name"]): item
        for item in _task8_derive_probe_material(context.operation_id, prefix)
    }
    with _open_validated_operation_dir(
        context.paths.operation_dir,
        context.effective_uid,
    ) as directory_fd:
        for item in _task8_probe_objects(state):
            name = str(item["name"])
            expected = material[name]["contents"]
            assert isinstance(expected, bytes)
            source_path = _task8_probe_source_path(state, name)
            source_name = source_path.name
            try:
                named = (
                    os.stat(
                        source_name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                    if directory_fd is not None
                    else source_path.lstat()
                )
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(named.st_mode) or named.st_nlink != 1:
                raise OperationStateError(
                    "interrupted probe source path is not a single regular file"
                )
            if os.name == "posix" and (
                named.st_uid != context.effective_uid
                or stat.S_IMODE(named.st_mode) & 0o077
            ):
                raise OperationStateError(
                    "interrupted probe source ownership or mode is unsafe"
                )
            if named.st_size < 0 or named.st_size > len(expected):
                raise OperationStateError(
                    "interrupted probe source size is outside deterministic bounds"
                )
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
            if os.name == "posix":
                flags |= os.O_CLOEXEC | os.O_NOFOLLOW
            descriptor = (
                os.open(source_name, flags, dir_fd=directory_fd)
                if directory_fd is not None
                else os.open(source_path, flags)
            )
            try:
                opened = os.fstat(descriptor)
                if not _same_identity(named, opened):
                    raise OperationStateError(
                        "interrupted probe source binding changed while opening"
                    )
                observed = _read_exact_descriptor(
                    descriptor,
                    named.st_size,
                    "interrupted probe source",
                )
                if observed != expected[: named.st_size] or os.read(descriptor, 1):
                    raise OperationStateError(
                        "interrupted probe source bytes are not deterministic"
                    )
            finally:
                os.close(descriptor)
            _task8_force_checkpoint(
                context,
                binding,
                state,
                f"probe_source_cleanup:{name}",
            )
            latest = (
                os.stat(
                    source_name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                if directory_fd is not None
                else source_path.lstat()
            )
            if not _same_identity(named, latest):
                raise OperationStateError(
                    "interrupted probe source changed before cleanup"
                )
            if directory_fd is not None:
                os.unlink(source_name, dir_fd=directory_fd)
                os.fsync(directory_fd)
            else:
                source_path.unlink()
                if os.name == "posix":
                    _fsync_parent_directory(source_path)


def _task8_probe_identity_objects(
    operation_id: str,
    prefix: str,
) -> list[dict[str, object]]:
    return [
        {
            "name": item["name"],
            "expected_sha256": item["expected_sha256"],
            "expected_size": item["expected_size"],
        }
        for item in _task8_derive_probe_material(operation_id, prefix)
    ]


def _task8_probe_entry_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
    prefix: str,
) -> str:
    operation_id = str(state["operation_id"])
    return _task8_evidence_sha256(
        "probe-entry",
        {
            "operation_id": operation_id,
            "operation_dir": state["operation_dir"],
            "epoch": entry["epoch"],
            "prior_stable_phase": "installed",
            "prefix": prefix,
            "objects": _task8_probe_identity_objects(operation_id, prefix),
        },
    )


def _task8_probe_completion_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
    completed: dict[str, object],
    probe: dict[str, object],
    groups: list[dict[str, object]],
) -> str:
    return _task8_evidence_sha256(
        "probe-complete",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "completed_epoch": completed["epoch"],
            "prefix": probe["prefix"],
            "owned_names": probe["owned_names"],
            "cleanup_proven": probe["cleanup_proven"],
            "rclone_groups": groups,
        },
    )


def _validate_probe_receipt(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"prefix", "owned_names", "cleanup_proven", "evidence_sha256"}),
        "probe",
    )
    _validate_probe_prefix(item["prefix"], "probe.prefix")
    owned_names = _require_string_list(item["owned_names"], "probe.owned_names")
    if tuple(owned_names) != _PROBE_OBJECT_NAMES:
        raise OperationStateError(
            "probe.owned_names must contain the exact ordered probe object names"
        )
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


def _task8_dry_run_completion_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
    completed: dict[str, object],
    receipt: dict[str, object],
) -> str:
    groups = _task8_dry_run_attempt_groups(state, entry)
    return _task8_evidence_sha256(
        "dry-run-complete",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "completed_epoch": completed["epoch"],
            "inventory_names": receipt["inventory_names"],
            "casefold_names": receipt["casefold_names"],
            "keep_names": receipt["keep_names"],
            "protected_names": receipt["protected_names"],
            "delete_names": receipt["delete_names"],
            "candidate_sha256": receipt["candidate_sha256"],
            "rclone_groups": groups,
        },
    )


def _task8_dry_run_entry_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
) -> str:
    effective_config = state.get("effective_config")
    install = state.get("install")
    if not isinstance(effective_config, dict) or not isinstance(install, dict):
        raise OperationStateError("dry_run entry provenance is incomplete")
    installed_hashes = install.get("installed_hashes")
    if not isinstance(installed_hashes, dict):
        raise OperationStateError("dry_run installed hash provenance is incomplete")
    return _task8_evidence_sha256(
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


def _task8_dry_run_result_sha256(
    purpose: str,
    receipt: dict[str, object],
) -> str:
    if purpose not in _TASK8_DRY_RUN_PURPOSES:
        raise OperationStateError("dry_run result purpose is invalid")
    if purpose in {
        "dry-run-inventory-before",
        "dry-run-inventory-after",
    }:
        result_domain = {
            "inventory_names": receipt["inventory_names"],
            "casefold_names": receipt["casefold_names"],
        }
    else:
        result_domain = {
            "delete_names": receipt["delete_names"],
            "candidate_sha256": receipt["candidate_sha256"],
        }
    return _task8_evidence_sha256(
        "dry-run-result",
        {
            "purpose": purpose,
            "result_domain": result_domain,
        },
    )


def _task8_policy_inventory_result_sha256(
    dry_run: dict[str, object],
) -> str:
    return _task8_evidence_sha256(
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


def _task8_policy_runtime_baseline_sha256(
    runtime_baseline: dict[str, object],
) -> str:
    _validate_prior_runtime(
        runtime_baseline,
        "policy runtime baseline",
    )
    return _task8_evidence_sha256(
        "policy-runtime-baseline",
        runtime_baseline,
    )


def _task8_policy_entry_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
) -> str:
    effective_config = state.get("effective_config")
    install = state.get("install")
    dry_run = state.get("dry_run")
    host_stage = state.get("host_stage")
    if not all(
        isinstance(value, dict)
        for value in (effective_config, install, dry_run, host_stage)
    ):
        raise OperationStateError("policy entry provenance is incomplete")
    assert isinstance(install, dict)
    installed_hashes = install.get("installed_hashes")
    if not isinstance(installed_hashes, dict):
        raise OperationStateError("policy installed hash provenance is incomplete")
    transaction = state.get("active_transaction")
    policy = state.get("policy")
    if (
        isinstance(transaction, dict)
        and transaction.get("kind") == "policy"
        and transaction.get("started_epoch") == entry.get("epoch")
    ):
        runtime_baseline = transaction.get("runtime_baseline")
        if not isinstance(runtime_baseline, dict):
            raise OperationStateError("policy transaction runtime baseline is missing")
        runtime_baseline_sha256 = _task8_policy_runtime_baseline_sha256(
            runtime_baseline
        )
    elif isinstance(policy, dict):
        runtime_baseline_sha256 = policy.get("runtime_baseline_sha256")
        _require_hash(
            runtime_baseline_sha256,
            "policy.runtime_baseline_sha256",
        )
    else:
        raise OperationStateError("policy entry runtime provenance is unavailable")
    assert isinstance(dry_run, dict) and isinstance(host_stage, dict)
    return _task8_evidence_sha256(
        "policy-entry",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "epoch": entry["epoch"],
            "prior_stable_phase": "dry_run_recorded",
            "effective_config": effective_config,
            "installed_hashes": installed_hashes,
            "dry_run_evidence_sha256": dry_run["evidence_sha256"],
            "enabled_environment_sha256": host_stage[
                "enabled_environment_sha256"
            ],
            "runtime_baseline_sha256": runtime_baseline_sha256,
        },
    )


def _task8_policy_applied_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
    policy: dict[str, object],
    group: dict[str, object],
) -> str:
    dry_run = state.get("dry_run")
    if not isinstance(dry_run, dict):
        raise OperationStateError("policy applied evidence lacks dry_run receipt")
    return _task8_evidence_sha256(
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
            "dry_run_evidence_sha256": dry_run["evidence_sha256"],
            "inventory_group": group,
        },
    )


def _task8_policy_attempt_groups(
    state: dict[str, object],
    entry: dict[str, object],
) -> list[dict[str, object]]:
    entries = _task8_attempt_entries(state, "policy")
    matching_attempts = [
        index for index, candidate in enumerate(entries) if candidate is entry
    ]
    if len(matching_attempts) != 1:
        raise OperationStateError("policy entry occurrence is ambiguous")
    attempt_ordinal = matching_attempts[0]
    selected: list[tuple[int, dict[str, object]]] = []
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    for group in groups:
        assert isinstance(group, dict)
        if group["group_id"] == "install":
            continue
        kind, started_epoch, group_attempt, group_ordinal = (
            _task8_group_identity(group)
        )
        if (
            kind == "policy"
            and started_epoch == entry["epoch"]
            and group_attempt == attempt_ordinal
        ):
            selected.append((group_ordinal, group))
    selected.sort(key=lambda item: item[0])
    return [group for _ordinal, group in selected]


def _task8_require_completed_policy_inventory_group(
    state: dict[str, object],
    entry: dict[str, object],
) -> dict[str, object]:
    dry_run = state.get("dry_run")
    if not isinstance(dry_run, dict):
        raise OperationStateError("policy inventory commitment lacks dry_run receipt")
    groups = _task8_policy_attempt_groups(state, entry)
    if len(groups) != 1:
        raise OperationStateError(
            "policy inventory commitment requires exactly one audited group"
        )
    group = groups[0]
    _kind, _started, _attempt, group_ordinal = _task8_group_identity(group)
    if (
        group_ordinal != 0
        or group["purpose"] != _TASK8_POLICY_PURPOSES[0]
        or group["after"] is None
        or group.get("outcome") != "success"
        or group.get("result_sha256")
        != _task8_policy_inventory_result_sha256(dry_run)
        or group["evidence_sha256"] != _task8_group_evidence_sha256(group)
    ):
        raise OperationStateError(
            "policy inventory audit commitment is incomplete or invalid"
        )
    return group


def _task8_policy_completion_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
    completed: dict[str, object],
    policy: dict[str, object],
    group: dict[str, object],
) -> str:
    dry_run = state.get("dry_run")
    if not isinstance(dry_run, dict):
        raise OperationStateError("policy completion lacks dry_run receipt")
    policy_recovery = None
    history = state["phase_history"]
    assert isinstance(history, list)
    entry_indices = [
        index for index, candidate in enumerate(history) if candidate is entry
    ]
    completed_indices = [
        index for index, candidate in enumerate(history) if candidate is completed
    ]
    if len(entry_indices) != 1 or len(completed_indices) != 1:
        raise OperationStateError("policy completion history identity is ambiguous")
    entry_index = entry_indices[0]
    completed_index = completed_indices[0]
    recovery_entries = [
        candidate
        for index, candidate in enumerate(history)
        if (
            entry_index < index < completed_index
            and candidate["phase"] == "recovering_policy"
            and index > 0
            and history[index - 1]["phase"] == "policy_enabling"
        )
    ]
    if len(recovery_entries) > 1:
        raise OperationStateError("policy completion has ambiguous recovery history")
    if recovery_entries:
        recovery_entry = recovery_entries[0]
        policy_recovery = {
            "kind": "policy",
            "started_epoch": recovery_entry["epoch"],
            "completed_epoch": completed["epoch"],
            "evidence_sha256": recovery_entry["evidence_sha256"],
            "next_target_index": 1,
        }
    return _task8_evidence_sha256(
        "policy-complete",
        {
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "completed_epoch": completed["epoch"],
            "policy": policy,
            "dry_run_evidence_sha256": dry_run["evidence_sha256"],
            "inventory_group": group,
            "policy_recovery": policy_recovery,
        },
    )


def _task8_observation_runtime_baseline(
    state: dict[str, object],
) -> dict[str, object]:
    prior_runtime = state.get("prior_runtime")
    if not isinstance(prior_runtime, dict):
        raise OperationStateError("observation prior runtime is missing")
    transaction = state.get("active_transaction")
    observation = state.get("observation")
    if isinstance(transaction, dict) and transaction.get("kind") == "observe":
        runtime_baseline = transaction.get("runtime_baseline")
        if not isinstance(runtime_baseline, dict):
            raise OperationStateError("observation runtime baseline is missing")
        run_epoch = runtime_baseline.get("preinstall_trigger_epoch")
    elif isinstance(observation, dict):
        run_epoch = observation.get("run_epoch")
        runtime_baseline = {
            **copy.deepcopy(prior_runtime),
            "preinstall_trigger_epoch": run_epoch,
        }
    else:
        raise OperationStateError("observation run identity is missing")
    if type(run_epoch) is not int:
        raise OperationStateError("observation timer trigger is missing")
    expected = {
        **copy.deepcopy(prior_runtime),
        "preinstall_trigger_epoch": run_epoch,
    }
    if runtime_baseline != expected:
        raise OperationStateError(
            "observation runtime baseline differs from protected PID/timer provenance"
        )
    return expected


def _task8_observation_entry_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
) -> str:
    effective = state.get("effective_config")
    install = state.get("install")
    policy = state.get("policy")
    if (
        not isinstance(effective, dict)
        or not isinstance(install, dict)
        or not isinstance(policy, dict)
        or entry.get("phase") != "observing"
    ):
        raise OperationStateError("observation entry provenance is incomplete")
    installed_hashes = install.get("installed_hashes")
    if not isinstance(installed_hashes, dict):
        raise OperationStateError("observation installed hashes are missing")
    return _task8_evidence_sha256(
        "observation-entry",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "epoch": entry["epoch"],
            "prior_stable_phase": "policy_enabled",
            "effective_config": effective,
            "installed_hashes": installed_hashes,
            "policy": policy,
            "runtime_baseline": _task8_observation_runtime_baseline(state),
        },
    )


def _task8_observation_attempt_groups(
    state: dict[str, object],
    entry: dict[str, object],
) -> list[dict[str, object]]:
    entries = _task8_attempt_entries(state, "observe")
    matching_attempts = [
        index for index, candidate in enumerate(entries) if candidate is entry
    ]
    if len(matching_attempts) != 1:
        raise OperationStateError("observation entry is not in phase history")
    attempt_ordinal = matching_attempts[0]
    selected: list[tuple[int, dict[str, object]]] = []
    groups = state.get("rclone_evidence_groups")
    if not isinstance(groups, list):
        raise OperationStateError("observation rclone evidence stream is invalid")
    for group in groups:
        assert isinstance(group, dict)
        if group["group_id"] == "install":
            continue
        kind, started_epoch, group_attempt, group_ordinal = (
            _task8_group_identity(group)
        )
        if (
            kind == "observe"
            and started_epoch == entry["epoch"]
            and group_attempt == attempt_ordinal
        ):
            selected.append((group_ordinal, group))
    selected.sort(key=lambda item: item[0])
    return [group for _ordinal, group in selected]


def _task8_require_completed_observation_groups(
    state: dict[str, object],
    entry: dict[str, object],
) -> list[dict[str, object]]:
    groups = _task8_observation_attempt_groups(state, entry)
    if len(groups) != len(_TASK8_OBSERVE_PURPOSES):
        raise OperationStateError("observation rclone sequence is incomplete")
    for ordinal, (group, purpose) in enumerate(
        zip(groups, _TASK8_OBSERVE_PURPOSES, strict=True)
    ):
        _kind, _started, _attempt, group_ordinal = _task8_group_identity(group)
        if (
            group_ordinal != ordinal
            or group["purpose"] != purpose
            or group["after"] is None
            or group.get("outcome") != "success"
            or group.get("result_sha256") is None
            or group["evidence_sha256"] != _task8_group_evidence_sha256(group)
        ):
            raise OperationStateError(
                "observation rclone sequence is incomplete or invalid"
            )
    for before, after in zip(groups, groups[1:]):
        if before["after"] != after["before"]:
            raise OperationStateError(
                "observation rclone configuration audit chain is discontinuous"
            )
    return groups


def _task8_observation_completion_evidence_sha256(
    state: dict[str, object],
    entry: dict[str, object],
    completed: dict[str, object],
    observation: dict[str, object],
    groups: list[dict[str, object]],
) -> str:
    install = state.get("install")
    policy = state.get("policy")
    prior_runtime = state.get("prior_runtime")
    if (
        not isinstance(install, dict)
        or not isinstance(policy, dict)
        or not isinstance(prior_runtime, dict)
        or completed.get("phase") != "observed"
    ):
        raise OperationStateError("observation completion provenance is incomplete")
    return _task8_evidence_sha256(
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
            "install_completed_epoch": install["completed_epoch"],
            "policy_enabled_epoch": policy["enabled_epoch"],
            "policy_applied_evidence_sha256": policy[
                "applied_evidence_sha256"
            ],
            "preinstall_trigger_epoch": prior_runtime[
                "preinstall_trigger_epoch"
            ],
            "rclone_groups": groups,
        },
    )


def _validate_observation_receipt_semantics(
    state: dict[str, object],
) -> None:
    observation = state.get("observation")
    entries = _task8_attempt_entries(state, "observe")
    transaction = state.get("active_transaction")
    if isinstance(transaction, dict) and transaction.get("kind") == "observe":
        if not entries:
            raise OperationStateError("observation transaction has no history entry")
        entry = entries[-1]
        if (
            entry["epoch"] != transaction["started_epoch"]
            or entry["evidence_sha256"]
            != _task8_observation_entry_evidence_sha256(state, entry)
        ):
            raise OperationStateError("observation entry evidence digest is invalid")
    if observation is None:
        return
    history = state["phase_history"]
    assert isinstance(history, list)
    completion_indices = [
        index
        for index, item in enumerate(history)
        if (
            index > 0
            and item["phase"] == "observed"
            and history[index - 1]["phase"] == "observing"
        )
    ]
    if not completion_indices:
        raise OperationStateError("observation receipt has no completion history")
    completed_index = completion_indices[-1]
    entry = history[completed_index - 1]
    completed = history[completed_index]
    assert isinstance(entry, dict) and isinstance(completed, dict)
    if entry["evidence_sha256"] != _task8_observation_entry_evidence_sha256(
        state,
        entry,
    ):
        raise OperationStateError("observation entry evidence digest is invalid")
    groups = _task8_require_completed_observation_groups(state, entry)
    expected = _task8_observation_completion_evidence_sha256(
        state,
        entry,
        completed,
        observation,
        groups,
    )
    if (
        observation["evidence_sha256"] != expected
        or completed["evidence_sha256"] != expected
    ):
        raise OperationStateError("observation completion evidence digest is invalid")


def _task8_dry_run_attempt_groups(
    state: dict[str, object],
    entry: dict[str, object],
) -> list[dict[str, object]]:
    entries = _task8_attempt_entries(state, "dry_run")
    matching_attempts = [
        index for index, candidate in enumerate(entries) if candidate is entry
    ]
    if len(matching_attempts) != 1:
        raise OperationStateError("dry_run entry occurrence is ambiguous")
    attempt_ordinal = matching_attempts[0]
    selected: list[tuple[int, dict[str, object]]] = []
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    for group in groups:
        assert isinstance(group, dict)
        if group["group_id"] == "install":
            continue
        kind, started_epoch, group_attempt, group_ordinal = (
            _task8_group_identity(group)
        )
        if (
            kind == "dry_run"
            and started_epoch == entry["epoch"]
            and group_attempt == attempt_ordinal
        ):
            selected.append((group_ordinal, group))
    selected.sort(key=lambda item: item[0])
    return [group for _ordinal, group in selected]


def _task8_require_completed_dry_run_groups(
    state: dict[str, object],
    entry: dict[str, object],
) -> list[dict[str, object]]:
    receipt = state.get("dry_run")
    if not isinstance(receipt, dict):
        raise OperationStateError("dry_run completion receipt is missing")
    groups = _task8_dry_run_attempt_groups(state, entry)
    if len(groups) != len(_TASK8_DRY_RUN_PURPOSES):
        raise OperationStateError(
            "dry_run rclone group completion requires the exact audited command sequence"
        )
    for ordinal, (group, expected_purpose) in enumerate(
        zip(groups, _TASK8_DRY_RUN_PURPOSES, strict=True)
    ):
        _kind, _started, _attempt, group_ordinal = _task8_group_identity(group)
        if (
            group_ordinal != ordinal
            or group["purpose"] != expected_purpose
            or group["after"] is None
            or group.get("outcome") != "success"
            or group.get("result_sha256")
            != _task8_dry_run_result_sha256(
                expected_purpose,
                receipt,
            )
            or group["evidence_sha256"] != _task8_group_evidence_sha256(group)
        ):
            raise OperationStateError(
                "dry_run completion audit sequence is incomplete or invalid"
            )
    return groups


def _task8_require_complete_backup_pairs(
    names: list[str],
    *,
    prefix: str,
    label: str,
) -> None:
    members = set(names)
    for index, name in enumerate(names):
        match = _BACKUP_NAME_RE.fullmatch(name)
        if match is None or match.group("prefix") != prefix:
            raise OperationStateError(f"{label}[{index}] is not an owned backup name")
        counterpart = name[:-7] if name.endswith(".sha256") else f"{name}.sha256"
        if counterpart not in members:
            raise OperationStateError(f"{label} splits a dump/checksum pair")


def _validate_dry_run_receipt_semantics(state: dict[str, object]) -> None:
    receipt = state["dry_run"]
    if receipt is None:
        return
    assert isinstance(receipt, dict)
    inventory_names = _task8_validate_canonical_remote_names(
        receipt["inventory_names"],
        "dry_run.inventory_names",
        require_sorted=True,
    )
    casefold_names = _require_string_list(
        receipt["casefold_names"],
        "dry_run.casefold_names",
    )
    if casefold_names != [name.casefold() for name in inventory_names]:
        raise OperationStateError(
            "dry_run.casefold_names must parallel the exact inventory"
        )
    partition_names: list[list[str]] = []
    for field in ("keep_names", "protected_names", "delete_names"):
        partition_names.append(
            _task8_validate_canonical_remote_names(
                receipt[field],
                f"dry_run.{field}",
                require_sorted=False,
            )
        )
    keep_names, protected_names, delete_names = partition_names
    partition_sets = [set(items) for items in partition_names]
    if any(
        partition_sets[left] & partition_sets[right]
        for left in range(len(partition_sets))
        for right in range(left + 1, len(partition_sets))
    ):
        raise OperationStateError("dry_run keep/protected/delete partitions overlap")
    if set().union(*partition_sets) != set(inventory_names):
        raise OperationStateError(
            "dry_run keep/protected/delete partitions do not cover inventory"
        )
    effective = state["effective_config"]
    assert isinstance(effective, dict)
    prefix = str(effective["BACKUP_PREFIX"])
    _task8_require_complete_backup_pairs(
        keep_names,
        prefix=prefix,
        label="dry_run.keep_names",
    )
    _task8_require_complete_backup_pairs(
        delete_names,
        prefix=prefix,
        label="dry_run.delete_names",
    )
    expected_candidate = _task8_remote_dry_run_candidate_sha256(delete_names)
    if receipt["candidate_sha256"] != expected_candidate:
        raise OperationStateError("dry_run candidate digest is invalid")

    history = state["phase_history"]
    assert isinstance(history, list)
    completion_indices = [
        index
        for index, item in enumerate(history)
        if (
            item["phase"] == "dry_run_recorded"
            and index > 0
            and history[index - 1]["phase"] == "dry_run_recording"
        )
    ]
    if not completion_indices:
        raise OperationStateError("dry_run receipt has no completion history entry")
    completed_index = completion_indices[-1]
    entry = history[completed_index - 1]
    completed = history[completed_index]
    assert isinstance(entry, dict) and isinstance(completed, dict)
    if entry["evidence_sha256"] != _task8_dry_run_entry_evidence_sha256(
        state,
        entry,
    ):
        raise OperationStateError("dry_run entry evidence digest is invalid")
    _task8_require_completed_dry_run_groups(state, entry)

    def retention_count(key: str) -> int:
        raw = effective.get(key)
        if (
            type(raw) is not str
            or len(raw) > 20
            or re.fullmatch(r"[0-9]+", raw, re.ASCII) is None
        ):
            raise OperationStateError(f"dry_run {key} is invalid")
        try:
            return int(raw)
        except ValueError as exc:
            raise OperationStateError(f"dry_run {key} is invalid") from exc

    try:
        plan_now = datetime.fromtimestamp(
            int(entry["epoch"]),
            tz=timezone.utc,
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise OperationStateError("dry_run entry epoch is invalid") from exc
    expected_plan = _task8_plan_remote_inventory(
        inventory_names,
        prefix=prefix,
        now=plan_now,
        daily=retention_count("KEEP_REMOTE_DAILY"),
        weekly=retention_count("KEEP_REMOTE_WEEKLY"),
        monthly=retention_count("KEEP_REMOTE_MONTHLY"),
    )
    if expected_plan != {
        "keep_names": keep_names,
        "protected_names": protected_names,
        "delete_names": delete_names,
    }:
        raise OperationStateError(
            "dry_run receipt differs from the independently recomputed plan"
        )
    expected_evidence = _task8_dry_run_completion_evidence_sha256(
        state,
        entry,
        completed,
        receipt,
    )
    if (
        receipt["evidence_sha256"] != expected_evidence
        or completed["evidence_sha256"] != expected_evidence
    ):
        raise OperationStateError("dry_run completion evidence digest is invalid")


def _validate_policy(value: object) -> None:
    item = _require_object(
        value,
        frozenset(
            {
                "environment_sha256",
                "enabled_epoch",
                "runtime_baseline_sha256",
                "applied_target",
                "applied_evidence_sha256",
            }
        ),
        "policy",
    )
    _require_hash(item["environment_sha256"], "policy.environment_sha256")
    _require_int(item["enabled_epoch"], "policy.enabled_epoch")
    _require_hash(
        item["runtime_baseline_sha256"],
        "policy.runtime_baseline_sha256",
    )
    _validate_snapshot_target(item["applied_target"], "policy.applied_target")
    target = item["applied_target"]
    assert isinstance(target, dict)
    if (
        target["present"] is not True
        or target["sha256"] != item["environment_sha256"]
        or target["mode"] != 0o600
        or not isinstance(target["uid"], int)
        or target["uid"] < 0
        or not isinstance(target["gid"], int)
        or target["gid"] < 0
    ):
        raise OperationStateError(
            "policy applied target digest or metadata is not an exact enabled environment receipt"
        )
    _require_hash(
        item["applied_evidence_sha256"],
        "policy.applied_evidence_sha256",
    )


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
    _validate_probe_prefix(item["prefix"], "active_transaction.probe.prefix")
    objects = item["objects"]
    if type(objects) is not list:
        raise OperationStateError("active_transaction.probe.objects must be a list")
    if len(objects) != len(_PROBE_OBJECT_NAMES):
        raise OperationStateError(
            "active_transaction.probe.objects must contain the exact two probe objects"
        )
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
        name = _require_string(obj["name"], f"{label}.name", nonempty=True)
        if name != _PROBE_OBJECT_NAMES[index]:
            raise OperationStateError(
                "active_transaction probe object names are unsafe or case-colliding"
            )
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
                "runtime_baseline",
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
    prior_timer_enabled = _require_bool(
        item["prior_timer_enabled"],
        "active_transaction.prior_timer_enabled",
    )
    prior_timer_active = _require_bool(
        item["prior_timer_active"],
        "active_transaction.prior_timer_active",
    )
    _validate_prior_runtime(
        item["runtime_baseline"],
        "active_transaction.runtime_baseline",
    )
    runtime_baseline = item["runtime_baseline"]
    assert isinstance(runtime_baseline, dict)
    if (
        prior_timer_enabled is not runtime_baseline["timer_enabled"]
        or prior_timer_active is not runtime_baseline["timer_active"]
    ):
        raise OperationStateError(
            "active_transaction prior timer state must mirror runtime_baseline"
        )
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
                "runtime_directory_created",
                "runtime_baseline",
                "restored_epoch",
                "restore_evidence_sha256",
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
    _require_bool(
        item["runtime_directory_created"],
        "recovery.runtime_directory_created",
    )
    _validate_prior_runtime(item["runtime_baseline"])
    _require_optional_int(item["restored_epoch"], "recovery.restored_epoch")
    _require_optional_hash(
        item["restore_evidence_sha256"],
        "recovery.restore_evidence_sha256",
    )


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
    if frozenset(targets) != frozenset(_TARGET_ORDER):
        raise OperationStateError("snapshot.targets must contain the exact eight targets")
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
    validated_epoch = install["validated_epoch"]
    validation_evidence = install["validation_evidence_sha256"]
    if (validated_epoch is None) != (validation_evidence is None):
        raise OperationStateError(
            "install validated_epoch and validation_evidence_sha256 must be recorded together"
        )
    if validated_epoch is not None:
        assert isinstance(validated_epoch, int)
        if index != len(_TARGET_ORDER) or any(value is not None for value in cursor):
            raise OperationStateError(
                "install provisional validation requires a terminal cursor"
            )
        if validated_epoch < install["started_epoch"]:
            raise OperationStateError("install validation precedes install start")
    if not installed_reached:
        if hashes:
            raise OperationStateError("install.installed_hashes must be empty before installed")
        if install["completed_epoch"] is not None:
            raise OperationStateError("install.completed_epoch must be null before installed")
        return
    if index != len(_TARGET_ORDER) or any(value is not None for value in cursor):
        raise OperationStateError("installed requires a terminal cleared install cursor")
    if frozenset(hashes) != frozenset(_TARGET_ORDER):
        raise OperationStateError("install.installed_hashes must contain the exact eight targets")
    if install["completed_epoch"] is None:
        raise OperationStateError("install.completed_epoch is required after installed")
    if validated_epoch is None:
        raise OperationStateError("installed requires provisional validation evidence")
    if install["completed_epoch"] < validated_epoch:
        raise OperationStateError("install completion precedes provisional validation")
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
        if (
            transaction["policy_environment_sha256"]
            != host_stage["enabled_environment_sha256"]
        ):
            raise OperationStateError(
                "policy_environment_sha256 must equal the precomputed enabled environment digest"
            )
        entries = _task8_attempt_entries(state, "policy")
        if not entries:
            raise OperationStateError("policy transaction has no history entry")
        entry = entries[-1]
        if entry["epoch"] != transaction["started_epoch"]:
            raise OperationStateError(
                "policy transaction start epoch does not match its history entry"
            )
        if entry["evidence_sha256"] != _task8_policy_entry_evidence_sha256(
            state,
            entry,
        ):
            raise OperationStateError("policy entry evidence digest is invalid")
    probe_transaction = transaction["probe"]
    if isinstance(probe_transaction, dict):
        prefix = _validate_probe_prefix(
            probe_transaction["prefix"],
            "active_transaction.probe.prefix",
            operation_id=str(state["operation_id"]),
        )
        expected_objects = _task8_probe_identity_objects(
            str(state["operation_id"]),
            prefix,
        )
        objects = probe_transaction["objects"]
        assert isinstance(objects, list)
        observed_objects = [
            {
                "name": item["name"],
                "expected_sha256": item["expected_sha256"],
                "expected_size": item["expected_size"],
            }
            for item in objects
        ]
        if observed_objects != expected_objects:
            raise OperationStateError(
                "active_transaction probe objects do not match deterministic identity"
            )
        entries = _task8_attempt_entries(state, "probe")
        if not entries:
            raise OperationStateError("probe transaction has no history entry")
        entry = entries[-1]
        if entry["epoch"] != transaction["started_epoch"]:
            raise OperationStateError(
                "probe transaction start epoch does not match its history entry"
            )
        if entry["evidence_sha256"] != _task8_probe_entry_evidence_sha256(
            state,
            entry,
            prefix,
        ):
            raise OperationStateError("probe entry evidence digest is invalid")
    if expected[0] == "dry_run":
        entries = _task8_attempt_entries(state, "dry_run")
        if not entries:
            raise OperationStateError("dry_run transaction has no history entry")
        entry = entries[-1]
        if entry["epoch"] != transaction["started_epoch"]:
            raise OperationStateError(
                "dry_run transaction start epoch does not match its history entry"
            )
        if entry["evidence_sha256"] != _task8_dry_run_entry_evidence_sha256(
            state,
            entry,
        ):
            raise OperationStateError("dry_run entry evidence digest is invalid")


def _validate_policy_environment_semantics(state: dict[str, object]) -> None:
    policy = state["policy"]
    transaction = state.get("active_transaction")
    if isinstance(transaction, dict) and transaction.get("kind") == "policy":
        guard = transaction.get("guard")
        assert isinstance(guard, dict)
        held_prefix = (
            "timer_stopped",
            "service_inactive_verified",
            "legacy_lock_acquired",
            "runtime_lock_acquired",
        )
        suffix_started = bool(
            guard["locks_released"] or guard["timer_restored"]
        )
        if policy is not None and not all(guard[field] for field in held_prefix):
            raise OperationStateError(
                "applied policy guard requires stopped runtime and both held locks"
            )
        if policy is None and suffix_started:
            recovery = state.get("recovery")
            cursor = (
                recovery.get("current_target"),
                recovery.get("previous_sha256"),
                recovery.get("intended_sha256"),
            ) if isinstance(recovery, dict) else (None, None, None)
            terminal_rollback = (
                state["phase"] in {"recovering_policy", "recovery_required"}
                and isinstance(recovery, dict)
                and recovery.get("kind") == "policy"
                and recovery.get("next_target_index") == 1
                and all(value is None for value in cursor)
            )
            if not terminal_rollback:
                raise OperationStateError(
                    "uncommitted policy cannot release or restore its guard"
                )
    if policy is None:
        return
    host_stage = state["host_stage"]
    assert isinstance(policy, dict) and isinstance(host_stage, dict)
    if policy["environment_sha256"] != host_stage["enabled_environment_sha256"]:
        raise OperationStateError(
            "policy environment digest must equal the precomputed enabled environment digest"
        )
    history = state["phase_history"]
    assert isinstance(history, list)
    entry_indices = [
        index
        for index, item in enumerate(history)
        if isinstance(item, dict) and item["phase"] == "policy_enabling"
    ]
    if not entry_indices:
        raise OperationStateError("policy receipt has no policy_enabling entry")
    entry_index = entry_indices[-1]
    entry = history[entry_index]
    assert isinstance(entry, dict)
    group = _task8_require_completed_policy_inventory_group(state, entry)
    if entry["evidence_sha256"] != _task8_policy_entry_evidence_sha256(
        state,
        entry,
    ):
        raise OperationStateError("policy entry evidence digest is invalid")
    if policy["enabled_epoch"] < entry["epoch"]:
        raise OperationStateError(
            "policy enabled_epoch cannot precede its durable transaction entry"
        )
    if isinstance(transaction, dict) and transaction.get("kind") == "policy":
        expected_runtime = _task8_policy_runtime_baseline_sha256(
            transaction["runtime_baseline"]
        )
        if policy["runtime_baseline_sha256"] != expected_runtime:
            raise OperationStateError(
                "policy runtime baseline digest differs from its transaction"
            )
    applied_expected = _task8_policy_applied_evidence_sha256(
        state,
        entry,
        policy,
        group,
    )
    if policy["applied_evidence_sha256"] != applied_expected:
        raise OperationStateError("policy applied evidence digest is invalid")
    completed = next(
        (
            item
            for item in history[entry_index + 1 :]
            if isinstance(item, dict) and item["phase"] == "policy_enabled"
        ),
        None,
    )
    if completed is not None:
        if policy["enabled_epoch"] > completed["epoch"]:
            raise OperationStateError(
                "policy enabled_epoch cannot follow policy completion"
            )
        expected = _task8_policy_completion_evidence_sha256(
            state,
            entry,
            completed,
            policy,
            group,
        )
        if completed["evidence_sha256"] != expected:
            raise OperationStateError("policy completion evidence digest is invalid")


def _validate_probe_receipt_semantics(state: dict[str, object]) -> None:
    probe = state["probe"]
    if probe is None:
        return
    assert isinstance(probe, dict)
    prefix = _validate_probe_prefix(
        probe["prefix"],
        "probe.prefix",
        operation_id=str(state["operation_id"]),
    )
    if probe["cleanup_proven"] is not True:
        raise OperationStateError("probe cleanup must be proven before completion")
    history = state["phase_history"]
    assert isinstance(history, list)
    history_epochs = [entry["epoch"] for entry in history]
    if history_epochs != sorted(history_epochs):
        return
    probed_indices = [
        index
        for index, entry in enumerate(history)
        if (
            entry["phase"] == "probed"
            and index > 0
            and history[index - 1]["phase"] == "probing"
        )
    ]
    if not probed_indices:
        raise OperationStateError("probe receipt has no completion history entry")
    completed_index = probed_indices[-1]
    entry = history[completed_index - 1]
    completed = history[completed_index]
    assert isinstance(entry, dict) and isinstance(completed, dict)
    if entry["evidence_sha256"] != _task8_probe_entry_evidence_sha256(
        state,
        entry,
        prefix,
    ):
        raise OperationStateError("probe entry evidence digest is invalid")
    attempt_ordinal = sum(
        1
        for prior in history[: completed_index - 1]
        if prior["phase"] == "probing"
    )
    matching_groups: list[tuple[int, dict[str, object]]] = []
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    for group in groups:
        assert isinstance(group, dict)
        if group["group_id"] == "install":
            continue
        kind, started_epoch, group_attempt, group_ordinal = (
            _task8_group_identity(group)
        )
        if (
            kind == "probe"
            and started_epoch == entry["epoch"]
            and group_attempt == attempt_ordinal
        ):
            if group["after"] is None or group["evidence_sha256"] is None:
                raise OperationStateError(
                    "probe completion requires completed rclone groups"
                )
            matching_groups.append((group_ordinal, group))
    if not matching_groups:
        raise OperationStateError(
            "probe completion requires a nonempty rclone group sequence"
        )
    matching_groups.sort(key=lambda item: item[0])
    ordered_groups = [group for _ordinal, group in matching_groups]
    events = [
        (str(group["purpose"]), str(group["outcome"]))
        for group in ordered_groups
    ]
    if any(outcome != "success" for _purpose, outcome in events):
        raise OperationStateError(
            "completed probe receipt contains an indeterminate rclone outcome"
        )
    _task8_require_normal_probe_success_sequence(events)
    receipt_objects = [
        {
            **item,
            "created": True,
            "verified": True,
            "cleaned": True,
        }
        for item in _task8_probe_identity_objects(str(state["operation_id"]), prefix)
    ]
    _task8_require_probe_event_chain(receipt_objects, events)
    final_group = ordered_groups[-1]
    if (
        final_group["purpose"] != "probe-prefix-empty"
        or final_group["outcome"] != "success"
    ):
        raise OperationStateError(
            "completed probe receipt lacks terminal successful prefix-empty proof"
        )
    expected_completion = _task8_probe_completion_evidence_sha256(
        state,
        entry,
        completed,
        probe,
        ordered_groups,
    )
    if (
        probe["evidence_sha256"] != expected_completion
        or completed["evidence_sha256"] != expected_completion
    ):
        raise OperationStateError("probe completion evidence digest is invalid")


def _manual_rollback_origin_phase(history: list[object]) -> str:
    stable = {"installed", "probed", "dry_run_recorded", "policy_enabled", "observed"}
    for index, entry in enumerate(history):
        assert isinstance(entry, dict)
        if entry["phase"] != "manual_rollback" or index == 0:
            continue
        previous = history[index - 1]
        assert isinstance(previous, dict)
        phase = str(previous["phase"])
        if phase in stable:
            return phase
    raise OperationStateError("manual rollback history has no stable origin")


def _receipt_baseline_phase(state: dict[str, object]) -> str:
    phase = str(state["phase"])
    if phase in _NORMAL_PHASE_ORDER:
        return phase
    kind = _recovery_kind(state)
    if phase == "rolled_back":
        history = state["phase_history"]
        assert isinstance(history, list)
        if any(entry["phase"] == "installed" for entry in history):
            return _manual_rollback_origin_phase(history)
        return "installing"
    if phase in {"recovering", "manual_rollback"}:
        if phase == "manual_rollback":
            history = state["phase_history"]
            assert isinstance(history, list)
            return _manual_rollback_origin_phase(history)
        return "installing"
    if phase in {"recovery_required", "recovering_policy", "recovering_probe", "recovering_guard"}:
        if kind == "install":
            return "installing"
        if kind == "manual_rollback":
            history = state["phase_history"]
            assert isinstance(history, list)
            return _manual_rollback_origin_phase(history)
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
    if kind == "install" and recovery["runtime_baseline"] != state["prior_runtime"]:
        raise OperationStateError(
            "install recovery runtime baseline must equal immutable prior_runtime"
        )
    transaction = state["active_transaction"]
    if (
        isinstance(transaction, dict)
        and recovery["runtime_baseline"] != transaction["runtime_baseline"]
    ):
        raise OperationStateError(
            "recovery runtime_baseline must equal the immutable active_transaction baseline"
        )
    if latest_kind is not None and kind != latest_kind:
        raise OperationStateError("recovery kind does not match the latest recovery attempt")
    if latest_entry_epoch is not None and recovery["started_epoch"] != latest_entry_epoch:
        raise OperationStateError("recovery start must equal the latest attempt entry epoch")
    if kind == "policy":
        policy_start_entries = [
            entry
            for index, entry in enumerate(history)
            if (
                index > 0
                and entry["phase"] == "recovering_policy"
                and history[index - 1]["phase"] == "policy_enabling"
                and entry["epoch"] == recovery["started_epoch"]
            )
        ]
        if (
            not policy_start_entries
            or policy_start_entries[-1]["epoch"] != recovery["started_epoch"]
            or recovery["evidence_sha256"]
            != policy_start_entries[-1]["evidence_sha256"]
        ):
            raise OperationStateError(
                "policy recovery evidence does not match its attempt-start history"
            )
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
            if kind == "manual_rollback":
                origin = _manual_rollback_origin_phase(history)
                if (
                    target_name == _TARGET_ORDER[-1]
                    and origin in {"policy_enabled", "observed"}
                ):
                    policy = state["policy"]
                    assert isinstance(policy, dict)
                    allowed_previous = {policy["environment_sha256"]}
                else:
                    allowed_previous = {installed_hash}
            else:
                install_cursor = int(install["next_target_index"])
                if index < install_cursor:
                    allowed_previous = {staged_hash}
                elif index == install_cursor:
                    allowed_previous = {restore_hash, staged_hash}
                else:
                    allowed_previous = {restore_hash}
            if recovery["intended_sha256"] != restore_hash:
                raise OperationStateError("recovery intended hash is not bound to snapshot provenance")
            if recovery["previous_sha256"] not in allowed_previous:
                label = (
                    "install recovery previous hash is not bound to the frozen install cursor baseline"
                    if kind == "install"
                    else "recovery previous hash is not bound to the exact live environment provenance"
                )
                raise OperationStateError(label)
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
            committed = isinstance(state.get("policy"), dict)
            intended_environment = (
                enabled_environment if committed else expected_environment
            )
            allowed_previous = (
                {enabled_environment}
                if committed
                else {expected_environment, enabled_environment}
            )
            if recovery["intended_sha256"] != intended_environment:
                raise OperationStateError(
                    "policy recovery intended hash lacks commit-point provenance"
                )
            if recovery["previous_sha256"] not in allowed_previous:
                raise OperationStateError("policy recovery previous hash lacks environment provenance")
        elif any(value is not None for value in cursor):
            raise OperationStateError("policy recovery cursor must clear after the environment target")
    else:
        if index != 0 or any(value is not None for value in cursor):
            raise OperationStateError("probe and guard recovery require a null file cursor")
    completed = recovery["completed_epoch"]
    restored_epoch = recovery["restored_epoch"]
    restore_evidence = recovery["restore_evidence_sha256"]
    if (restored_epoch is None) != (restore_evidence is None):
        raise OperationStateError(
            "recovery restored_epoch and restore_evidence_sha256 must be recorded together"
        )
    if restored_epoch is not None:
        assert isinstance(restored_epoch, int)
        if kind not in {"install", "manual_rollback"}:
            raise OperationStateError(
                "recovery restore evidence is valid only for target restoration"
            )
        if index != len(_TARGET_ORDER) or any(value is not None for value in cursor):
            raise OperationStateError(
                "recovery provisional restoration requires a terminal cursor"
            )
        if restored_epoch < recovery["started_epoch"]:
            raise OperationStateError("recovery restoration precedes recovery start")
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
        if kind in {"install", "manual_rollback"}:
            if restored_epoch is None:
                raise OperationStateError(
                    "completed target recovery requires provisional restore evidence"
                )
            if completed < restored_epoch:
                raise OperationStateError(
                    "recovery completion precedes provisional restoration"
                )
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
    policy_required = index >= _phase_index("policy_enabled")
    if policy_required:
        _require_receipt(state, "policy", True)
    elif baseline_phase == "policy_enabling":
        if state["policy"] is not None:
            _validate_policy(state["policy"])
    else:
        _require_receipt(state, "policy", False)
    _require_receipt(state, "observation", index >= _phase_index("observed"))
    _validate_snapshot_semantics(state)
    _validate_install_semantics(state, index >= _phase_index("installed"))
    _validate_active_transaction_for_phase(state)
    _validate_probe_receipt_semantics(state)
    _validate_dry_run_receipt_semantics(state)
    _validate_policy_environment_semantics(state)
    _validate_observation_receipt_semantics(state)


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
        validated = install["validated_epoch"]
        if validated is not None and validated < install["started_epoch"]:
            raise OperationStateError("install validation precedes install start")
        if completed is not None and validated is not None and completed < validated:
            raise OperationStateError("install completion precedes validation")
    recovery = state["recovery"]
    if recovery is not None:
        assert isinstance(recovery, dict)
        restored = recovery["restored_epoch"]
        completed_recovery = recovery["completed_epoch"]
        if restored is not None and restored < recovery["started_epoch"]:
            raise OperationStateError("recovery restoration precedes recovery start")
        if (
            restored is not None
            and completed_recovery is not None
            and completed_recovery < restored
        ):
            raise OperationStateError("recovery completion precedes restoration")
        if completed_recovery is not None and not any(
            entry["epoch"] == completed_recovery
            and entry["phase"]
            in {"installed", "probed", "policy_enabled", "observed", "rolled_back"}
            for entry in history
        ):
            raise OperationStateError(
                "recovery completion is not bound to a terminal phase epoch"
            )
    transaction = state["active_transaction"]
    if transaction is not None:
        assert isinstance(transaction, dict)
        entering_phase = {
            "probe": "probing",
            "dry_run": "dry_run_recording",
            "policy": "policy_enabling",
            "observe": "observing",
        }[str(transaction["kind"])]
        if transaction["started_epoch"] != _phase_epoch(state, entering_phase):
            raise OperationStateError(
                "active_transaction start must equal its current entry phase epoch"
            )
    policy = state["policy"]
    if policy is not None:
        assert isinstance(policy, dict) and isinstance(install, dict)
        if install["completed_epoch"] is None or policy["enabled_epoch"] < install["completed_epoch"]:
            raise OperationStateError("policy enablement precedes completed install")
        if policy["enabled_epoch"] < _phase_epoch(state, "policy_enabling"):
            raise OperationStateError("policy enablement precedes policy_enabling")
    observation = state["observation"]
    if observation is not None:
        prior_runtime = state["prior_runtime"]
        assert (
            isinstance(observation, dict)
            and isinstance(policy, dict)
            and isinstance(install, dict)
            and isinstance(prior_runtime, dict)
        )
        completed_epoch = install["completed_epoch"]
        assert isinstance(completed_epoch, int)
        cutoffs = [completed_epoch, int(policy["enabled_epoch"])]
        preinstall_trigger_epoch = prior_runtime["preinstall_trigger_epoch"]
        if preinstall_trigger_epoch is not None:
            cutoffs.append(int(preinstall_trigger_epoch))
        if observation["run_epoch"] <= max(cutoffs):
            raise OperationStateError(
                "observation run is not newer than the scheduled-run cutoff or trigger"
            )
        if observation["run_epoch"] > _phase_epoch(state, "observing"):
            raise OperationStateError(
                "observation run is in the future relative to the observing entry"
            )


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
    if old == "recovering_policy" and new in {
        "installed",
        "policy_enabled",
        "recovery_required",
    }:
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
    old_groups = previous["rclone_evidence_groups"]
    new_groups = current["rclone_evidence_groups"]
    assert isinstance(old_groups, list) and isinstance(new_groups, list)
    groups_valid = _is_prefix(old_groups, new_groups)
    if groups_valid and len(new_groups) > len(old_groups):
        appended = new_groups[len(old_groups) :]
        protected_appended = [
            group
            for group in appended
            if isinstance(group, dict)
            and (
                group.get("group_id") == "install"
                or str(group.get("group_id", "")).startswith("task8:")
            )
        ]
        if protected_appended:
            invalid_protected_append = (
                len(appended) != 1
                or len(protected_appended) != 1
                or protected_appended[0].get("after") is not None
                or protected_appended[0].get("evidence_sha256") is not None
            )
            prior_transaction = previous["active_transaction"]
            guard_precedence = (
                previous["phase"] != current["phase"]
                and isinstance(prior_transaction, dict)
                and current["active_transaction"] is None
                and isinstance(prior_transaction.get("guard"), dict)
                and not all(prior_transaction["guard"].values())
            )
            if invalid_protected_append and not guard_precedence:
                raise OperationStateError(
                    "install and Task 8 rclone append must add exactly one pending group"
                )
    if (
        not groups_valid
        and len(old_groups) == len(new_groups)
        and bool(old_groups)
        and old_groups[:-1] == new_groups[:-1]
    ):
        old_last = old_groups[-1]
        new_last = new_groups[-1]
        assert isinstance(old_last, dict) and isinstance(new_last, dict)
        protected_completion = (
            old_last.get("group_id") == "install"
            or str(old_last.get("group_id", "")).startswith("task8:")
        )
        task8_completion = str(old_last.get("group_id", "")).startswith(
            "task8:"
        )
        groups_valid = (
            old_last.get("after") is None
            and old_last.get("evidence_sha256") is None
            and new_last.get("after") is not None
            and new_last.get("evidence_sha256") is not None
            and all(
                old_last.get(field) == new_last.get(field)
                for field in ("group_id", "purpose", "before")
            )
            and (
                not task8_completion
                or (
                    old_last.get("outcome") is None
                    and new_last.get("outcome") in _TASK8_RCLONE_OUTCOMES
                )
            )
            and (
                not protected_completion
                or previous["phase"] == current["phase"]
            )
        )
    if not groups_valid:
        raise OperationStateError("rclone_evidence_groups must remain append-only")
    old_errors = previous["secondary_errors"]
    new_errors = current["secondary_errors"]
    assert isinstance(old_errors, list) and isinstance(new_errors, list)
    if not _is_prefix(old_errors, new_errors):
        raise OperationStateError("secondary_errors must remain append-only")
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
        "runtime_baseline",
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


def _validate_rclone_probe_causality(
    previous: dict[str, object],
    current: dict[str, object],
) -> None:
    old_groups = previous["rclone_evidence_groups"]
    new_groups = current["rclone_evidence_groups"]
    assert isinstance(old_groups, list) and isinstance(new_groups, list)
    old_transaction = previous["active_transaction"]
    new_transaction = current["active_transaction"]
    if not isinstance(old_transaction, dict) or not isinstance(new_transaction, dict):
        return

    groups_changed = old_groups != new_groups
    if groups_changed and (
        old_transaction["guard"] != new_transaction["guard"]
        or old_transaction["probe"] != new_transaction["probe"]
    ):
        raise OperationStateError(
            "rclone audit and transaction flag progress require separate causal writes"
        )

    old_probe = old_transaction["probe"]
    new_probe = new_transaction["probe"]
    if not isinstance(old_probe, dict) or not isinstance(new_probe, dict):
        return
    old_objects = old_probe["objects"]
    new_objects = new_probe["objects"]
    assert isinstance(old_objects, list) and isinstance(new_objects, list)
    advances: list[tuple[str, str, dict[str, object]]] = []
    for old_object, new_object in zip(old_objects, new_objects):
        assert isinstance(old_object, dict) and isinstance(new_object, dict)
        for field in ("created", "verified", "cleaned"):
            if old_object[field] is False and new_object[field] is True:
                advances.append((str(old_object["name"]), field, old_object))
    if not advances:
        return
    if groups_changed:
        raise OperationStateError(
            "probe progress cannot advance with a simultaneous rclone audit change"
        )
    if len(advances) != 1 or not old_groups:
        raise OperationStateError(
            "probe progress lacks one exact completed rclone purpose"
        )
    last_group = old_groups[-1]
    if (
        not isinstance(last_group, dict)
        or last_group["after"] is None
        or last_group.get("outcome") != "success"
        or last_group["evidence_sha256"] is None
    ):
        raise OperationStateError(
            "probe progress requires a completed final rclone purpose"
        )
    try:
        group_kind, group_started, group_attempt, _group_ordinal = (
            _task8_group_identity(last_group)
        )
    except OperationStateError as exc:
        raise OperationStateError(
            "probe progress requires a current-attempt rclone purpose"
        ) from exc
    entries = _task8_attempt_entries(previous, "probe")
    if (
        group_kind != "probe"
        or group_started != old_transaction["started_epoch"]
        or not entries
        or entries[-1]["epoch"] != old_transaction["started_epoch"]
        or group_attempt != len(entries) - 1
    ):
        raise OperationStateError(
            "probe progress purpose is not bound to the current attempt"
        )
    _task8_require_probe_purpose_chain(previous)
    name, field, old_object = advances[0]
    expected_purpose = {
        "created": f"probe-create:{name}:strict-no-existing",
        "verified": f"probe-verify:{name}",
        "cleaned": f"probe-cleanup:{name}",
    }[field]
    last_purpose = str(last_group["purpose"])
    if last_purpose == expected_purpose:
        return
    adopt_purpose = f"probe-adopt:{name}"
    indeterminate = _task8_probe_indeterminate_purposes(previous)
    if (
        field in {"created", "verified"}
        and previous["phase"] == "recovering_probe"
        and last_purpose == adopt_purpose
        and (
            f"probe-create:{name}:strict-no-existing" in indeterminate
            or f"probe-create:{name}:strict-no-existing"
            in _task8_probe_completed_purposes(previous)
        )
        and (field != "verified" or old_object["created"] is True)
    ):
        return
    if (
        field == "cleaned"
        and previous["phase"] == "recovering_probe"
        and last_purpose
        in {
            "probe-recovery-inventory",
            "probe-recovery-root-absence",
            "probe-recovery-parent-absence",
        }
        and old_object["created"] is True
        and old_object["verified"] is True
        and expected_purpose in indeterminate
    ):
        return
    raise OperationStateError(
        "probe progress flag is not bound to the exact completed purpose and object"
    )


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
    if phase == "policy_enabling":
        mutable.add("policy")
    if phase in {"recovering", "manual_rollback", "recovering_policy", "recovering_probe", "recovering_guard"}:
        mutable.add("recovery")
    if previous["active_transaction"] is not None:
        mutable.add("active_transaction")
    for field in _TOP_LEVEL_KEYS - mutable - {"phase_history"}:
        if current[field] != previous[field]:
            raise OperationStateError(f"same-phase mutation changed immutable field {field}")
    if previous["policy"] != current["policy"]:
        if (
            phase != "policy_enabling"
            or previous["policy"] is not None
            or not isinstance(current["policy"], dict)
        ):
            raise OperationStateError("provisional policy receipt is immutable")
        for field in mutable - {"policy"}:
            if current[field] != previous[field]:
                raise OperationStateError(
                    "provisional policy receipt requires its own durable write"
                )
        transaction = previous.get("active_transaction")
        if not isinstance(transaction, dict) or transaction.get("kind") != "policy":
            raise OperationStateError(
                "provisional policy receipt requires an active policy transaction"
            )
        guard = transaction.get("guard")
        required_guard = {
            "timer_stopped": True,
            "service_inactive_verified": True,
            "legacy_lock_acquired": True,
            "runtime_lock_acquired": True,
            "locks_released": False,
            "timer_restored": False,
        }
        if guard != required_guard:
            raise OperationStateError(
                "provisional policy receipt requires stopped runtime and both held locks"
            )
    if phase == "installing":
        assert isinstance(previous["install"], dict) and isinstance(current["install"], dict)
        for field in (
            "started_epoch",
            "installed_hashes",
            "completed_epoch",
            "runtime_directory_created",
        ):
            if current["install"][field] != previous["install"][field]:
                raise OperationStateError("install start and completion receipt are immutable during progress")
        _validate_cursor_progress(previous["install"], current["install"], "install")
        old_validation = (
            previous["install"]["validated_epoch"],
            previous["install"]["validation_evidence_sha256"],
        )
        new_validation = (
            current["install"]["validated_epoch"],
            current["install"]["validation_evidence_sha256"],
        )
        if old_validation != new_validation:
            if old_validation != (None, None) or None in new_validation:
                raise OperationStateError("install provisional validation is immutable")
            if (
                previous["install"]["next_target_index"]
                != current["install"]["next_target_index"]
            ):
                raise OperationStateError(
                    "install cursor and provisional validation require separate writes"
                )
    if "recovery" in mutable:
        old_recovery = previous["recovery"]
        new_recovery = current["recovery"]
        assert isinstance(old_recovery, dict) and isinstance(new_recovery, dict)
        for field in (
            "kind",
            "started_epoch",
            "evidence_sha256",
            "runtime_directory_created",
            "runtime_baseline",
        ):
            if old_recovery[field] != new_recovery[field]:
                raise OperationStateError("recovery attempt identity is immutable")
        _validate_cursor_progress(old_recovery, new_recovery, "recovery")
        old_restoration = (
            old_recovery["restored_epoch"],
            old_recovery["restore_evidence_sha256"],
        )
        new_restoration = (
            new_recovery["restored_epoch"],
            new_recovery["restore_evidence_sha256"],
        )
        if old_restoration != new_restoration:
            if old_restoration != (None, None) or None in new_restoration:
                raise OperationStateError("recovery provisional restoration is immutable")
            if old_recovery["next_target_index"] != new_recovery["next_target_index"]:
                raise OperationStateError(
                    "recovery cursor and provisional restoration require separate writes"
                )
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
        _validate_rclone_probe_causality(previous, current)
        return
    if len(new_history) != len(old_history) + 1 or new_history[:-1] != old_history:
        raise OperationStateError("phase change must append exactly one stable history entry")
    if not _history_transition_allowed(old_phase, new_phase, current):
        raise OperationStateError("forbidden phase transition")
    _validate_append_only_streams(previous, current)
    _validate_rclone_probe_causality(previous, current)
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
        ("recovering_policy", "policy_enabled"),
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
        if (
            (old_phase, new_phase)
            in {
                ("probing", "probed"),
                ("recovering_probe", "installed"),
            }
            and not _task8_probe_remote_cleanup_complete(previous)
        ):
            raise OperationStateError(
                "probe completion lacks terminal current-attempt prefix-empty proof"
            )
        if (old_phase, new_phase) == ("probing", "probed"):
            if any(
                outcome == "indeterminate"
                for _purpose, outcome in _task8_probe_completed_events(previous)
            ):
                raise OperationStateError(
                    "successful probe completion cannot contain indeterminate rclone outcomes"
                )
            probe_transaction = old_transaction["probe"]
            probe_receipt = current["probe"]
            assert isinstance(probe_transaction, dict)
            assert isinstance(probe_receipt, dict)
            objects = probe_transaction["objects"]
            assert isinstance(objects, list)
            owned_names = [str(item["name"]) for item in objects]
            if (
                probe_receipt["prefix"] != probe_transaction["prefix"]
                or probe_receipt["owned_names"] != owned_names
                or probe_receipt["cleanup_proven"] is not True
            ):
                raise OperationStateError(
                    "probe completion receipt is not bound to the transaction identity"
                )
        if (old_phase, new_phase) in {
            ("policy_enabling", "policy_enabled"),
            ("recovering_policy", "policy_enabled"),
        }:
            policy = current["policy"]
            assert isinstance(policy, dict)
            if previous["policy"] != policy:
                raise OperationStateError(
                    "policy receipt must be durable before runtime restoration"
                )
            if policy["environment_sha256"] != old_transaction["policy_environment_sha256"]:
                raise OperationStateError(
                    "policy environment receipt must match the precommitted transaction digest"
                )
    if old_phase == "recovering_policy" and new_phase == "installed":
        if previous["policy"] is not None:
            raise OperationStateError(
                "committed policy recovery must finish forward to policy_enabled"
            )
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
        old_cursor = (
            old_recovery["current_target"],
            old_recovery["previous_sha256"],
            old_recovery["intended_sha256"],
        )
        if old_recovery["next_target_index"] != 1 or any(
            value is not None for value in old_cursor
        ):
            raise OperationStateError(
                "policy recovery prior cursor must be terminal before installed"
            )
        if new_recovery["completed_epoch"] is None:
            raise OperationStateError("policy recovery reset requires recovery completion")
    if old_phase == "recovering_policy" and new_phase == "policy_enabled":
        if not isinstance(previous["policy"], dict):
            raise OperationStateError(
                "uncommitted policy recovery cannot finish forward"
            )
        for field in (
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
            "failure",
            "secondary_errors",
        ):
            if current[field] != previous[field]:
                raise OperationStateError(
                    f"committed policy recovery must preserve {field}"
                )
        old_recovery = previous["recovery"]
        new_recovery = current["recovery"]
        assert isinstance(old_recovery, dict) and isinstance(new_recovery, dict)
        old_cursor = (
            old_recovery["current_target"],
            old_recovery["previous_sha256"],
            old_recovery["intended_sha256"],
        )
        if old_recovery["next_target_index"] != 1 or any(
            value is not None for value in old_cursor
        ):
            raise OperationStateError(
                "committed policy recovery prior cursor must be terminal"
            )
        if new_recovery["completed_epoch"] is None:
            raise OperationStateError(
                "committed policy recovery requires recovery completion"
            )
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
        if (
            install["validated_epoch"] is not None
            or install["validation_evidence_sha256"] is not None
        ):
            raise OperationStateError(
                "installing entry cannot fabricate provisional validation"
            )
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
        for field in (
            "started_epoch",
            "runtime_directory_created",
            "validated_epoch",
            "validation_evidence_sha256",
        ):
            if completed_install[field] != prior_install[field]:
                raise OperationStateError(
                    "install start and provisional receipt are immutable at completion"
                )
    receipt_creation_edges = {
        "probe": ("probing", "probed"),
        "dry_run": ("dry_run_recording", "dry_run_recorded"),
        "observation": ("observing", "observed"),
    }
    policy_reset = old_phase == "recovering_policy" and new_phase == "installed"
    for field, edge in receipt_creation_edges.items():
        if current[field] != previous[field] and (old_phase, new_phase) != edge and not policy_reset:
            raise OperationStateError(f"{field} receipt is immutable outside its completion transition")
    policy_provisional = (
        old_phase == new_phase == "policy_enabling"
        and previous["policy"] is None
        and isinstance(current["policy"], dict)
    )
    if (
        current["policy"] != previous["policy"]
        and not policy_provisional
        and not policy_reset
    ):
        raise OperationStateError(
            "policy receipt is immutable outside its provisional or reset transition"
        )
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
        for field in (
            "kind",
            "started_epoch",
            "evidence_sha256",
            "runtime_directory_created",
            "runtime_baseline",
            "restored_epoch",
            "restore_evidence_sha256",
        ):
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
    expected_paths = build_operation_paths(context.paths.operation_dir)
    if any(
        getattr(context.paths, field) != getattr(expected_paths, field)
        for field in (
            "operation_dir",
            "source_archive",
            "source_dir",
            "snapshot_dir",
            "staged_dir",
            "state_file",
        )
    ):
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
    policy = state.get("policy")
    if isinstance(policy, dict):
        applied_target = policy.get("applied_target")
        assert isinstance(applied_target, dict)
        expected_gid = (
            0
            if context.host_root == _PRODUCTION_HOST_ROOT or os.name != "posix"
            else os.getegid()
        )
        if (
            applied_target.get("uid") != _task7_expected_owner(context)
            or applied_target.get("gid") != expected_gid
        ):
            raise OperationStateError(
                "policy applied target owner does not match the operations context"
            )


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
    *,
    max_output_bytes: int = _MAX_COMMAND_OUTPUT_BYTES,
    timeout_seconds: int | float = _DEFAULT_COMMAND_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    if pass_fds and os.name != "posix":
        raise OperationStateError("inherited file descriptors require POSIX")
    if (
        type(max_output_bytes) is not int
        or max_output_bytes < 0
        or max_output_bytes > _MAX_STATE_BYTES
    ):
        raise OperationStateError("command output limit is invalid")
    if (
        isinstance(timeout_seconds, bool)
        or type(timeout_seconds) not in {int, float}
        or not 0 < timeout_seconds <= 24 * 60 * 60
    ):
        raise OperationStateError("command timeout is invalid")
    common: dict[str, object] = {
        "check": False,
        "shell": False,
        "close_fds": True,
        "pass_fds": pass_fds,
        "env": {
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        },
    }
    discard_output = tuple(argv[:1]) == (_PG_RESTORE_EXECUTABLE,)
    popen_common = dict(common)
    popen_common.pop("check")
    if os.name == "posix":
        popen_common["start_new_session"] = True
    process = subprocess.Popen(
        list(argv),
        stdout=subprocess.DEVNULL if discard_output else subprocess.PIPE,
        stderr=subprocess.DEVNULL if discard_output else subprocess.PIPE,
        text=False,
        **popen_common,
    )
    if not discard_output and (process.stdout is None or process.stderr is None):
        process.kill()
        process.wait()
        raise OperationStateError("command output pipes are unavailable")
    retained: dict[str, bytearray] = {
        "stdout": bytearray(),
        "stderr": bytearray(),
    }
    reader_errors: list[BaseException] = []

    def drain(name: str, stream: object) -> None:
        try:
            while True:
                chunk = stream.read(64 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise OperationStateError("command output stream is invalid")
                remaining = max_output_bytes + 1 - len(retained[name])
                if remaining > 0:
                    retained[name].extend(chunk[:remaining])
        except BaseException as exc:
            reader_errors.append(exc)
        finally:
            try:
                stream.close()
            except BaseException as exc:
                reader_errors.append(exc)

    readers = []
    if not discard_output:
        assert process.stdout is not None and process.stderr is not None
        readers = [
            threading.Thread(
                target=drain,
                args=("stdout", process.stdout),
                name="degen-command-stdout-drain",
                daemon=True,
            ),
            threading.Thread(
                target=drain,
                args=("stderr", process.stderr),
                name="degen-command-stderr-drain",
                daemon=True,
            ),
        ]
    started: list[threading.Thread] = []
    force_kill_signal = getattr(signal, "SIGKILL", signal.SIGTERM)

    def signal_process_tree(signal_number: int) -> None:
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal_number)
            elif process.poll() is None:
                if signal_number == force_kill_signal:
                    process.kill()
                else:
                    process.terminate()
        except ProcessLookupError:
            pass
        except OSError as exc:
            raise OperationStateError("command process cleanup failed") from exc

    def process_group_exists() -> bool:
        if os.name != "posix":
            return process.poll() is None
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError as exc:
            raise OperationStateError("command process-group probe failed") from exc
        return True

    def close_output_pipes() -> None:
        for stream in (process.stdout, process.stderr):
            if stream is None:
                continue
            try:
                stream.close()
            except BaseException:
                pass

    def join_readers(timeout: float) -> None:
        deadline = time.monotonic() + timeout
        for reader in started:
            reader.join(timeout=max(0.0, deadline - time.monotonic()))

    def terminate_process_tree() -> bool:
        cleanup_ok = True
        try:
            signal_process_tree(signal.SIGTERM)
        except OperationStateError:
            cleanup_ok = False
        try:
            process.wait(timeout=_COMMAND_CLEANUP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            cleanup_ok = False
        except BaseException:
            cleanup_ok = False
        join_readers(_COMMAND_CLEANUP_GRACE_SECONDS)
        group_alive = False
        try:
            group_alive = process_group_exists()
        except OperationStateError:
            cleanup_ok = False
        if group_alive or process.poll() is None or any(
            reader.is_alive() for reader in started
        ):
            try:
                signal_process_tree(force_kill_signal)
            except OperationStateError:
                cleanup_ok = False
            try:
                process.wait(timeout=_COMMAND_CLEANUP_GRACE_SECONDS)
            except BaseException:
                cleanup_ok = False
            join_readers(_COMMAND_CLEANUP_GRACE_SECONDS)
        if any(reader.is_alive() for reader in started):
            close_output_pipes()
            join_readers(_COMMAND_CLEANUP_GRACE_SECONDS)
        if process.poll() is None or any(reader.is_alive() for reader in started):
            cleanup_ok = False
        return cleanup_ok

    try:
        for reader in readers:
            reader.start()
            started.append(reader)
        returncode = process.wait(timeout=float(timeout_seconds))
        join_readers(_COMMAND_CLEANUP_GRACE_SECONDS)
        if any(reader.is_alive() for reader in started):
            if not terminate_process_tree():
                raise OperationStateError("command descendant cleanup failed")
            raise OperationStateError("command descendants did not terminate")
    except subprocess.TimeoutExpired:
        if not terminate_process_tree():
            raise OperationStateError("command timeout cleanup failed") from None
        raise OperationStateError("command timeout exceeded") from None
    except BaseException:
        terminate_process_tree()
        raise
    if reader_errors:
        raise OperationStateError("command output capture failed")
    if any(len(value) > max_output_bytes for value in retained.values()):
        raise OperationStateError("command output exceeds the size limit")
    if discard_output:
        stdout = ""
        stderr = ""
    else:
        try:
            stdout = bytes(retained["stdout"]).decode("utf-8", errors="strict")
            stderr = bytes(retained["stderr"]).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise OperationStateError("command output encoding is invalid") from exc
    return subprocess.CompletedProcess(tuple(argv), returncode, stdout, stderr)


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
    validate_operation_state(state, context.paths.operation_dir)
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


def _require_host_file_suffix(
    proof: _HostFileProof,
    expected_suffix: bytes,
) -> None:
    if not expected_suffix or len(expected_suffix) > _MAX_STATE_BYTES:
        raise OperationStateError(f"{proof.label} suffix size is invalid")
    try:
        opened = os.fstat(proof.descriptor)
        if opened.st_size < len(expected_suffix):
            raise OperationStateError(f"{proof.label} is shorter than its required suffix")
        os.lseek(proof.descriptor, -len(expected_suffix), os.SEEK_END)
        observed = _read_exact_descriptor(
            proof.descriptor,
            len(expected_suffix),
            f"{proof.label} suffix",
        )
        if observed != expected_suffix or os.read(proof.descriptor, 1):
            raise OperationStateError(f"{proof.label} required suffix is missing")
    except OperationStateError:
        raise
    except OSError as exc:
        raise OperationStateError(f"{proof.label} suffix read failed") from exc
    _revalidate_host_file_proof(proof)


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
    if len(artifacts) != 9 or _SNAPSHOT_MANIFEST_NAME in artifacts:
        raise OperationStateError("snapshot manifest requires exactly nine artifacts")
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
    if names != sorted(artifacts) or len(names) != 9:
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
    if len(expected_bytes) != 10 or _SNAPSHOT_MANIFEST_NAME not in expected_bytes:
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
    include_safe_stderr: bool = False,
    max_output_bytes: int = _MAX_COMMAND_OUTPUT_BYTES,
) -> subprocess.CompletedProcess[str]:
    if (
        type(max_output_bytes) is not int
        or max_output_bytes < 0
        or max_output_bytes > _MAX_STATE_BYTES
    ):
        raise OperationStateError(f"{label} output limit is invalid")
    try:
        if context.command_runner is _default_command_runner:
            completed = context.command_runner(
                argv,
                pass_fds,
                max_output_bytes=max_output_bytes,
            )
        else:
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
    try:
        stdout_size = len(completed.stdout.encode("utf-8"))
        stderr_size = len(completed.stderr.encode("utf-8"))
    except UnicodeEncodeError:
        _scrub_completed_process(completed)
        raise OperationStateError(f"{label} returned invalid output") from None
    if stdout_size > max_output_bytes or stderr_size > max_output_bytes:
        _scrub_completed_process(completed)
        raise OperationStateError(f"{label} output exceeds the size limit")
    if type(completed.returncode) is not int or completed.returncode != 0:
        detail = ""
        if (
            include_safe_stderr
            and completed.stderr
            and not _string_contains_secret(completed.stderr)
        ):
            detail = sanitize_error_text(completed.stderr)
        completed.stdout = ""
        completed.stderr = ""
        suffix = f": {detail}" if detail else ""
        raise OperationStateError(f"{label} failed{suffix}")
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


@contextlib.contextmanager
def _task8_open_observation_local_evidence(
    context: OperationsContext,
    state: dict[str, object],
    *,
    runtime_lock_fd: int,
    expected_dump_name: str,
    expected_log_suffix: str,
):
    effective = state.get("effective_config")
    if not isinstance(effective, dict):
        raise OperationStateError("observation local configuration is missing")
    if effective.get("KEEP_LOCAL_COUNT") != "2":
        raise OperationStateError(
            "observation requires the exact two-pair local retention policy"
        )
    dump_name = _task8_validate_remote_name(
        expected_dump_name,
        "observation expected local dump",
    )
    if (
        type(expected_log_suffix) is not str
        or not expected_log_suffix
        or not expected_log_suffix.endswith("\n")
        or "\r" in expected_log_suffix
        or "\x00" in expected_log_suffix
        or _task8_utf8_size(expected_log_suffix, "observation log suffix")
        > _MAX_STATE_BYTES
    ):
        raise OperationStateError("observation log suffix is invalid")
    if type(runtime_lock_fd) is not int or runtime_lock_fd < 0:
        raise OperationStateError("observation runtime lock descriptor is invalid")
    try:
        os.fstat(runtime_lock_fd)
    except OSError as exc:
        raise OperationStateError(
            "observation runtime lock descriptor is invalid"
        ) from exc
    backup_dir = _require_string(
        effective.get("BACKUP_DIR"),
        "observation backup directory",
        nonempty=True,
    )
    log_dir = _require_string(
        effective.get("LOG_DIR"),
        "observation log directory",
        nonempty=True,
    )
    prefix = _require_string(
        effective.get("BACKUP_PREFIX"),
        "observation backup prefix",
        nonempty=True,
    )
    if _BACKUP_PREFIX_RE.fullmatch(prefix) is None:
        raise OperationStateError("observation backup prefix is invalid")
    reserve_raw = effective.get("MIN_FREE_AFTER_BYTES")
    if (
        type(reserve_raw) is not str
        or re.fullmatch(r"[0-9]+", reserve_raw, re.ASCII) is None
        or len(reserve_raw) > 20
    ):
        raise OperationStateError("observation disk reserve is invalid")
    reserve_bytes = int(reserve_raw)

    with contextlib.ExitStack() as stack:
        directory = stack.enter_context(_open_host_directory(context, backup_dir))
        inventory = _inventory_backup_directory(directory)
        if len(inventory) != 4:
            raise OperationStateError(
                "observation local inventory must contain exactly two complete pairs"
            )
        dumps: list[tuple[datetime, str, str]] = []
        for name in inventory:
            if name.endswith(".sha256"):
                continue
            match = _BACKUP_NAME_RE.fullmatch(name)
            if (
                match is None
                or match.group("sidecar") is not None
                or match.group("prefix") != prefix
                or f"{name}.sha256" not in inventory
            ):
                raise OperationStateError(
                    "observation local inventory contains an invalid pair"
                )
            try:
                parsed = datetime.strptime(
                    match.group("stamp"),
                    "%Y%m%dT%H%M%SZ",
                ).replace(tzinfo=timezone.utc)
            except ValueError as exc:
                raise OperationStateError(
                    "observation local backup timestamp is invalid"
                ) from exc
            dumps.append((parsed, name, match.group("stamp")))
        dumps.sort(reverse=True)
        if len(dumps) != 2 or dumps[0][1] != dump_name:
            raise OperationStateError(
                "observation newest local pair does not match the scheduled run"
            )
        opened: list[tuple[_HostFileProof, _HostFileProof]] = []
        for _parsed, name, _stamp in dumps:
            dump = stack.enter_context(
                _open_host_file_from_directory(
                    directory,
                    name,
                    "observation local dump",
                    maximum_size=_MAX_BACKUP_DUMP_BYTES,
                )
            )
            sidecar = stack.enter_context(
                _open_host_file_from_directory(
                    directory,
                    f"{name}.sha256",
                    "observation local sidecar",
                    maximum_size=256,
                )
            )
            opened.append((dump, sidecar))
        log = stack.enter_context(
            _open_host_file(
                context,
                f"{log_dir.rstrip('/')}/prod-db-backup.log",
                "observation backup log",
                maximum_size=_MAX_BACKUP_DUMP_BYTES,
            )
        )

        pair_receipts: list[dict[str, object]] = []
        initial_inventory = {
            name: _stable_file_metadata(metadata)
            for name, metadata in inventory.items()
        }

        def validate_pair(
            parsed: datetime,
            stamp: str,
            dump: _HostFileProof,
            sidecar: _HostFileProof,
            *,
            run_archive_check: bool,
        ) -> dict[str, object]:
            sidecar_bytes = _read_host_file(sidecar, 256)
            expected_tail = b"  " + dump.name.encode("ascii") + b"\n"
            if (
                len(sidecar_bytes) != 64 + len(expected_tail)
                or sidecar_bytes[64:] != expected_tail
            ):
                raise OperationStateError(
                    "observation local sidecar record grammar is invalid"
                )
            try:
                recorded = sidecar_bytes[:64].decode("ascii")
            except UnicodeDecodeError as exc:
                raise OperationStateError(
                    "observation local sidecar digest is invalid"
                ) from exc
            if _SHA256_RE.fullmatch(recorded) is None:
                raise OperationStateError(
                    "observation local sidecar digest is invalid"
                )
            observed = _hash_host_file(dump)
            if observed != recorded:
                raise OperationStateError(
                    "observation local dump hash differs from its sidecar"
                )
            if run_archive_check:
                archive_path = str(dump.path)
                if os.name == "posix":
                    proc_path = Path(f"/proc/self/fd/{dump.descriptor}")
                    if not proc_path.exists():
                        raise OperationStateError(
                            "observation descriptor archive path is unavailable"
                        )
                    archive_path = str(proc_path)
                completed = _checked_command(
                    context,
                    (_PG_RESTORE_EXECUTABLE, "--list", archive_path),
                    tuple(dict.fromkeys((dump.descriptor, runtime_lock_fd))),
                    "observation local PostgreSQL archive verification",
                )
                try:
                    if completed.stderr:
                        raise OperationStateError(
                            "observation local archive verification returned stderr"
                        )
                finally:
                    _scrub_completed_process(completed)
                if _hash_host_file(dump) != observed:
                    raise OperationStateError(
                        "observation local dump changed during archive verification"
                    )
            _revalidate_host_file_proof(dump)
            _revalidate_host_file_proof(sidecar)
            return {
                "dump_name": dump.name,
                "sidecar_name": sidecar.name,
                "stamp": stamp,
                "stamp_epoch": int(parsed.timestamp()),
                "dump_size": dump.metadata.st_size,
                "dump_mtime_ns": dump.metadata.st_mtime_ns,
                "sidecar_mtime_ns": sidecar.metadata.st_mtime_ns,
                "dump_sha256": observed,
                "sidecar_sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
                "sidecar_text": sidecar_bytes.decode("ascii"),
            }

        for (parsed, _name, stamp), (dump, sidecar) in zip(
            dumps,
            opened,
            strict=True,
        ):
            pair_receipts.append(
                validate_pair(
                    parsed,
                    stamp,
                    dump,
                    sidecar,
                    run_archive_check=True,
                )
            )
        expected_log_bytes = expected_log_suffix.encode("utf-8")
        _require_host_file_suffix(log, expected_log_bytes)
        if directory.descriptor is not None and hasattr(os, "fstatvfs"):
            filesystem = os.fstatvfs(directory.descriptor)
            free_bytes = int(filesystem.f_bavail) * int(filesystem.f_frsize)
        else:
            free_bytes = int(shutil.disk_usage(directory.path).free)
        if free_bytes < reserve_bytes:
            raise OperationStateError(
                "observation local filesystem is below the required reserve"
            )
        evidence = {
            "backup_dir": backup_dir,
            "prefix": prefix,
            "keep_local_count": 2,
            "free_bytes": free_bytes,
            "reserve_bytes": reserve_bytes,
            "pairs": pair_receipts,
            "newest_dump": dump_name,
            "log_suffix_sha256": hashlib.sha256(
                expected_log_suffix.encode("utf-8")
            ).hexdigest(),
        }
        yield evidence

        final_inventory = _inventory_backup_directory(directory)
        if {
            name: _stable_file_metadata(metadata)
            for name, metadata in final_inventory.items()
        } != initial_inventory:
            raise OperationStateError(
                "observation local backup inventory changed during verification"
            )
        for (parsed, _name, stamp), (dump, sidecar), prior in zip(
            dumps,
            opened,
            pair_receipts,
            strict=True,
        ):
            current = validate_pair(
                parsed,
                stamp,
                dump,
                sidecar,
                run_archive_check=False,
            )
            if current != prior:
                raise OperationStateError(
                    "observation local pair changed during verification"
                )
        _require_host_file_suffix(log, expected_log_bytes)


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
        values_for_validation = dict(parsed.values)
        if values_for_validation.get("LOG_DIR") == "/var/log/degen":
            values_for_validation["LOG_DIR"] = helper.MANAGED_DEFAULTS["LOG_DIR"]
        effective = helper.validate_effective_configuration(
            values_for_validation,
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
    enabled_environment_sha256: str,
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
        "enabled_sha256": enabled_environment_sha256,
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
        enabled_environment_sha256 = hashlib.sha256(
            _task8_enabled_environment_bytes(environment_raw)
        ).hexdigest()
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
            enabled_environment_sha256,
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
            "enabled_environment_sha256": enabled_environment_sha256,
        }
        if host_stage != expected_receipt:
            _close_host_stage_proof(proof)
            raise OperationStateError("host-stage receipt does not match held stage bytes")
        return proof, manifest
    except BaseException:
        _close_stage_directories(stage_directories)
        raise


def _open_snapshot_proof_from_state(
    context: OperationsContext,
    operation_directory_fd: int | None,
    operation_metadata: os.stat_result,
    state: dict[str, object],
) -> _SnapshotDirectoryProof:
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    expected_names: set[str] = {_RCLONE_AUDIT_NAME, _SNAPSHOT_MANIFEST_NAME}
    for target_name in _TARGET_ORDER:
        receipt = targets[target_name]
        assert isinstance(receipt, dict)
        basename = _SNAPSHOT_TARGET_NAMES[target_name]
        expected_names.add(basename if receipt["present"] else f"{basename}.absent")
    directory_fd: int | None = None
    file_descriptors: dict[str, int] = {}
    file_metadata: dict[str, os.stat_result] = {}
    expected_bytes: dict[str, bytes] = {}
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    file_flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    file_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        use_descriptors = os.name == "posix" and operation_directory_fd is not None
        if use_descriptors:
            directory_fd = os.open(
                "snapshot",
                directory_flags,
                dir_fd=operation_directory_fd,
            )
            directory_metadata = os.fstat(directory_fd)
            named_directory = os.stat(
                "snapshot",
                dir_fd=operation_directory_fd,
                follow_symlinks=False,
            )
        else:
            named_directory = context.paths.snapshot_dir.lstat()
            directory_metadata = named_directory
        _validate_snapshot_directory_metadata(directory_metadata, context)
        _validate_snapshot_directory_metadata(named_directory, context)
        if not _same_identity(directory_metadata, named_directory):
            raise OperationStateError("snapshot directory binding changed")
        _revalidate_operation_dir_binding(
            context.paths.operation_dir,
            operation_directory_fd,
            operation_metadata,
            context.effective_uid,
        )
        try:
            inventory = {
                entry.name
                for entry in os.scandir(
                    directory_fd
                    if directory_fd is not None
                    else context.paths.snapshot_dir
                )
            }
        except OSError as exc:
            raise OperationStateError("snapshot inventory cannot be read") from exc
        if inventory != expected_names:
            raise OperationStateError("snapshot artifact inventory changed")
        for name in sorted(expected_names):
            _validate_source_basename(name)
            descriptor = (
                os.open(name, file_flags, dir_fd=directory_fd)
                if directory_fd is not None
                else os.open(context.paths.snapshot_dir / name, file_flags)
            )
            file_descriptors[name] = descriptor
            opened = os.fstat(descriptor)
            named = (
                os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if directory_fd is not None
                else (context.paths.snapshot_dir / name).lstat()
            )
            if (
                not _snapshot_file_metadata_is_safe(opened, context)
                or not _same_identity(opened, named)
            ):
                raise OperationStateError("snapshot artifact binding changed")
            file_metadata[name] = opened
            os.lseek(descriptor, 0, os.SEEK_SET)
            raw = _read_exact_descriptor(
                descriptor,
                opened.st_size,
                "snapshot artifact",
            )
            opened_after = os.fstat(descriptor)
            named_after = (
                os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if directory_fd is not None
                else (context.paths.snapshot_dir / name).lstat()
            )
            if (
                _stage_stable_metadata(opened_after)
                != _stage_stable_metadata(opened)
                or not _same_identity(opened_after, named_after)
            ):
                raise OperationStateError("snapshot artifact changed while reading")
            expected_bytes[name] = raw
        artifacts = {
            name: raw
            for name, raw in expected_bytes.items()
            if name != _SNAPSHOT_MANIFEST_NAME
        }
        for target_name in _TARGET_ORDER:
            receipt = targets[target_name]
            assert isinstance(receipt, dict)
            basename = _SNAPSHOT_TARGET_NAMES[target_name]
            name = basename if receipt["present"] else f"{basename}.absent"
            raw = artifacts[name]
            if receipt["present"]:
                if hashlib.sha256(raw).hexdigest() != receipt["sha256"]:
                    raise OperationStateError(
                        "snapshot target artifact differs from its state receipt"
                    )
            elif raw != f"ABSENT {target_name}\n".encode("ascii"):
                raise OperationStateError("snapshot absence marker is invalid")
        audit = artifacts[_RCLONE_AUDIT_NAME]
        rclone_receipt = snapshot["rclone_audit"]
        assert isinstance(rclone_receipt, dict)
        if hashlib.sha256(audit).hexdigest() != rclone_receipt["sha256"]:
            raise OperationStateError(
                "snapshot rclone audit differs from its state receipt"
            )
        manifest = expected_bytes[_SNAPSHOT_MANIFEST_NAME]
        if hashlib.sha256(manifest).hexdigest() != snapshot["manifest_sha256"]:
            raise OperationStateError(
                "snapshot manifest differs from its state receipt"
            )
        _parse_snapshot_manifest(manifest, artifacts)
        proof = _SnapshotDirectoryProof(
            context,
            operation_directory_fd,
            directory_fd,
            directory_metadata,
            expected_bytes,
            file_descriptors,
            file_metadata,
        )
        _revalidate_snapshot_directory_proof(proof)
        return proof
    except BaseException:
        for descriptor in file_descriptors.values():
            try:
                os.close(descriptor)
            except OSError:
                pass
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except OSError:
                pass
        raise


@dataclass
class _VerifiedTransactionMaterial:
    source: _VerifiedSourceMaterial
    stage_proof: _HostStageProof
    stage_manifest: dict[str, object]
    snapshot_proof: _SnapshotDirectoryProof
    installed_helper_proof: _HostFileProof | None


@contextlib.contextmanager
def _open_running_installed_helper_proof(
    context: OperationsContext,
    state: dict[str, object],
):
    logical_path = "/usr/local/sbin/degen-prod-db-backup-ops"
    expected_path = _host_path(context, logical_path)
    running_path = Path(os.path.abspath(str(__file__)))
    expected_absolute = Path(os.path.abspath(str(expected_path)))
    if running_path != expected_absolute:
        raise OperationStateError(
            "Task 8 commands must run the exact installed helper path"
        )
    install = state.get("install")
    reviewed = state.get("reviewed_source")
    if not isinstance(install, dict) or not isinstance(reviewed, dict):
        raise OperationStateError("installed helper provenance receipt is missing")
    installed_hashes = install.get("installed_hashes")
    reviewed_hashes = reviewed.get("asset_hashes")
    if not isinstance(installed_hashes, dict) or not isinstance(reviewed_hashes, dict):
        raise OperationStateError("installed helper provenance receipt is invalid")
    expected_hash = installed_hashes.get(logical_path)
    reviewed_hash = reviewed_hashes.get(
        "deploy/linux/degen-prod-db-backup-ops.py"
    )
    if not isinstance(expected_hash, str) or expected_hash != reviewed_hash:
        raise OperationStateError("installed helper hash provenance is invalid")
    with _open_host_file(
        context,
        logical_path,
        "installed helper",
        maximum_size=_MAX_SOURCE_FILE_BYTES,
        exact_mode=0o755,
    ) as proof:
        if _hash_host_file(proof) != expected_hash:
            raise OperationStateError(
                "installed helper hash does not match installed provenance"
            )
        _revalidate_host_file_proof(proof)
        yield proof
        _revalidate_host_file_proof(proof)


def _revalidate_transaction_material(
    material: _VerifiedTransactionMaterial,
) -> None:
    if material.installed_helper_proof is not None:
        _revalidate_host_file_proof(material.installed_helper_proof)
    _revalidate_verified_source_material(material.source)
    _revalidate_host_stage_proof(
        material.stage_proof,
        material.stage_manifest,
    )
    _revalidate_snapshot_directory_proof(material.snapshot_proof)
    _revalidate_verified_source_material(material.source)
    if material.installed_helper_proof is not None:
        _revalidate_host_file_proof(material.installed_helper_proof)


@contextlib.contextmanager
def _open_verified_transaction_material(
    context: OperationsContext,
    allowed_phases: frozenset[str],
    *,
    helper_route: str = "source",
):
    if helper_route not in {"source", "installed"}:
        raise OperationStateError("transaction helper route is invalid")
    _validate_source_context(context, context.paths.source_dir)
    source_proof: _SourceTreeProof | None = None
    stage_proof: _HostStageProof | None = None
    snapshot_proof: _SnapshotDirectoryProof | None = None
    with _open_validated_operation_dir(
        context.paths.operation_dir,
        context.effective_uid,
    ) as directory_fd:
        operation_metadata = (
            context.paths.operation_dir.lstat()
            if directory_fd is None
            else os.fstat(directory_fd)
        )
        with _open_source_archive(context, directory_fd) as (
            archive_fd,
            archive_metadata,
        ):
            try:
                if _hash_source_archive(archive_fd) != context.expected_archive_sha256:
                    raise OperationStateError(
                        "source archive SHA-256 does not match the approved digest"
                    )
                _revalidate_source_archive(
                    context,
                    directory_fd,
                    archive_fd,
                    archive_metadata,
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
                if state is None or state["phase"] not in allowed_phases:
                    raise OperationStateError(
                        "operation state is not in an allowed transaction phase"
                    )
                validate_operation_state_for_context(state, context)
                reviewed = state["reviewed_source"]
                effective_config = state["effective_config"]
                host_stage = state["host_stage"]
                snapshot = state["snapshot"]
                prior_runtime = state["prior_runtime"]
                history = state["phase_history"]
                assert isinstance(reviewed, dict)
                assert isinstance(effective_config, dict)
                assert isinstance(host_stage, dict)
                assert isinstance(snapshot, dict)
                assert isinstance(prior_runtime, dict)
                assert isinstance(history, list)
                expected_reviewed: dict[str, object] = {
                    "commit": context.expected_commit,
                    "archive_sha256": context.expected_archive_sha256,
                    "manifest_sha256": context.expected_manifest_sha256,
                    "asset_hashes": dict(source_proof.asset_hashes),
                }
                expected_prefix = [
                    (
                        "source_verified",
                        hashlib.sha256(
                            _source_verification_evidence(expected_reviewed)
                        ).hexdigest(),
                    ),
                    (
                        "staging_prepared",
                        hashlib.sha256(
                            _staging_evidence(effective_config, host_stage)
                        ).hexdigest(),
                    ),
                    (
                        "snapshotted",
                        hashlib.sha256(
                            _snapshot_evidence(snapshot, prior_runtime)
                        ).hexdigest(),
                    ),
                ]
                if reviewed != expected_reviewed or len(history) < len(expected_prefix):
                    raise OperationStateError(
                        "transaction source history is not provenance-bound"
                    )
                for entry, (phase, evidence) in zip(history, expected_prefix):
                    if entry["phase"] != phase or entry["evidence_sha256"] != evidence:
                        raise OperationStateError(
                            "transaction source history is not provenance-bound"
                        )
                running_helper = Path(os.path.abspath(str(__file__)))
                expected_helper = Path(
                    os.path.abspath(
                        str(
                            context.paths.source_dir
                            / "deploy/linux/degen-prod-db-backup-ops.py"
                        )
                    )
                )
                if helper_route == "source" and context.host_root == _PRODUCTION_HOST_ROOT:
                    if running_helper != expected_helper:
                        raise OperationStateError(
                            "transaction helper is outside verified source"
                        )
                    helper_proofs = [
                        item
                        for item in source_proof.files
                        if item.relative == "deploy/linux/degen-prod-db-backup-ops.py"
                    ]
                    if len(helper_proofs) != 1:
                        raise OperationStateError(
                            "verified transaction helper proof is incomplete"
                        )
                    helper_digest, _contents = _hash_open_source_file(
                        helper_proofs[0].descriptor,
                        helper_proofs[0].metadata,
                        "verified transaction helper",
                        capture=False,
                    )
                    if helper_digest != expected_reviewed["asset_hashes"][
                        "deploy/linux/degen-prod-db-backup-ops.py"
                    ]:
                        raise OperationStateError(
                            "verified transaction helper hash changed"
                        )
                helper_proof_context = (
                    _open_running_installed_helper_proof(context, state)
                    if helper_route == "installed"
                    else contextlib.nullcontext(None)
                )
                with helper_proof_context as installed_helper_proof:
                    source = _VerifiedSourceMaterial(
                        context,
                        directory_fd,
                        operation_metadata,
                        archive_fd,
                        archive_metadata,
                        source_proof,
                        state,
                    )
                    stage_proof, stage_manifest = _open_existing_host_stage_proof(
                        context,
                        source,
                    )
                    snapshot_proof = _open_snapshot_proof_from_state(
                        context,
                        directory_fd,
                        operation_metadata,
                        state,
                    )
                    material = _VerifiedTransactionMaterial(
                        source,
                        stage_proof,
                        stage_manifest,
                        snapshot_proof,
                        installed_helper_proof,
                    )
                    _revalidate_transaction_material(material)
                    yield material
            finally:
                if snapshot_proof is not None:
                    _close_snapshot_directory_proof(snapshot_proof)
                if stage_proof is not None:
                    _close_host_stage_proof(stage_proof)
                if source_proof is not None:
                    _close_source_tree_proof(source_proof)


def _host_stage_manifest(
    context: OperationsContext,
    asset_hashes: dict[str, str],
    environment_sha256: str,
    enabled_environment_sha256: str,
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
            "enabled_sha256": enabled_environment_sha256,
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


def _task8_enabled_environment_bytes(disabled: bytes) -> bytes:
    if type(disabled) is not bytes or not disabled:
        raise OperationStateError("disabled managed environment bytes are invalid")
    disabled_assignment = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled_assignment = b"REMOTE_PRUNE_ENABLED=1\n"
    if disabled.count(disabled_assignment) != 1:
        raise OperationStateError(
            "disabled managed environment must contain one exact prune marker"
        )
    if enabled_assignment in disabled:
        raise OperationStateError(
            "disabled managed environment already contains the enabled prune marker"
        )
    return disabled.replace(disabled_assignment, enabled_assignment, 1)


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
        enabled_environment_sha256 = hashlib.sha256(
            _task8_enabled_environment_bytes(environment_bytes)
        ).hexdigest()
        manifest = _host_stage_manifest(
            context,
            asset_hashes,
            environment_sha256,
            enabled_environment_sha256,
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
            "enabled_environment_sha256": enabled_environment_sha256,
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


def _task7_expected_owner(context: OperationsContext) -> int:
    return 0 if context.host_root == _PRODUCTION_HOST_ROOT else context.effective_uid


def _validate_task7_directory(
    metadata: os.stat_result,
    context: OperationsContext,
    label: str,
    *,
    exact_mode: int,
) -> None:
    if not stat.S_ISDIR(metadata.st_mode):
        raise OperationStateError(f"{label} is not a real directory")
    if metadata.st_uid != _task7_expected_owner(context):
        raise OperationStateError(f"{label} owner is unsafe")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != exact_mode:
        raise OperationStateError(f"{label} mode is unsafe")


@dataclass
class _Task7DirectoryProof:
    context: OperationsContext
    host_root_proof: _HostRootProof
    logical_path: str
    descriptors: list[int]
    names: list[str]
    metadata: list[os.stat_result]
    modes: list[int]

    @property
    def descriptor(self) -> int:
        return self.descriptors[-1]


def _revalidate_task7_directory_proof(proof: _Task7DirectoryProof) -> None:
    _revalidate_host_root_proof(proof.host_root_proof)
    if len(proof.descriptors) != len(proof.metadata) or len(proof.names) != len(
        proof.modes
    ):
        raise OperationStateError("Task 7 directory proof is incomplete")
    root_opened = os.fstat(proof.descriptors[0])
    root_named = os.fstat(proof.host_root_proof.descriptors[-1])
    if not _same_identity(root_opened, root_named):
        raise OperationStateError("Task 7 host-root binding changed")
    for index, (name, expected_mode) in enumerate(zip(proof.names, proof.modes), start=1):
        opened = os.fstat(proof.descriptors[index])
        named = os.stat(
            name,
            dir_fd=proof.descriptors[index - 1],
            follow_symlinks=False,
        )
        _validate_task7_directory(
            opened,
            proof.context,
            proof.logical_path,
            exact_mode=expected_mode,
        )
        if (
            not _same_identity(opened, named)
            or _task7_directory_stable_metadata(opened)
            != _task7_directory_stable_metadata(proof.metadata[index])
        ):
            raise OperationStateError("Task 7 directory binding changed")


def _task7_directory_stable_metadata(metadata: os.stat_result) -> tuple[int, ...]:
    """Return directory security metadata that child creation cannot change.

    Creating the runtime directory or either lock file legitimately changes a
    held parent directory's size, link count, mtime, and ctime.  The proof must
    still bind its inode, owner, group, type, and exact permission bits across
    that controlled mutation.
    """
    return (
        metadata.st_dev,
        metadata.st_ino,
        stat.S_IFMT(metadata.st_mode),
        stat.S_IMODE(metadata.st_mode),
        metadata.st_uid,
        metadata.st_gid,
    )


def _close_task7_directory_proof(proof: _Task7DirectoryProof) -> None:
    for descriptor in reversed(proof.descriptors):
        try:
            os.close(descriptor)
        except OSError:
            pass
    proof.descriptors.clear()
    _close_host_root_proof(proof.host_root_proof)


def _open_task7_directory(
    context: OperationsContext,
    logical_path: str,
    *,
    exact_mode: int,
) -> _Task7DirectoryProof:
    _host_path(context, logical_path)
    components = list(PurePosixPath(logical_path).parts[1:])
    if not components or components[0] != "run":
        raise OperationStateError("Task 7 directory must be under /run")
    host_root_proof = _open_host_root_proof(context)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptors: list[int] = []
    names: list[str] = []
    metadata: list[os.stat_result] = []
    modes: list[int] = []
    try:
        root_fd = os.dup(host_root_proof.descriptors[-1])
        descriptors.append(root_fd)
        metadata.append(os.fstat(root_fd))
        for index, component in enumerate(components):
            _validate_source_basename(component)
            mode = exact_mode if index == len(components) - 1 else 0o755
            named_before = os.stat(
                component,
                dir_fd=descriptors[-1],
                follow_symlinks=False,
            )
            _validate_task7_directory(
                named_before,
                context,
                logical_path,
                exact_mode=mode,
            )
            child_fd = os.open(component, flags, dir_fd=descriptors[-1])
            try:
                opened = os.fstat(child_fd)
                named_after = os.stat(
                    component,
                    dir_fd=descriptors[-1],
                    follow_symlinks=False,
                )
            except BaseException:
                os.close(child_fd)
                raise
            if (
                not _same_identity(named_before, opened)
                or not _same_identity(opened, named_after)
            ):
                os.close(child_fd)
                raise OperationStateError(f"{logical_path} binding changed")
            descriptors.append(child_fd)
            names.append(component)
            metadata.append(opened)
            modes.append(mode)
        proof = _Task7DirectoryProof(
            context,
            host_root_proof,
            logical_path,
            descriptors,
            names,
            metadata,
            modes,
        )
        _revalidate_task7_directory_proof(proof)
        return proof
    except BaseException as exc:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass
        _close_host_root_proof(host_root_proof)
        if isinstance(exc, OSError):
            raise OperationStateError(
                f"{logical_path} cannot be opened safely"
            ) from exc
        raise


def _validate_task7_lock_file(
    metadata: os.stat_result,
    context: OperationsContext,
    label: str,
) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise OperationStateError(f"{label} is not a regular file")
    if metadata.st_uid != _task7_expected_owner(context):
        raise OperationStateError(f"{label} owner is unsafe")
    if metadata.st_nlink != 1:
        raise OperationStateError(f"{label} must have one link")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise OperationStateError(f"{label} mode is unsafe")


def _open_and_flock_task7_file(
    context: OperationsContext,
    parent_fd: int,
    name: str,
    label: str,
) -> int:
    try:
        import fcntl
    except ImportError as exc:
        raise OperationStateError("required POSIX flock primitive is unavailable") from exc
    descriptor: int | None = None
    created = False
    base_flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    base_flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        try:
            named_before = os.stat(
                name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            named_before = None
        if named_before is None:
            descriptor = os.open(
                name,
                base_flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=parent_fd,
            )
            created = True
            os.fchmod(descriptor, 0o600)
        else:
            _validate_task7_lock_file(named_before, context, label)
            descriptor = os.open(name, base_flags, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        named_after = os.stat(
            name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        for metadata in (opened, named_after):
            _validate_task7_lock_file(metadata, context, label)
        if not _same_identity(opened, named_after):
            raise OperationStateError(f"{label} path/descriptor binding changed")
        if named_before is not None and not _same_identity(named_before, opened):
            raise OperationStateError(f"{label} changed while opening")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationStateError(f"{label} is busy with a contender") from exc
        locked = os.fstat(descriptor)
        named_locked = os.stat(
            name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        if (
            not _same_identity(locked, named_locked)
            or _stage_stable_metadata(locked)
            != _stage_stable_metadata(opened)
        ):
            raise OperationStateError(f"{label} changed after locking")
        result = descriptor
        descriptor = None
        return result
    except FileExistsError as exc:
        raise OperationStateError(f"{label} exclusive creation raced") from exc
    except OSError as exc:
        raise OperationStateError(f"{label} cannot be opened safely") from exc
    finally:
        if descriptor is not None:
            try:
                if not created:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(descriptor)
            except OSError:
                pass


def _open_or_create_runtime_directory_proof(
    context: OperationsContext,
    *,
    require_create: bool,
) -> tuple[bool, _Task7DirectoryProof]:
    _require_posix_descriptor_primitives()
    proof = _open_task7_directory(
        context,
        "/run",
        exact_mode=0o755,
    )
    created = False
    runtime_fd: int | None = None
    name = "degen-prod-db-backup"
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        try:
            named_before = os.stat(
                name,
                dir_fd=proof.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            named_before = None
        if named_before is None:
            try:
                os.mkdir(name, 0o700, dir_fd=proof.descriptor)
            except FileExistsError as exc:
                raise OperationStateError(
                    "runtime directory exclusive creation raced"
                ) from exc
            created = True
        else:
            if require_create:
                raise OperationStateError(
                    "runtime directory appeared after the held absence proof"
                )
            _validate_task7_directory(
                named_before,
                context,
                "runtime directory",
                exact_mode=0o700,
            )
        runtime_fd = os.open(name, flags, dir_fd=proof.descriptor)
        if created:
            os.fchmod(runtime_fd, 0o700)
        opened = os.fstat(runtime_fd)
        named_after = os.stat(
            name,
            dir_fd=proof.descriptor,
            follow_symlinks=False,
        )
        for metadata in (opened, named_after):
            _validate_task7_directory(
                metadata,
                context,
                "runtime directory",
                exact_mode=0o700,
            )
        if not _same_identity(opened, named_after):
            raise OperationStateError("runtime directory binding changed")
        if named_before is not None and not _same_identity(named_before, opened):
            raise OperationStateError("runtime directory changed while opening")
        proof.descriptors.append(runtime_fd)
        proof.names.append(name)
        proof.metadata.append(opened)
        proof.modes.append(0o700)
        proof.logical_path = "/run/degen-prod-db-backup"
        runtime_fd = None
        _revalidate_task7_directory_proof(proof)
        if created:
            os.fsync(proof.descriptors[-2])
            _atomic_event_hook("task7_after_runtime_directory_create")
        return created, proof
    except OSError as exc:
        if runtime_fd is not None:
            try:
                os.close(runtime_fd)
            except OSError:
                pass
        _close_task7_directory_proof(proof)
        raise OperationStateError("runtime directory cannot be created safely") from exc
    except BaseException:
        if runtime_fd is not None:
            try:
                os.close(runtime_fd)
            except OSError:
                pass
        _close_task7_directory_proof(proof)
        raise


def ensure_runtime_directory(context: OperationsContext) -> bool:
    created, proof = _open_or_create_runtime_directory_proof(
        context,
        require_create=False,
    )
    _close_task7_directory_proof(proof)
    return created


def _require_backup_service_inactive(context: OperationsContext) -> None:
    _parse_positive_main_pid(
        _systemctl_show_unit(context, "degen-prod-db-backup.service"),
        "backup service",
        require_active=False,
    )


def _acquire_migration_locks(
    context: OperationsContext,
    *,
    require_runtime_create: bool,
    before_action: Callable[[str], None] | None = None,
    after_action: Callable[[str, int], None] | None = None,
) -> MigrationLocks:
    _require_posix_descriptor_primitives()
    legacy_fd: int | None = None
    runtime_fd: int | None = None
    lock_parent_proof: _Task7DirectoryProof | None = None
    runtime_parent_proof: _Task7DirectoryProof | None = None
    try:
        lock_parent_proof = _open_task7_directory(
            context,
            "/run/lock",
            exact_mode=0o1777,
        )
        if before_action is not None:
            before_action("legacy_lock_acquire")
        _atomic_event_hook("task7_before_legacy_lock")
        legacy_fd = _open_and_flock_task7_file(
            context,
            lock_parent_proof.descriptor,
            "degen-prod-db-backup.lock",
            "legacy migration lock",
        )
        if after_action is not None:
            after_action("legacy_lock_acquire", legacy_fd)
        _atomic_event_hook(
            "migration_lock_acquired",
            kind="legacy",
            fd=legacy_fd,
        )
        _atomic_event_hook("task7_after_legacy_lock")
        _created, runtime_parent_proof = _open_or_create_runtime_directory_proof(
            context,
            require_create=require_runtime_create,
        )
        _revalidate_task7_directory_proof(runtime_parent_proof)
        if before_action is not None:
            before_action("runtime_lock_acquire")
        _atomic_event_hook("task7_before_runtime_lock")
        runtime_fd = _open_and_flock_task7_file(
            context,
            runtime_parent_proof.descriptor,
            "backup.lock",
            "runtime migration lock",
        )
        if after_action is not None:
            after_action("runtime_lock_acquire", runtime_fd)
        _atomic_event_hook(
            "migration_lock_acquired",
            kind="runtime",
            fd=runtime_fd,
        )
        _atomic_event_hook("task7_after_runtime_lock")
        _revalidate_task7_directory_proof(lock_parent_proof)
        _revalidate_task7_directory_proof(runtime_parent_proof)
        if before_action is not None:
            before_action("post_lock_service_recheck")
        _require_backup_service_inactive(context)
        locks = MigrationLocks(legacy_fd=legacy_fd, runtime_fd=runtime_fd)
        legacy_fd = None
        runtime_fd = None
        return locks
    finally:
        if runtime_parent_proof is not None:
            _close_task7_directory_proof(runtime_parent_proof)
        if lock_parent_proof is not None:
            _close_task7_directory_proof(lock_parent_proof)
        for descriptor in (runtime_fd, legacy_fd):
            if descriptor is not None:
                try:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except (ImportError, OSError):
                    pass
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def acquire_migration_locks(
    context: OperationsContext,
    *,
    before_action: Callable[[str], None] | None = None,
    after_action: Callable[[str, int], None] | None = None,
) -> MigrationLocks:
    return _acquire_migration_locks(
        context,
        require_runtime_create=False,
        before_action=before_action,
        after_action=after_action,
    )


def _release_migration_locks(
    locks: MigrationLocks,
    *,
    before_action: Callable[[str], None] | None = None,
) -> list[_Task7LockReleaseIssue]:
    try:
        import fcntl
    except ImportError:
        fcntl = None  # type: ignore[assignment]
    errors: list[_Task7LockReleaseIssue] = []
    crashes: list[BaseException] = []
    for kind, descriptor in (
        ("runtime", locks.runtime_fd),
        ("legacy", locks.legacy_fd),
    ):
        stage = f"release_{kind}_lock"
        if before_action is not None:
            try:
                before_action(f"{kind}_lock_release")
            except BaseException as exc:
                crashes.append(exc)
        try:
            _atomic_event_hook(f"task7_before_{kind}_lock_release")
        except BaseException as exc:
            if isinstance(exc, Exception):
                errors.append(_Task7LockReleaseIssue(stage, exc, False))
            else:
                crashes.append(exc)
        if fcntl is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError as exc:
                errors.append(_Task7LockReleaseIssue(stage, exc, True))
        elif os.name == "posix":
            errors.append(
                _Task7LockReleaseIssue(
                    stage,
                    OperationStateError(
                        "required POSIX flock primitive is unavailable"
                    ),
                    True,
                )
            )
        try:
            os.close(descriptor)
        except OSError as exc:
            errors.append(_Task7LockReleaseIssue(stage, exc, True))
        try:
            _atomic_event_hook("migration_lock_released", kind=kind, fd=descriptor)
            _atomic_event_hook(f"task7_after_{kind}_lock_release")
        except BaseException as exc:
            if isinstance(exc, Exception):
                errors.append(_Task7LockReleaseIssue(stage, exc, False))
            else:
                crashes.append(exc)
    try:
        _atomic_event_hook("migration_locks_released")
    except BaseException as exc:
        if isinstance(exc, Exception):
            errors.append(
                _Task7LockReleaseIssue("release_migration_locks", exc, False)
            )
        else:
            crashes.append(exc)
    if crashes:
        raise crashes[0]
    return errors


def verify_running_source_helper(context: OperationsContext) -> None:
    relative = "deploy/linux/degen-prod-db-backup-ops.py"
    expected = context.paths.source_dir / relative
    running = Path(os.path.abspath(str(__file__)))
    expected_absolute = Path(os.path.abspath(str(expected)))
    if running != expected_absolute:
        raise OperationStateError(
            "recover must run the helper under the verified source directory"
        )
    descriptor: int | None = None
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        named_before = expected.lstat()
        _validate_source_file_metadata(
            named_before,
            context.effective_uid,
            "verified source helper",
        )
        descriptor = os.open(expected, flags)
        opened_before = os.fstat(descriptor)
        _validate_source_file_metadata(
            opened_before,
            context.effective_uid,
            "verified source helper",
        )
        if not _same_identity(named_before, opened_before):
            raise OperationStateError("verified source helper binding changed")
        digest, _contents = _hash_open_source_file(
            descriptor,
            opened_before,
            "verified source helper",
            capture=False,
        )
        named_after = expected.lstat()
        _validate_source_file_metadata(
            named_after,
            context.effective_uid,
            "verified source helper",
        )
        if (
            not _same_identity(opened_before, named_after)
            or _source_binding_metadata(opened_before)
            != _source_binding_metadata(named_after)
        ):
            raise OperationStateError("verified source helper binding changed")
        state = load_operation_state(
            context.paths.state_file,
            effective_uid=context.effective_uid,
        )
        validate_operation_state_for_context(state, context)
        reviewed = state["reviewed_source"]
        assert isinstance(reviewed, dict)
        hashes = reviewed["asset_hashes"]
        assert isinstance(hashes, dict)
        if digest != hashes[relative]:
            raise OperationStateError(
                "running helper hash does not match verified source"
            )
    except OSError as exc:
        raise OperationStateError("verified source helper proof failed") from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def verify_running_installed_helper(context: OperationsContext) -> None:
    state = load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    validate_operation_state_for_context(state, context)
    with _open_running_installed_helper_proof(context, state):
        return


def _task7_epoch(context: OperationsContext) -> int:
    now = context.clock()
    if type(now) is not datetime or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise OperationStateError("operations clock must return an aware UTC datetime")
    return int(now.astimezone(timezone.utc).timestamp())


def _task7_state_epoch(
    context: OperationsContext,
    state: dict[str, object],
) -> int:
    epoch = _task7_epoch(context)
    history = state["phase_history"]
    assert isinstance(history, list) and history
    last = history[-1]
    assert isinstance(last, dict)
    return max(epoch, int(last["epoch"]))


def _task7_evidence_sha256(label: str, payload: object) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(
        f"degen-task7-{label}-v1\n".encode("ascii") + canonical + b"\n"
    ).hexdigest()


def _task7_error_evidence_sha256(
    kind: str,
    receipt: dict[str, object],
) -> str:
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
        raise OperationStateError("failure evidence kind is invalid")
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


def _task7_systemctl(
    context: OperationsContext,
    action: str,
) -> None:
    if action not in {"disable", "stop", "enable", "start", "daemon-reload"}:
        raise OperationStateError("Task 7 systemctl action is invalid")
    argv = (
        (_SYSTEMCTL_EXECUTABLE, action)
        if action == "daemon-reload"
        else (
            _SYSTEMCTL_EXECUTABLE,
            action,
            "degen-prod-db-backup.timer",
        )
    )
    _checked_command(
        context,
        argv,
        (),
        f"systemctl {action}",
        include_safe_stderr=True,
    )


def _quiesce_backup_timer(
    context: OperationsContext,
    prior_runtime: dict[str, object],
    *,
    before_action: Callable[[str], None] | None = None,
) -> None:
    if prior_runtime["timer_enabled"]:
        if before_action is not None:
            before_action("timer_disable")
        _task7_systemctl(context, "disable")
        _atomic_event_hook("task7_after_timer_disable")
    if before_action is not None:
        before_action("timer_stop")
    _task7_systemctl(context, "stop")
    _atomic_event_hook("task7_after_timer_stop")
    if before_action is not None:
        before_action("quiesce_service_check")
    _require_backup_service_inactive(context)
    if before_action is not None:
        before_action("quiesce_runtime_readback")
    observed = _capture_prior_runtime(context)
    expected = copy.deepcopy(prior_runtime)
    expected["timer_enabled"] = False
    expected["timer_active"] = False
    if observed != expected:
        raise OperationStateError(
            "backup timer did not reach the exact quiesced runtime state"
        )


def _restore_backup_timer(
    context: OperationsContext,
    prior_runtime: dict[str, object],
    *,
    before_action: Callable[[str], None] | None = None,
) -> None:
    if prior_runtime["timer_enabled"]:
        if before_action is not None:
            before_action("timer_enable")
        _task7_systemctl(context, "enable")
        _atomic_event_hook("task7_after_timer_enable")
    if prior_runtime["timer_active"]:
        if before_action is not None:
            before_action("timer_start")
        _task7_systemctl(context, "start")
        _atomic_event_hook("task7_after_timer_start")
    if before_action is not None:
        before_action("restore_runtime_readback")
    if _capture_prior_runtime(context) != prior_runtime:
        raise OperationStateError(
            "backup timer did not return to the exact prior runtime state"
        )
    _atomic_event_hook("task7_after_timer_restore")


def _task7_write_state(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    pre_replace_validator: PreReplaceValidator | None = None,
    force_checkpoint: bool = False,
) -> None:
    _atomic_write_operation_state_internal(
        context.paths.state_file,
        state,
        effective_uid=context.effective_uid,
        pre_replace_validator=pre_replace_validator,
        operation_directory_binding=binding,
        operation_lock_held=True,
        force_checkpoint=force_checkpoint,
    )


def _task7_load_state(context: OperationsContext) -> dict[str, object]:
    state = load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    validate_operation_state_for_context(state, context)
    return state


def _task7_recovery_command(context: OperationsContext) -> str:
    helper = context.paths.source_dir / "deploy/linux/degen-prod-db-backup-ops.py"
    return f"{helper} recover --operation-dir {context.paths.operation_dir}"


def _task7_append_history(
    state: dict[str, object],
    phase: str,
    epoch: int,
    label: str,
    payload: object,
) -> None:
    state["phase"] = phase
    history = state["phase_history"]
    assert isinstance(history, list)
    history.append(
        {
            "phase": phase,
            "epoch": epoch,
            "evidence_sha256": _task7_evidence_sha256(label, payload),
        }
    )


def _task8_force_checkpoint(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    action: str,
) -> None:
    _require_string(action, "Task 8 checkpoint action", nonempty=True)
    _task7_write_state(
        context,
        binding,
        state,
        force_checkpoint=True,
    )


def _task8_advance_guard(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    field: str,
) -> dict[str, object]:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict):
        raise OperationStateError("Task 8 guard progress requires an active transaction")
    guard = transaction.get("guard")
    if not isinstance(guard, dict) or field not in guard:
        raise OperationStateError("Task 8 guard progress field is invalid")
    if guard[field] is not False:
        raise OperationStateError("Task 8 guard progress may advance only once")
    updated = copy.deepcopy(state)
    updated_transaction = updated["active_transaction"]
    assert isinstance(updated_transaction, dict)
    updated_guard = updated_transaction["guard"]
    assert isinstance(updated_guard, dict)
    updated_guard[field] = True
    validate_operation_state(updated, context.paths.operation_dir, state)
    _task7_write_state(context, binding, updated)
    state.clear()
    state.update(updated)
    return state


def _task8_enter_guarded_transaction(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    kind: str,
    runtime_baseline: dict[str, object],
    policy_environment_sha256: str | None,
    probe: dict[str, object] | None,
    pre_replace_validator: PreReplaceValidator | None = None,
) -> dict[str, object]:
    if kind not in _TRANSACTION_KINDS:
        raise OperationStateError("Task 8 transaction kind is invalid")
    prior_phase = _TASK8_PRIOR_PHASES[kind]
    entry_phase = _TASK8_ENTRY_PHASES[kind]
    if state.get("phase") != prior_phase or state.get("active_transaction") is not None:
        raise OperationStateError(
            "Task 8 guarded transaction does not start from its fixed stable phase"
        )
    _validate_prior_runtime(
        runtime_baseline,
        "active_transaction.runtime_baseline",
    )
    epoch = _task7_state_epoch(context, state)
    updated = copy.deepcopy(state)
    updated["active_transaction"] = {
        "kind": kind,
        "prior_stable_phase": prior_phase,
        "prior_timer_enabled": runtime_baseline["timer_enabled"],
        "prior_timer_active": runtime_baseline["timer_active"],
        "runtime_baseline": copy.deepcopy(runtime_baseline),
        "guard": {
            "timer_stopped": False,
            "service_inactive_verified": False,
            "legacy_lock_acquired": False,
            "runtime_lock_acquired": False,
            "locks_released": False,
            "timer_restored": False,
        },
        "started_epoch": epoch,
        "policy_environment_sha256": policy_environment_sha256,
        "probe": copy.deepcopy(probe),
    }
    _task7_append_history(
        updated,
        entry_phase,
        epoch,
        "guard-entry",
        {
            "kind": kind,
            "operation_id": context.operation_id,
            "prior_stable_phase": prior_phase,
            "runtime_baseline": runtime_baseline,
            "policy_environment_sha256": policy_environment_sha256,
            "probe": probe,
        },
    )
    if kind == "probe":
        transaction = updated["active_transaction"]
        assert isinstance(transaction, dict)
        probe_transaction = transaction["probe"]
        if not isinstance(probe_transaction, dict):
            raise OperationStateError("probe transaction identity is required")
        history = updated["phase_history"]
        assert isinstance(history, list) and history
        entry = history[-1]
        assert isinstance(entry, dict)
        entry["evidence_sha256"] = _task8_probe_entry_evidence_sha256(
            updated,
            entry,
            str(probe_transaction["prefix"]),
        )
    elif kind == "dry_run":
        history = updated["phase_history"]
        assert isinstance(history, list) and history
        entry = history[-1]
        assert isinstance(entry, dict)
        entry["evidence_sha256"] = _task8_dry_run_entry_evidence_sha256(
            updated,
            entry,
        )
    elif kind == "policy":
        history = updated["phase_history"]
        assert isinstance(history, list) and history
        entry = history[-1]
        assert isinstance(entry, dict)
        entry["evidence_sha256"] = _task8_policy_entry_evidence_sha256(
            updated,
            entry,
        )
    elif kind == "observe":
        history = updated["phase_history"]
        assert isinstance(history, list) and history
        entry = history[-1]
        assert isinstance(entry, dict)
        entry["evidence_sha256"] = _task8_observation_entry_evidence_sha256(
            updated,
            entry,
        )
    validate_operation_state(updated, context.paths.operation_dir, state)
    _task7_write_state(
        context,
        binding,
        updated,
        pre_replace_validator=pre_replace_validator,
    )
    state.clear()
    state.update(updated)
    return state


def _task8_acquire_guard(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    revalidate: Callable[[], None] | None = None,
) -> MigrationLocks:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict):
        raise OperationStateError("Task 8 guard acquisition requires an active transaction")
    runtime_baseline = transaction.get("runtime_baseline")
    _validate_prior_runtime(
        runtime_baseline,
        "active_transaction.runtime_baseline",
    )
    assert isinstance(runtime_baseline, dict)
    guard = transaction.get("guard")
    if not isinstance(guard, dict) or any(guard.values()):
        raise OperationStateError("Task 8 guard acquisition requires zero guard progress")

    def checkpoint(action: str) -> None:
        if revalidate is not None:
            revalidate()
        _task8_force_checkpoint(context, binding, state, action)
        if revalidate is not None:
            revalidate()

    _quiesce_backup_timer(
        context,
        runtime_baseline,
        before_action=checkpoint,
    )
    _task8_advance_guard(context, binding, state, "timer_stopped")
    if revalidate is not None:
        revalidate()
    _task8_advance_guard(context, binding, state, "service_inactive_verified")
    if revalidate is not None:
        revalidate()

    lock_milestones = {
        "legacy_lock_acquire": "legacy_lock_acquired",
        "runtime_lock_acquire": "runtime_lock_acquired",
    }

    def acquired(action: str, descriptor: int) -> None:
        del descriptor
        try:
            milestone = lock_milestones[action]
        except KeyError as exc:
            raise OperationStateError("Task 8 lock acquisition action is invalid") from exc
        _task8_advance_guard(context, binding, state, milestone)
        if revalidate is not None:
            revalidate()

    return acquire_migration_locks(
        context,
        before_action=checkpoint,
        after_action=acquired,
    )


def _task8_release_guard_locks(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    locks: MigrationLocks,
    *,
    revalidate: Callable[[], None] | None = None,
) -> None:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict):
        raise OperationStateError("Task 8 lock release requires an active transaction")
    guard = transaction.get("guard")
    if (
        not isinstance(guard, dict)
        or guard.get("runtime_lock_acquired") is not True
        or guard.get("locks_released") is not False
    ):
        raise OperationStateError("Task 8 guard locks are not ready for release")

    def checkpoint(action: str) -> None:
        if revalidate is not None:
            revalidate()
        _task8_force_checkpoint(context, binding, state, action)
        if revalidate is not None:
            revalidate()

    issues = _release_migration_locks(
        locks,
        before_action=checkpoint,
    )
    if issues:
        raise _Task8GuardReleaseError(issues)
    _task8_advance_guard(context, binding, state, "locks_released")
    if revalidate is not None:
        revalidate()


def _task8_restore_guard_timer(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    revalidate: Callable[[], None] | None = None,
) -> None:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict):
        raise OperationStateError("Task 8 timer restore requires an active transaction")
    guard = transaction.get("guard")
    if (
        not isinstance(guard, dict)
        or guard.get("locks_released") is not True
        or guard.get("timer_restored") is not False
    ):
        raise OperationStateError(
            "Task 8 guard lock release is incomplete; timer restore is blocked"
        )
    runtime_baseline = transaction.get("runtime_baseline")
    _validate_prior_runtime(
        runtime_baseline,
        "active_transaction.runtime_baseline",
    )
    assert isinstance(runtime_baseline, dict)

    def checkpoint(action: str) -> None:
        if revalidate is not None:
            revalidate()
        _task8_force_checkpoint(context, binding, state, action)
        if revalidate is not None:
            revalidate()

    checkpoint("pre_restore_service_check")
    _require_backup_service_inactive(context)
    _restore_backup_timer(
        context,
        runtime_baseline,
        before_action=checkpoint,
    )
    _task8_advance_guard(context, binding, state, "timer_restored")
    if revalidate is not None:
        revalidate()


def _task7_install_bytes(material: _VerifiedTransactionMaterial) -> dict[str, bytes]:
    expected: dict[str, bytes] = {}
    for source, target in _SOURCE_TO_TARGET:
        expected[target] = material.stage_proof.expected_bytes[f"reviewed/{source}"]
    expected[_TARGET_ORDER[-1]] = material.stage_proof.expected_bytes[
        "host/etc/degen/prod-db-backup.env"
    ]
    if tuple(expected) != _TARGET_ORDER:
        raise OperationStateError("Task 7 staged target order is incomplete")
    return expected


def _task7_install_mode(target: str) -> int:
    if target in _TARGET_ORDER[:4]:
        return 0o755
    if target in _TARGET_ORDER[4:7]:
        return 0o644
    if target == _TARGET_ORDER[-1]:
        return 0o600
    raise OperationStateError("Task 7 target is outside the fixed order")


def _task7_runtime_directory_preexists(context: OperationsContext) -> bool:
    if os.name != "posix" or not _descriptor_primitives_available():
        path = _host_path(context, "/run/degen-prod-db-backup")
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return False
        _validate_task7_directory(
            metadata,
            context,
            "runtime directory",
            exact_mode=0o700,
        )
        return True
    proof = _open_task7_directory(context, "/run", exact_mode=0o755)
    try:
        try:
            metadata = os.stat(
                "degen-prod-db-backup",
                dir_fd=proof.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return False
        _validate_task7_directory(
            metadata,
            context,
            "runtime directory",
            exact_mode=0o700,
        )
        return True
    finally:
        _close_task7_directory_proof(proof)


def _task7_acquire_migration_locks(
    context: OperationsContext,
    *,
    runtime_directory_created: bool,
) -> MigrationLocks:
    if os.name == "posix" and _descriptor_primitives_available():
        return _acquire_migration_locks(
            context,
            require_runtime_create=runtime_directory_created,
        )
    if runtime_directory_created and _task7_runtime_directory_preexists(context):
        raise OperationStateError(
            "runtime directory appeared after the held absence proof"
        )
    if runtime_directory_created:
        runtime_path = _host_path(context, "/run/degen-prod-db-backup")
        try:
            runtime_path.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise OperationStateError(
                "runtime directory exclusive creation raced"
            ) from exc
        if os.name == "posix":
            runtime_path.chmod(0o700)
        _atomic_event_hook("task7_after_runtime_directory_create")
    return acquire_migration_locks(context)


@contextlib.contextmanager
def _open_task7_live_target_proofs(context: OperationsContext):
    proofs: dict[str, _SnapshotTargetProof] = {}
    with contextlib.ExitStack() as stack:
        try:
            for target in _TARGET_ORDER:
                parent = str(PurePosixPath(target).parent)
                directory = stack.enter_context(_open_host_directory(context, parent))
                proofs[target] = _capture_snapshot_target(directory, target)
            yield proofs
        finally:
            for proof in proofs.values():
                _close_snapshot_target_proof(proof)


def _task7_revalidate_live_target_proofs(
    proofs: dict[str, _SnapshotTargetProof],
) -> None:
    if tuple(proofs) != _TARGET_ORDER:
        raise OperationStateError("Task 7 live target proof order is incomplete")
    for proof in proofs.values():
        _revalidate_snapshot_target_proof(proof)


def _task8_validate_live_installed_target_proofs(
    state: dict[str, object],
    expected_bytes: dict[str, bytes],
    proofs: dict[str, _SnapshotTargetProof],
) -> None:
    if tuple(expected_bytes) != _TARGET_ORDER or tuple(proofs) != _TARGET_ORDER:
        raise OperationStateError("live installed target proof order is incomplete")
    install = state.get("install")
    if not isinstance(install, dict):
        raise OperationStateError("live installed target receipt is missing")
    installed_hashes = install.get("installed_hashes")
    if not isinstance(installed_hashes, dict) or tuple(installed_hashes) != _TARGET_ORDER:
        raise OperationStateError("live installed target hash receipt is incomplete")
    _task7_revalidate_live_target_proofs(proofs)
    for target in _TARGET_ORDER:
        proof = proofs[target]
        expected = expected_bytes[target]
        expected_hash = hashlib.sha256(expected).hexdigest()
        receipt = _snapshot_target_receipt(proof)
        if proof.contents != expected or installed_hashes[target] != expected_hash:
            raise OperationStateError(
                f"live installed target differs from verified provenance: {target}"
            )
        if os.name == "posix" and receipt["mode"] != _task7_install_mode(target):
            raise OperationStateError(
                f"live installed target mode differs from verified provenance: {target}"
            )
    _task7_revalidate_live_target_proofs(proofs)


@contextlib.contextmanager
def _open_task8_policy_immutable_target_proofs(context: OperationsContext):
    proofs: dict[str, _SnapshotTargetProof] = {}
    with contextlib.ExitStack() as stack:
        try:
            for target in _TARGET_ORDER[:-1]:
                parent = str(PurePosixPath(target).parent)
                directory = stack.enter_context(_open_host_directory(context, parent))
                proofs[target] = _capture_snapshot_target(directory, target)
            yield proofs
        finally:
            for proof in proofs.values():
                _close_snapshot_target_proof(proof)


def _task8_validate_live_installed_immutable_target_proofs(
    state: dict[str, object],
    expected_bytes: dict[str, bytes],
    proofs: dict[str, _SnapshotTargetProof],
) -> None:
    immutable_targets = _TARGET_ORDER[:-1]
    if tuple(expected_bytes) != _TARGET_ORDER or tuple(proofs) != immutable_targets:
        raise OperationStateError(
            "live immutable installed target proof order is incomplete"
        )
    install = state.get("install")
    if not isinstance(install, dict):
        raise OperationStateError("live installed target receipt is missing")
    installed_hashes = install.get("installed_hashes")
    if not isinstance(installed_hashes, dict) or tuple(installed_hashes) != _TARGET_ORDER:
        raise OperationStateError("live installed target hash receipt is incomplete")
    for target in immutable_targets:
        proof = proofs[target]
        _revalidate_snapshot_target_proof(proof)
        expected = expected_bytes[target]
        receipt = _snapshot_target_receipt(proof)
        if (
            proof.contents != expected
            or installed_hashes[target] != hashlib.sha256(expected).hexdigest()
        ):
            raise OperationStateError(
                f"live immutable target differs from verified provenance: {target}"
            )
        if os.name == "posix" and receipt["mode"] != _task7_install_mode(target):
            raise OperationStateError(
                f"live immutable target mode differs from verified provenance: {target}"
            )
    for proof in proofs.values():
        _revalidate_snapshot_target_proof(proof)


def _task7_require_snapshot_live_targets(
    state: dict[str, object],
    proofs: dict[str, _SnapshotTargetProof],
) -> None:
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    for target, proof in proofs.items():
        if _snapshot_target_receipt(proof) != targets[target]:
            raise OperationStateError(
                f"live target changed from snapshot before installation: {target}"
            )


def _task7_temp_name(context: OperationsContext, target: str, kind: str) -> str:
    if kind not in {"install", "recovery"}:
        raise OperationStateError("Task 7 temporary kind is invalid")
    return f".{PurePosixPath(target).name}.{context.operation_id}.{kind}.tmp"


def _task8_policy_environment_temp_name(
    context: OperationsContext,
    action: str,
) -> str:
    if action not in {"enable", "restore"}:
        raise OperationStateError("policy environment action is invalid")
    return (
        f".{PurePosixPath(_TARGET_ORDER[-1]).name}."
        f"{context.operation_id}.policy-{action}.tmp"
    )


def _task7_named_metadata(
    directory: _HostDirectoryProof,
    name: str,
) -> os.stat_result:
    if directory.descriptor is None:
        return (directory.path / name).lstat()
    return os.stat(name, dir_fd=directory.descriptor, follow_symlinks=False)


def _task7_refresh_mutable_directory_proof(
    proof: _HostDirectoryProof,
) -> None:
    """Rebind expected directory timestamps after our own child mutation."""
    _revalidate_host_root_proof(proof.host_root_proof)
    if not proof.descriptors:
        current = proof.context.host_root
        refreshed: list[os.stat_result] = []
        for index, expected in enumerate(proof.metadata):
            named = current.lstat()
            _validate_host_directory_metadata(
                named,
                proof.context.effective_uid,
                "Task 7 target parent",
            )
            if not _same_identity(named, expected):
                raise OperationStateError("Task 7 target parent binding changed")
            refreshed.append(named)
            if index < len(proof.names):
                current = current / proof.names[index]
        proof.metadata[:] = refreshed
        return
    refreshed = []
    for index, descriptor in enumerate(proof.descriptors):
        opened = os.fstat(descriptor)
        _validate_host_directory_metadata(
            opened,
            proof.context.effective_uid,
            "Task 7 target parent",
        )
        if not _same_identity(opened, proof.metadata[index]):
            raise OperationStateError("Task 7 target parent identity changed")
        if index:
            named = os.stat(
                proof.names[index - 1],
                dir_fd=proof.descriptors[index - 1],
                follow_symlinks=False,
            )
            if not _same_identity(opened, named):
                raise OperationStateError("Task 7 target parent binding changed")
        refreshed.append(opened)
    proof.metadata[:] = refreshed


def _task7_require_no_install_temporaries(context: OperationsContext) -> None:
    for target in _TARGET_ORDER:
        parent = str(PurePosixPath(target).parent)
        with _open_host_directory(context, parent) as directory:
            name = _task7_temp_name(context, target, "install")
            try:
                _task7_named_metadata(directory, name)
            except FileNotFoundError:
                continue
            raise OperationStateError(
                f"preexisting operation target temporary exists: {name}"
            )


def _task7_install_cursor(
    state: dict[str, object],
    expected_bytes: dict[str, bytes],
    index: int,
) -> tuple[str | None, str | None, str | None]:
    if index == len(_TARGET_ORDER):
        return None, None, None
    target = _TARGET_ORDER[index]
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    snapshot_target = targets[target]
    assert isinstance(snapshot_target, dict)
    previous = snapshot_target["sha256"] if snapshot_target["present"] else None
    intended = hashlib.sha256(expected_bytes[target]).hexdigest()
    return target, previous, intended


def _task7_build_installing_state(
    context: OperationsContext,
    previous: dict[str, object],
    expected_bytes: dict[str, bytes],
    *,
    runtime_directory_created: bool,
) -> dict[str, object]:
    epoch = _task7_state_epoch(context, previous)
    target, previous_hash, intended_hash = _task7_install_cursor(
        previous,
        expected_bytes,
        0,
    )
    state = copy.deepcopy(previous)
    state["install"] = {
        "next_target_index": 0,
        "current_target": target,
        "previous_sha256": previous_hash,
        "intended_sha256": intended_hash,
        "installed_hashes": {},
        "started_epoch": epoch,
        "completed_epoch": None,
        "runtime_directory_created": runtime_directory_created,
        "validated_epoch": None,
        "validation_evidence_sha256": None,
    }
    _task7_append_history(
        state,
        "installing",
        epoch,
        "install-start",
        {
            "operation_id": context.operation_id,
            "runtime_directory_created": runtime_directory_created,
            "target_order": list(_TARGET_ORDER),
        },
    )
    validate_operation_state(state, context.paths.operation_dir, previous)
    validate_operation_state_for_context(state, context)
    return state


def _task7_advance_install_cursor(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    expected_bytes: dict[str, bytes],
    next_index: int,
) -> dict[str, object]:
    updated = copy.deepcopy(state)
    install = updated["install"]
    assert isinstance(install, dict)
    target, previous_hash, intended_hash = _task7_install_cursor(
        updated,
        expected_bytes,
        next_index,
    )
    install.update(
        {
            "next_target_index": next_index,
            "current_target": target,
            "previous_sha256": previous_hash,
            "intended_sha256": intended_hash,
        }
    )
    _task7_write_state(context, binding, updated)
    _atomic_event_hook("task7_after_cursor_state", index=next_index)
    return updated


def _task7_validate_temp_metadata(
    metadata: os.stat_result,
    context: OperationsContext,
    mode: int,
) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise OperationStateError("operation target temporary is not regular")
    if metadata.st_uid != _task7_expected_owner(context):
        raise OperationStateError("operation target temporary owner is unsafe")
    if metadata.st_nlink != 1:
        raise OperationStateError("operation target temporary link count is unsafe")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != mode:
        raise OperationStateError("operation target temporary mode is unsafe")


def _task7_atomic_install_target(
    context: OperationsContext,
    target: str,
    contents: bytes,
    mode: int,
    index: int,
    expected_previous: dict[str, object],
) -> None:
    parent = str(PurePosixPath(target).parent)
    name = PurePosixPath(target).name
    temp_name = _task7_temp_name(context, target, "install")
    descriptor: int | None = None
    owned_temp_metadata: os.stat_result | None = None
    temp_metadata: os.stat_result | None = None
    replaced = False
    with _open_host_directory(context, parent) as directory:
        target_path = directory.path / name
        temp_path = directory.path / temp_name
        proof = _capture_snapshot_target(directory, target)
        try:
            if _snapshot_target_receipt(proof) != expected_previous:
                raise OperationStateError(
                    f"live target changed before installation replace: {target}"
                )
        finally:
            _close_snapshot_target_proof(proof)
        try:
            try:
                _task7_named_metadata(directory, temp_name)
            except FileNotFoundError:
                pass
            else:
                raise OperationStateError(
                    f"operation target temporary already exists: {temp_name}"
                )
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
            descriptor = (
                os.open(temp_path, flags, mode)
                if directory.descriptor is None
                else os.open(temp_name, flags, mode, dir_fd=directory.descriptor)
            )
            owned_temp_metadata = os.fstat(descriptor)
            if os.name == "posix":
                os.fchmod(descriptor, mode)
                expected_gid = 0 if context.host_root == _PRODUCTION_HOST_ROOT else os.getegid()
                os.fchown(descriptor, _task7_expected_owner(context), expected_gid)
            _write_all(descriptor, contents)
            os.fsync(descriptor)
            temp_metadata = os.fstat(descriptor)
            _task7_validate_temp_metadata(temp_metadata, context, mode)
            os.close(descriptor)
            descriptor = None
            _task7_refresh_mutable_directory_proof(directory)
            _atomic_event_hook(
                "task7_after_staged_file_fsync",
                index=index,
                target=target,
                temp_path=str(temp_path),
            )
            current_temp = _task7_named_metadata(directory, temp_name)
            if (
                not _same_identity(current_temp, temp_metadata)
                or _stage_stable_metadata(current_temp)
                != _stage_stable_metadata(temp_metadata)
            ):
                raise OperationStateError("operation target temporary binding changed")
            proof = _capture_snapshot_target(directory, target)
            try:
                if _snapshot_target_receipt(proof) != expected_previous:
                    raise OperationStateError(
                        f"live target changed before installation replace: {target}"
                    )
            finally:
                _close_snapshot_target_proof(proof)
            _atomic_event_hook(
                "task7_before_target_replace",
                index=index,
                target=target,
            )
            if directory.descriptor is None:
                os.replace(temp_path, target_path)
            else:
                os.replace(
                    temp_name,
                    name,
                    src_dir_fd=directory.descriptor,
                    dst_dir_fd=directory.descriptor,
                )
            replaced = True
            _task7_refresh_mutable_directory_proof(directory)
            _atomic_event_hook(
                "task7_after_target_replace",
                index=index,
                target=target,
            )
            if directory.descriptor is not None:
                os.fsync(directory.descriptor)
            elif os.name == "posix":
                _fsync_parent_directory(target_path)
            _atomic_event_hook(
                "task7_after_target_parent_fsync",
                index=index,
                target=target,
            )
            _task7_refresh_mutable_directory_proof(directory)
            installed = _capture_snapshot_target(directory, target)
            try:
                receipt = _snapshot_target_receipt(installed)
                if (
                    receipt["sha256"] != hashlib.sha256(contents).hexdigest()
                    or (os.name == "posix" and receipt["mode"] != mode)
                ):
                    raise OperationStateError(
                        f"installed target validation failed: {target}"
                    )
            finally:
                _close_snapshot_target_proof(installed)
        except Exception as target_error:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                descriptor = None
            if not replaced and owned_temp_metadata is not None:
                try:
                    current_temp = _task7_named_metadata(directory, temp_name)
                    if _same_identity(current_temp, owned_temp_metadata):
                        if directory.descriptor is None:
                            temp_path.unlink()
                        else:
                            os.unlink(temp_name, dir_fd=directory.descriptor)
                        _task7_refresh_mutable_directory_proof(directory)
                        if directory.descriptor is not None:
                            os.fsync(directory.descriptor)
                        elif os.name == "posix":
                            _fsync_parent_directory(temp_path)
                except OSError:
                    pass
            try:
                _task7_refresh_mutable_directory_proof(directory)
            except Exception:
                pass
            if isinstance(target_error, OSError):
                raise OperationStateError(
                    sanitize_error_text(target_error)
                ) from target_error
            raise
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def _task7_verify_installed_targets(
    context: OperationsContext,
    expected_bytes: dict[str, bytes],
) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for target in _TARGET_ORDER:
        parent = str(PurePosixPath(target).parent)
        with _open_host_directory(context, parent) as directory:
            proof = _capture_snapshot_target(directory, target)
            try:
                if proof.contents != expected_bytes[target]:
                    raise OperationStateError(
                        f"installed target bytes differ from verified stage: {target}"
                    )
                receipt = _snapshot_target_receipt(proof)
                if (
                    os.name == "posix"
                    and receipt["mode"] != _task7_install_mode(target)
                ):
                    raise OperationStateError(
                        f"installed target mode differs from verified stage: {target}"
                    )
                hashes[target] = hashlib.sha256(expected_bytes[target]).hexdigest()
            finally:
                _close_snapshot_target_proof(proof)
    return hashes


def _task7_require_protected_pid_parity(
    context: OperationsContext,
    prior_runtime: dict[str, object],
) -> None:
    current = _capture_prior_runtime(context)
    if current["pids"] != prior_runtime["pids"]:
        raise OperationStateError("protected process PID identity changed")


def _task7_release_locks(
    locks: MigrationLocks,
) -> list[_Task7LockReleaseIssue]:
    errors: list[_Task7LockReleaseIssue] = []
    crashes: list[BaseException] = []
    try:
        _atomic_event_hook("task7_before_lock_release")
    except BaseException as exc:
        if isinstance(exc, Exception):
            errors.append(
                _Task7LockReleaseIssue("release_migration_locks", exc, False)
            )
        else:
            crashes.append(exc)
    try:
        for issue in _release_migration_locks(locks):
            if isinstance(issue, _Task7LockReleaseIssue):
                errors.append(issue)
                continue
            stage, error = issue
            errors.append(
                _Task7LockReleaseIssue(
                    stage,
                    error,
                    isinstance(error, OSError),
                )
            )
    except BaseException as exc:
        crashes.append(exc)
    try:
        _atomic_event_hook("task7_after_lock_release")
    except BaseException as exc:
        if isinstance(exc, Exception):
            errors.append(
                _Task7LockReleaseIssue("release_migration_locks", exc, False)
            )
        else:
            crashes.append(exc)
    if crashes:
        raise crashes[0]
    return errors


def _task7_emergency_close_locks(locks: MigrationLocks) -> None:
    try:
        import fcntl
    except ImportError:
        fcntl = None  # type: ignore[assignment]
    for descriptor in (locks.runtime_fd, locks.legacy_fd):
        if fcntl is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
        try:
            os.close(descriptor)
        except OSError:
            pass


def _task7_capture_file_audit(
    context: OperationsContext,
    logical_path: str,
) -> dict[str, object]:
    with _open_host_file(
        context,
        logical_path,
        "Task 7 audit file",
        maximum_size=_MAX_SNAPSHOT_FILE_BYTES,
        exact_mode=0o600,
    ) as proof:
        raw = _read_host_file(proof, _MAX_SNAPSHOT_FILE_BYTES)
        _revalidate_host_file_proof(proof)
        metadata = os.fstat(proof.descriptor)
        return {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "inode": metadata.st_ino,
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
            "mode": stat.S_IMODE(metadata.st_mode) if os.name == "posix" else 0o600,
            "size": metadata.st_size,
            "mtime_ns": metadata.st_mtime_ns,
        }


def _task7_run_preflight_with_rclone_audit(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    runtime_lock_fd: int,
) -> dict[str, object]:
    effective = state["effective_config"]
    assert isinstance(effective, dict)
    rclone_path = str(effective["RCLONE_CONFIG"])
    if rclone_path != _RCLONE_CONFIG_PATH:
        raise OperationStateError("Task 7 rclone audit path is not fixed")
    before = _task7_capture_file_audit(context, rclone_path)
    pending_group: dict[str, object] = {
        "group_id": "install",
        "purpose": "credential-refresh-audit",
        "before": before,
        "after": None,
        "evidence_sha256": None,
    }
    pending_state = copy.deepcopy(state)
    pending_groups = pending_state["rclone_evidence_groups"]
    assert isinstance(pending_groups, list)
    pending_groups.append(pending_group)
    _task7_write_required_receipt_state(
        context,
        binding,
        pending_state,
        "rclone preflight intent",
    )
    preflight_error: BaseException | None = None
    try:
        executable = _host_path(context, _TARGET_ORDER[0])
        _checked_command(
            context,
            (
                str(executable),
                "preflight",
                "--lock-fd",
                str(runtime_lock_fd),
            ),
            (runtime_lock_fd,),
            "installed backup preflight",
        )
    except BaseException as exc:
        preflight_error = exc
    try:
        after = _task7_capture_file_audit(context, rclone_path)
    except BaseException as audit_error:
        if not isinstance(audit_error, Exception):
            raise
        if preflight_error is not None:
            if not isinstance(preflight_error, Exception):
                raise preflight_error
            raise _Task7RcloneAuditIncompleteError(
                preflight_error,
                audit_error,
            ) from audit_error
        raise _Task7RcloneAuditIncompleteError(
            audit_error,
            None,
        ) from audit_error
    group_without_evidence: dict[str, object] = {
        "group_id": "install",
        "purpose": "credential-refresh-audit",
        "before": before,
        "after": after,
    }
    group = {
        **group_without_evidence,
        "evidence_sha256": _task7_evidence_sha256(
            "rclone-audit",
            group_without_evidence,
        ),
    }
    updated = copy.deepcopy(pending_state)
    groups = updated["rclone_evidence_groups"]
    assert isinstance(groups, list)
    groups[-1] = group
    try:
        _task7_write_required_receipt_state(
            context,
            binding,
            updated,
            "rclone before/after audit",
        )
    except Exception as exc:
        raise _Task7RcloneEvidencePersistenceError(
            group,
            exc,
            preflight_error,
        ) from exc
    if preflight_error is not None:
        raise preflight_error
    return updated


def _task7_finalize_pending_rclone_audit(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
) -> dict[str, object]:
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    if not groups:
        return state
    pending = groups[-1]
    assert isinstance(pending, dict)
    if pending["after"] is not None:
        return state
    effective = state["effective_config"]
    assert isinstance(effective, dict)
    rclone_path = str(effective["RCLONE_CONFIG"])
    if rclone_path != _RCLONE_CONFIG_PATH:
        raise OperationStateError("pending rclone audit path is not fixed")
    after = _task7_capture_file_audit(context, rclone_path)
    payload: dict[str, object] = {
        "group_id": pending["group_id"],
        "purpose": pending["purpose"],
        "before": pending["before"],
        "after": after,
    }
    if str(pending["group_id"]).startswith("task8:"):
        payload["outcome"] = "indeterminate"
    if str(pending["group_id"]).startswith(
        ("task8:dry_run:", "task8:policy:", "task8:observe:")
    ):
        payload["result_sha256"] = None
    final = {
        **payload,
        "evidence_sha256": None,
    }
    final["evidence_sha256"] = (
        _task8_group_evidence_sha256(final)
        if str(final["group_id"]).startswith("task8:")
        else _task7_evidence_sha256("rclone-audit", payload)
    )
    updated = copy.deepcopy(state)
    updated_groups = updated["rclone_evidence_groups"]
    assert isinstance(updated_groups, list)
    updated_groups[-1] = final
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "pending rclone before/after audit",
    )
    return _task7_load_state(context)


def _task7_record_install_validation(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    installed_hashes: dict[str, str],
    prior_runtime: dict[str, object],
) -> dict[str, object]:
    epoch = _task7_state_epoch(context, state)
    updated = copy.deepcopy(state)
    install = updated["install"]
    assert isinstance(install, dict)
    evidence = {
        "installed_hashes": installed_hashes,
        "protected_pids": prior_runtime["pids"],
        "rclone_group_count": len(updated["rclone_evidence_groups"]),
    }
    install["validated_epoch"] = epoch
    install["validation_evidence_sha256"] = _task7_evidence_sha256(
        "install-validation",
        evidence,
    )
    _task7_write_state(context, binding, updated)
    _atomic_event_hook("task7_after_validation")
    return updated


def _task7_complete_install(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    installed_hashes: dict[str, str],
) -> dict[str, object]:
    epoch = _task7_state_epoch(context, state)
    updated = copy.deepcopy(state)
    install = updated["install"]
    assert isinstance(install, dict)
    install["installed_hashes"] = dict(installed_hashes)
    install["completed_epoch"] = epoch
    _task7_append_history(
        updated,
        "installed",
        epoch,
        "install-complete",
        {
            "installed_hashes": installed_hashes,
            "validation_evidence_sha256": install["validation_evidence_sha256"],
        },
    )
    _task7_write_state(context, binding, updated)
    return updated


def _task7_write_required_receipt_state(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    expected: dict[str, object],
    label: str,
    *,
    pre_replace_validator: PreReplaceValidator | None = None,
) -> None:
    try:
        _task7_write_state(
            context,
            binding,
            expected,
            pre_replace_validator=pre_replace_validator,
        )
    except Exception as first_error:
        current = _task7_load_state(context)
        if current == expected:
            try:
                _task7_write_state(
                    context,
                    binding,
                    expected,
                    pre_replace_validator=pre_replace_validator,
                    force_checkpoint=True,
                )
            except Exception as checkpoint_error:
                raise OperationStateError(
                    f"{label} visible receipt could not be checkpointed durably"
                ) from checkpoint_error
        else:
            try:
                _task7_write_state(
                    context,
                    binding,
                    expected,
                    pre_replace_validator=pre_replace_validator,
                )
            except Exception as retry_error:
                raise OperationStateError(
                    f"{label} receipt could not be persisted durably"
                ) from retry_error
        current = _task7_load_state(context)
        if current != expected:
            raise OperationStateError(
                f"{label} receipt persistence could not be verified"
            ) from first_error


def _task7_persist_pending_rclone_group(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    group: dict[str, object],
) -> dict[str, object]:
    _reject_residual_secrets(group)
    state = _task7_load_state(context)
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    if not groups:
        updated = copy.deepcopy(state)
        pending_groups = updated["rclone_evidence_groups"]
        assert isinstance(pending_groups, list)
        pending_groups.append(copy.deepcopy(group))
        _task7_write_required_receipt_state(
            context,
            binding,
            updated,
            "pending rclone before/after audit",
        )
        return _task7_load_state(context)
    last = groups[-1]
    assert isinstance(last, dict)
    if (
        last["after"] is None
        and group.get("after") is not None
        and all(
            last[field] == group.get(field)
            for field in ("group_id", "purpose", "before")
        )
    ):
        updated = copy.deepcopy(state)
        updated_groups = updated["rclone_evidence_groups"]
        assert isinstance(updated_groups, list)
        updated_groups[-1] = copy.deepcopy(group)
        _task7_write_required_receipt_state(
            context,
            binding,
            updated,
            "pending rclone before/after audit",
        )
        return _task7_load_state(context)
    if groups[-1] != group:
        raise OperationStateError(
            "durable rclone evidence differs from pending audit"
        )
    return state


def _task7_record_primary_failure(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    error: BaseException,
) -> dict[str, object]:
    state = _task7_load_state(context)
    if state["failure"] is not None:
        return state
    receipt: dict[str, object] = {
        "phase": state["phase"],
        "primary_error": sanitize_error_text(error),
        "epoch": _task7_state_epoch(context, state),
    }
    receipt["evidence_sha256"] = _task7_error_evidence_sha256(
        "primary",
        receipt,
    )
    updated = copy.deepcopy(state)
    updated["failure"] = receipt
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "primary failure",
    )
    return updated


def _task7_append_secondary_error(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    stage: str,
    error: BaseException,
) -> dict[str, object]:
    state = _task7_load_state(context)
    receipt: dict[str, object] = {
        "stage": stage,
        "error": sanitize_error_text(error),
        "epoch": _task7_state_epoch(context, state),
    }
    receipt["evidence_sha256"] = _task7_error_evidence_sha256(
        "secondary",
        receipt,
    )
    updated = copy.deepcopy(state)
    errors = updated["secondary_errors"]
    assert isinstance(errors, list)
    errors.append(receipt)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "secondary failure",
    )
    return updated


def _task7_capture_target_receipt(
    context: OperationsContext,
    target: str,
) -> dict[str, object]:
    parent = str(PurePosixPath(target).parent)
    with _open_host_directory(context, parent) as directory:
        proof = _capture_snapshot_target(directory, target)
        try:
            return _snapshot_target_receipt(proof)
        finally:
            _close_snapshot_target_proof(proof)


def _task7_receipt_matches_installed(
    receipt: dict[str, object],
    target: str,
    installed_hash: str,
) -> bool:
    if not receipt["present"] or receipt["sha256"] != installed_hash:
        return False
    return os.name != "posix" or receipt["mode"] == _task7_install_mode(target)


def _task7_require_recoverable_live_targets(
    context: OperationsContext,
    state: dict[str, object],
) -> dict[str, dict[str, object]]:
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    snapshot_targets = snapshot["targets"]
    assert isinstance(snapshot_targets, dict)
    policy = state["policy"]
    recovery = state["recovery"]
    phase = str(state["phase"])
    stable_phases = {
        "installed",
        "probed",
        "dry_run_recorded",
        "policy_enabled",
        "observed",
    }
    if phase == "installing":
        cursor_receipt = state["install"]
        matrix_kind = "installing"
    elif (
        phase in {"recovering", "manual_rollback", "recovery_required"}
        and isinstance(recovery, dict)
        and recovery["kind"] in {"install", "manual_rollback"}
    ):
        cursor_receipt = recovery
        matrix_kind = (
            "install_recovery"
            if recovery["kind"] == "install"
            else "manual_recovery"
        )
    elif phase in stable_phases:
        cursor_receipt = None
        matrix_kind = "installed"
    elif phase == "rolled_back":
        cursor_receipt = None
        matrix_kind = "snapshot"
    else:
        raise OperationStateError(
            "live target provenance cannot be checked in this operation phase"
        )
    if cursor_receipt is None:
        cursor = 0
    else:
        assert isinstance(cursor_receipt, dict)
        cursor = int(cursor_receipt["next_target_index"])
        if cursor < 0 or cursor > len(_TARGET_ORDER):
            raise OperationStateError("live target cursor is outside the target order")
    install_receipt = state["install"]
    assert isinstance(install_receipt, dict)
    install_cursor = int(install_receipt["next_target_index"])
    receipts: dict[str, dict[str, object]] = {}
    for index, target in enumerate(_TARGET_ORDER):
        receipt = _task7_capture_target_receipt(context, target)
        restore = snapshot_targets[target]
        assert isinstance(restore, dict)
        installed_hash = _target_staged_hash(state, target)
        if target == _TARGET_ORDER[-1] and isinstance(policy, dict):
            installed_hash = str(policy["environment_sha256"])
        is_snapshot = receipt == restore
        is_installed = _task7_receipt_matches_installed(
            receipt,
            target,
            installed_hash,
        )
        if matrix_kind == "installed":
            allowed = is_installed
        elif matrix_kind == "snapshot":
            allowed = is_snapshot
        elif matrix_kind == "installing":
            if cursor == len(_TARGET_ORDER) or index < cursor:
                allowed = is_installed
            elif index == cursor:
                allowed = is_snapshot or is_installed
            else:
                allowed = is_snapshot
        elif matrix_kind == "manual_recovery":
            if cursor == len(_TARGET_ORDER) or index < cursor:
                allowed = is_snapshot
            elif index == cursor:
                allowed = is_snapshot or is_installed
            else:
                allowed = is_installed
        else:
            if install_cursor == len(_TARGET_ORDER) or index < install_cursor:
                allowed_before_recovery = is_installed
            elif index == install_cursor:
                allowed_before_recovery = is_snapshot or is_installed
            else:
                allowed_before_recovery = is_snapshot
            if cursor == len(_TARGET_ORDER) or index < cursor:
                allowed = is_snapshot
            elif index == cursor:
                allowed = is_snapshot or allowed_before_recovery
            else:
                allowed = allowed_before_recovery
        if not allowed:
            policy_label = " policy environment" if (
                target == _TARGET_ORDER[-1] and isinstance(policy, dict)
            ) else ""
            raise OperationStateError(
                f"live{policy_label} target violates cursor-bound provenance: {target}"
            )
        receipts[target] = receipt
    return receipts


def _task7_read_named_regular_file(
    context: OperationsContext,
    directory: _HostDirectoryProof,
    name: str,
    *,
    mode: int,
    maximum_size: int | None = None,
) -> tuple[bytes, os.stat_result]:
    if maximum_size is not None and maximum_size < 0:
        raise OperationStateError("operation target temporary size bound is invalid")
    descriptor: int | None = None
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        named = _task7_named_metadata(directory, name)
        _task7_validate_temp_metadata(named, context, mode)
        if maximum_size is not None and named.st_size > maximum_size:
            raise OperationStateError(
                "operation target temporary size exceeds its exact bound"
            )
        descriptor = (
            os.open(directory.path / name, flags)
            if directory.descriptor is None
            else os.open(name, flags, dir_fd=directory.descriptor)
        )
        opened = os.fstat(descriptor)
        _task7_validate_temp_metadata(opened, context, mode)
        if maximum_size is not None and opened.st_size > maximum_size:
            raise OperationStateError(
                "operation target temporary size exceeds its exact bound"
            )
        if not _same_identity(named, opened):
            raise OperationStateError("operation target temporary binding changed")
        raw = _read_exact_descriptor(
            descriptor,
            opened.st_size,
            "operation target temporary",
        )
        after = os.fstat(descriptor)
        named_after = _task7_named_metadata(directory, name)
        if (
            not _same_identity(opened, after)
            or not _same_identity(after, named_after)
            or _stage_stable_metadata(after) != _stage_stable_metadata(opened)
        ):
            raise OperationStateError("operation target temporary changed while read")
        return raw, after
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _task8_validate_policy_environment_proof(
    context: OperationsContext,
    proof: _SnapshotTargetProof,
    expected: bytes,
    *,
    label: str,
) -> dict[str, object]:
    if type(expected) is not bytes or not expected:
        raise OperationStateError(f"{label} bytes are invalid")
    try:
        _revalidate_snapshot_target_proof(proof)
    except OperationStateError as exc:
        raise OperationStateError(f"{label} changed") from exc
    receipt = _snapshot_target_receipt(proof)
    expected_gid = (
        0 if context.host_root == _PRODUCTION_HOST_ROOT else (
            os.getegid() if os.name == "posix" else int(receipt["gid"] or 0)
        )
    )
    if (
        proof.contents != expected
        or receipt["present"] is not True
        or receipt["sha256"] != hashlib.sha256(expected).hexdigest()
        or receipt["uid"] != _task7_expected_owner(context)
        or (os.name == "posix" and receipt["mode"] != 0o600)
        or (os.name == "posix" and receipt["gid"] != expected_gid)
    ):
        raise OperationStateError(f"{label} changed from exact policy provenance")
    return receipt


def _task8_cleanup_policy_environment_temp(
    context: OperationsContext,
    *,
    expected: bytes,
    action: str,
) -> None:
    if (
        type(expected) is not bytes
        or not expected
        or len(expected) > _MAX_APP_ENV_BYTES
    ):
        raise OperationStateError(
            "policy environment cleanup bytes exceed the exact safe bound"
        )
    target = _TARGET_ORDER[-1]
    parent = str(PurePosixPath(target).parent)
    name = _task8_policy_environment_temp_name(context, action)
    with _open_host_directory(context, parent) as directory:
        try:
            _task7_named_metadata(directory, name)
        except FileNotFoundError:
            if directory.descriptor is not None:
                os.fsync(directory.descriptor)
            elif os.name == "posix":
                _fsync_parent_directory(directory.path / name)
            elif context.host_root == _PRODUCTION_HOST_ROOT:
                raise OperationStateError(
                    "production policy temp cleanup requires directory fsync"
                )
            _atomic_event_hook(
                "task8_policy_after_absent_temp_parent_fsync",
                action=action,
                target=target,
            )
            return
        raw, metadata = _task7_read_named_regular_file(
            context,
            directory,
            name,
            mode=0o600,
            maximum_size=min(len(expected), _MAX_APP_ENV_BYTES),
        )
        if raw != expected[: len(raw)]:
            raise OperationStateError(
                "policy environment temporary has unexpected bytes"
            )
        current = _task7_named_metadata(directory, name)
        if not _same_identity(current, metadata):
            raise OperationStateError(
                "policy environment temporary binding changed"
            )
        if directory.descriptor is None:
            (directory.path / name).unlink()
        else:
            os.unlink(name, dir_fd=directory.descriptor)
        _task7_refresh_mutable_directory_proof(directory)
        if directory.descriptor is not None:
            os.fsync(directory.descriptor)
        elif os.name == "posix":
            _fsync_parent_directory(directory.path / name)


def _task8_durably_adopt_policy_environment(
    context: OperationsContext,
    *,
    expected: bytes,
    revalidate: PreReplaceValidator,
) -> None:
    if (
        type(expected) is not bytes
        or not expected
        or len(expected) > _MAX_APP_ENV_BYTES
    ):
        raise OperationStateError(
            "policy environment adoption bytes exceed the exact safe bound"
        )
    target = _TARGET_ORDER[-1]
    parent = str(PurePosixPath(target).parent)
    revalidate()
    _task8_require_exact_policy_environment(context, expected)
    with _open_host_directory(context, parent) as directory:
        _atomic_event_hook(
            "task8_policy_before_environment_adoption_parent_fsync",
            target=target,
        )
        if directory.descriptor is not None:
            os.fsync(directory.descriptor)
        elif os.name == "posix":
            _fsync_parent_directory(directory.path / PurePosixPath(target).name)
        else:
            raise OperationStateError(
                "policy environment adoption cannot prove parent-directory durability"
            )
        _atomic_event_hook(
            "task8_policy_after_environment_adoption_parent_fsync",
            target=target,
        )
    revalidate()
    _task8_require_exact_policy_environment(context, expected)


def _task8_atomic_replace_policy_environment(
    context: OperationsContext,
    *,
    expected_previous: bytes,
    intended: bytes,
    action: str,
    revalidate: Callable[[], None] | None = None,
) -> dict[str, object]:
    if action not in {"enable", "restore"}:
        raise OperationStateError("policy environment action is invalid")
    if (
        type(expected_previous) is not bytes
        or not expected_previous
        or type(intended) is not bytes
        or not intended
        or expected_previous == intended
    ):
        raise OperationStateError("policy environment replacement bytes are invalid")
    if revalidate is None:
        revalidate = lambda: None

    target = _TARGET_ORDER[-1]
    parent = str(PurePosixPath(target).parent)
    target_name = PurePosixPath(target).name
    temp_name = _task8_policy_environment_temp_name(context, action)
    descriptor: int | None = None
    owned_temp_metadata: os.stat_result | None = None
    temp_metadata: os.stat_result | None = None
    replaced = False
    result: dict[str, object] | None = None

    with _open_host_directory(context, parent) as directory:
        if (
            context.host_root == _PRODUCTION_HOST_ROOT
            and directory.descriptor is None
        ):
            raise OperationStateError(
                "production policy replacement requires POSIX directory-fd semantics"
            )
        target_path = directory.path / target_name
        temp_path = directory.path / temp_name
        original = _capture_snapshot_target(directory, target)
        try:
            _task8_validate_policy_environment_proof(
                context,
                original,
                expected_previous,
                label="policy environment compare-and-swap target",
            )
            revalidate()
            try:
                _task7_named_metadata(directory, temp_name)
            except FileNotFoundError:
                pass
            else:
                raise OperationStateError(
                    f"policy environment temporary already exists: {temp_name}"
                )

            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
            descriptor = (
                os.open(temp_path, flags, 0o600)
                if directory.descriptor is None
                else os.open(
                    temp_name,
                    flags,
                    0o600,
                    dir_fd=directory.descriptor,
                )
            )
            owned_temp_metadata = os.fstat(descriptor)
            if os.name == "posix":
                os.fchmod(descriptor, 0o600)
                expected_gid = (
                    0
                    if context.host_root == _PRODUCTION_HOST_ROOT
                    else os.getegid()
                )
                os.fchown(
                    descriptor,
                    _task7_expected_owner(context),
                    expected_gid,
                )
            _atomic_event_hook(
                "task8_policy_after_environment_temp_open",
                action=action,
                target=target,
                temp_path=str(temp_path),
            )
            _write_all(descriptor, intended)
            os.fsync(descriptor)
            temp_metadata = os.fstat(descriptor)
            _task7_validate_temp_metadata(
                temp_metadata,
                context,
                0o600,
            )
            os.close(descriptor)
            descriptor = None
            _task7_refresh_mutable_directory_proof(directory)
            _atomic_event_hook(
                "task8_policy_after_environment_temp_fsync",
                action=action,
                target=target,
                temp_path=str(temp_path),
            )
            _atomic_event_hook(
                "task8_policy_before_environment_replace",
                action=action,
                target=target,
                temp_path=str(temp_path),
            )
            revalidate()
            temp_raw, current_temp = _task7_read_named_regular_file(
                context,
                directory,
                temp_name,
                mode=0o600,
                maximum_size=len(intended),
            )
            if (
                temp_metadata is None
                or not _same_identity(current_temp, temp_metadata)
                or _stage_stable_metadata(current_temp)
                != _stage_stable_metadata(temp_metadata)
                or temp_raw != intended
            ):
                raise OperationStateError(
                    "policy environment temporary binding or bytes changed"
                )
            _task8_validate_policy_environment_proof(
                context,
                original,
                expected_previous,
                label="policy environment compare-and-swap target",
            )
            if directory.descriptor is None:
                original_metadata = original.metadata
                assert original_metadata is not None
                _close_snapshot_target_proof(original)
                final_target = _capture_snapshot_target(directory, target)
                try:
                    _task8_validate_policy_environment_proof(
                        context,
                        final_target,
                        expected_previous,
                        label="policy environment final compare-and-swap target",
                    )
                    if (
                        final_target.metadata is None
                        or not _same_identity(final_target.metadata, original_metadata)
                        or _stage_stable_metadata(final_target.metadata)
                        != _stage_stable_metadata(original_metadata)
                    ):
                        raise OperationStateError(
                            "policy environment final compare-and-swap binding changed"
                        )
                finally:
                    _close_snapshot_target_proof(final_target)
            if directory.descriptor is None:
                os.replace(temp_path, target_path)
            else:
                os.replace(
                    temp_name,
                    target_name,
                    src_dir_fd=directory.descriptor,
                    dst_dir_fd=directory.descriptor,
                )
            replaced = True
            _task7_refresh_mutable_directory_proof(directory)
            _atomic_event_hook(
                "task8_policy_after_environment_replace",
                action=action,
                target=target,
                temp_path=str(temp_path),
            )
            if directory.descriptor is not None:
                os.fsync(directory.descriptor)
            elif os.name == "posix":
                _fsync_parent_directory(target_path)
            _atomic_event_hook(
                "task8_policy_after_environment_parent_fsync",
                action=action,
                target=target,
                temp_path=str(temp_path),
            )
            _task7_refresh_mutable_directory_proof(directory)
            installed = _capture_snapshot_target(directory, target)
            try:
                result = _task8_validate_policy_environment_proof(
                    context,
                    installed,
                    intended,
                    label="applied policy environment",
                )
                if (
                    temp_metadata is None
                    or installed.metadata is None
                    or not _same_identity(installed.metadata, temp_metadata)
                ):
                    raise OperationStateError(
                        "applied policy environment is not the staged temporary"
                    )
            finally:
                _close_snapshot_target_proof(installed)
            revalidate()
            _atomic_event_hook(
                "task8_policy_after_environment_readback",
                action=action,
                target=target,
                temp_path=str(temp_path),
            )
        except Exception as policy_error:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                descriptor = None
            if not replaced and owned_temp_metadata is not None:
                try:
                    current = _task7_named_metadata(directory, temp_name)
                    if _same_identity(current, owned_temp_metadata):
                        if directory.descriptor is None:
                            temp_path.unlink()
                        else:
                            os.unlink(temp_name, dir_fd=directory.descriptor)
                        _task7_refresh_mutable_directory_proof(directory)
                        if directory.descriptor is not None:
                            os.fsync(directory.descriptor)
                        elif os.name == "posix":
                            _fsync_parent_directory(temp_path)
                except OSError:
                    pass
            try:
                _task7_refresh_mutable_directory_proof(directory)
            except Exception:
                pass
            if isinstance(policy_error, OSError):
                raise OperationStateError(
                    sanitize_error_text(policy_error)
                ) from policy_error
            raise
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            _close_snapshot_target_proof(original)
    if result is None:
        raise OperationStateError("policy environment replacement returned no receipt")
    return result


def _task8_require_exact_policy_environment(
    context: OperationsContext,
    expected: bytes,
) -> dict[str, object]:
    target = _TARGET_ORDER[-1]
    parent = str(PurePosixPath(target).parent)
    with _open_host_directory(context, parent) as directory:
        proof = _capture_snapshot_target(directory, target)
        try:
            return _task8_validate_policy_environment_proof(
                context,
                proof,
                expected,
                label="live policy environment",
            )
        finally:
            _close_snapshot_target_proof(proof)


def _task8_capture_allowed_policy_environment(
    context: OperationsContext,
    allowed: tuple[bytes, ...],
) -> tuple[bytes, dict[str, object]]:
    if (
        not allowed
        or any(type(item) is not bytes or not item for item in allowed)
        or len(set(allowed)) != len(allowed)
    ):
        raise OperationStateError("allowed policy environment set is invalid")
    target = _TARGET_ORDER[-1]
    parent = str(PurePosixPath(target).parent)
    with _open_host_directory(context, parent) as directory:
        proof = _capture_snapshot_target(directory, target)
        try:
            raw = proof.contents
            if raw not in allowed:
                raise OperationStateError(
                    "live policy environment has an unauthorized digest"
                )
            assert isinstance(raw, bytes)
            receipt = _task8_validate_policy_environment_proof(
                context,
                proof,
                raw,
                label="live policy recovery environment",
            )
            return raw, receipt
        finally:
            _close_snapshot_target_proof(proof)


def _task7_cleanup_owned_install_temp(
    context: OperationsContext,
    target: str,
    expected_staged: bytes,
) -> None:
    parent = str(PurePosixPath(target).parent)
    name = _task7_temp_name(context, target, "install")
    with _open_host_directory(context, parent) as directory:
        try:
            _task7_named_metadata(directory, name)
        except FileNotFoundError:
            return
        raw, metadata = _task7_read_named_regular_file(
            context,
            directory,
            name,
            mode=_task7_install_mode(target),
            maximum_size=len(expected_staged),
        )
        if raw != expected_staged:
            raise OperationStateError(
                "operation-owned install temporary has unexpected bytes"
            )
        current = _task7_named_metadata(directory, name)
        if not _same_identity(current, metadata):
            raise OperationStateError(
                "operation-owned install temporary binding changed"
            )
        if directory.descriptor is None:
            (directory.path / name).unlink()
        else:
            os.unlink(name, dir_fd=directory.descriptor)
        _task7_refresh_mutable_directory_proof(directory)
        if directory.descriptor is not None:
            os.fsync(directory.descriptor)
        elif os.name == "posix":
            _fsync_parent_directory(directory.path / name)


def _task7_snapshot_restore_bytes(
    state: dict[str, object],
    material: _VerifiedTransactionMaterial,
    target: str,
) -> tuple[bytes | None, int | None, int | None, int | None]:
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    receipt = targets[target]
    assert isinstance(receipt, dict)
    if not receipt["present"]:
        absent_name = f"{_SNAPSHOT_TARGET_NAMES[target]}.absent"
        expected_marker = f"ABSENT {target}\n".encode("ascii")
        if material.snapshot_proof.expected_bytes[absent_name] != expected_marker:
            raise OperationStateError("snapshot absence artifact changed")
        return None, None, None, None
    raw = material.snapshot_proof.expected_bytes[_SNAPSHOT_TARGET_NAMES[target]]
    if hashlib.sha256(raw).hexdigest() != receipt["sha256"]:
        raise OperationStateError("snapshot restore artifact hash changed")
    return raw, int(receipt["mode"]), int(receipt["uid"]), int(receipt["gid"])


def _task7_remove_recovery_temp_if_owned(
    context: OperationsContext,
    directory: _HostDirectoryProof,
    target: str,
    expected: bytes,
    mode: int,
) -> None:
    name = _task7_temp_name(context, target, "recovery")
    try:
        _task7_named_metadata(directory, name)
    except FileNotFoundError:
        return
    raw, metadata = _task7_read_named_regular_file(
        context,
        directory,
        name,
        mode=mode,
        maximum_size=len(expected),
    )
    if raw != expected:
        raise OperationStateError("recovery temporary has unexpected bytes")
    current = _task7_named_metadata(directory, name)
    if not _same_identity(current, metadata):
        raise OperationStateError("recovery temporary binding changed")
    if directory.descriptor is None:
        (directory.path / name).unlink()
    else:
        os.unlink(name, dir_fd=directory.descriptor)
    _task7_refresh_mutable_directory_proof(directory)
    if directory.descriptor is not None:
        os.fsync(directory.descriptor)
    elif os.name == "posix":
        _fsync_parent_directory(directory.path / name)


def _task7_atomic_restore_present_target(
    context: OperationsContext,
    target: str,
    contents: bytes,
    mode: int,
    uid: int,
    gid: int,
    index: int,
) -> None:
    parent = str(PurePosixPath(target).parent)
    name = PurePosixPath(target).name
    temp_name = _task7_temp_name(context, target, "recovery")
    descriptor: int | None = None
    owned_temp_metadata: os.stat_result | None = None
    temp_metadata: os.stat_result | None = None
    replaced = False
    with _open_host_directory(context, parent) as directory:
        target_path = directory.path / name
        temp_path = directory.path / temp_name
        _task7_remove_recovery_temp_if_owned(
            context,
            directory,
            target,
            contents,
            mode,
        )
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
            descriptor = (
                os.open(temp_path, flags, mode)
                if directory.descriptor is None
                else os.open(temp_name, flags, mode, dir_fd=directory.descriptor)
            )
            owned_temp_metadata = os.fstat(descriptor)
            if os.name == "posix":
                os.fchmod(descriptor, mode)
                os.fchown(descriptor, uid, gid)
            _write_all(descriptor, contents)
            os.fsync(descriptor)
            temp_metadata = os.fstat(descriptor)
            if os.name == "posix":
                if (
                    stat.S_IMODE(temp_metadata.st_mode) != mode
                    or temp_metadata.st_uid != uid
                    or temp_metadata.st_gid != gid
                ):
                    raise OperationStateError("recovery temporary metadata is incorrect")
            os.close(descriptor)
            descriptor = None
            _task7_refresh_mutable_directory_proof(directory)
            _atomic_event_hook(
                "task7_after_recovery_file_fsync",
                index=index,
                target=target,
                temp_path=str(temp_path),
            )
            current_temp = _task7_named_metadata(directory, temp_name)
            if (
                not _same_identity(current_temp, temp_metadata)
                or _stage_stable_metadata(current_temp)
                != _stage_stable_metadata(temp_metadata)
            ):
                raise OperationStateError("recovery temporary binding changed")
            if directory.descriptor is None:
                os.replace(temp_path, target_path)
            else:
                os.replace(
                    temp_name,
                    name,
                    src_dir_fd=directory.descriptor,
                    dst_dir_fd=directory.descriptor,
                )
            replaced = True
            _task7_refresh_mutable_directory_proof(directory)
            _atomic_event_hook(
                "task7_after_recovery_target_replace",
                index=index,
                target=target,
            )
            if directory.descriptor is not None:
                os.fsync(directory.descriptor)
            elif os.name == "posix":
                _fsync_parent_directory(target_path)
            _atomic_event_hook(
                "task7_after_recovery_target_parent_fsync",
                index=index,
                target=target,
            )
            _task7_refresh_mutable_directory_proof(directory)
        except Exception as recovery_error:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                descriptor = None
            if not replaced and owned_temp_metadata is not None:
                try:
                    current = _task7_named_metadata(directory, temp_name)
                    if _same_identity(current, owned_temp_metadata):
                        if directory.descriptor is None:
                            temp_path.unlink()
                        else:
                            os.unlink(temp_name, dir_fd=directory.descriptor)
                        _task7_refresh_mutable_directory_proof(directory)
                        if directory.descriptor is not None:
                            os.fsync(directory.descriptor)
                        elif os.name == "posix":
                            _fsync_parent_directory(temp_path)
                except OSError:
                    pass
            try:
                _task7_refresh_mutable_directory_proof(directory)
            except Exception:
                pass
            if isinstance(recovery_error, OSError):
                raise OperationStateError(
                    sanitize_error_text(recovery_error)
                ) from recovery_error
            raise
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def _task7_restore_target(
    context: OperationsContext,
    state: dict[str, object],
    material: _VerifiedTransactionMaterial,
    expected_staged: dict[str, bytes],
    target: str,
    index: int,
) -> None:
    _atomic_event_hook("task7_before_recovery_target", index=index, target=target)
    _task7_cleanup_owned_install_temp(context, target, expected_staged[target])
    raw, mode, uid, gid = _task7_snapshot_restore_bytes(state, material, target)
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    snapshot_targets = snapshot["targets"]
    assert isinstance(snapshot_targets, dict)
    current = _task7_capture_target_receipt(context, target)
    if current == snapshot_targets[target]:
        return
    if raw is None:
        parent = str(PurePosixPath(target).parent)
        name = PurePosixPath(target).name
        with _open_host_directory(context, parent) as directory:
            latest = _task7_capture_target_receipt(context, target)
            if not latest["present"]:
                return
            if directory.descriptor is None:
                (directory.path / name).unlink()
            else:
                os.unlink(name, dir_fd=directory.descriptor)
            _task7_refresh_mutable_directory_proof(directory)
            _atomic_event_hook(
                "task7_after_target_unlink",
                index=index,
                target=target,
            )
            if directory.descriptor is not None:
                os.fsync(directory.descriptor)
            elif os.name == "posix":
                _fsync_parent_directory(directory.path / name)
            _atomic_event_hook(
                "task7_after_recovery_target_parent_fsync",
                index=index,
                target=target,
            )
        return
    assert mode is not None and uid is not None and gid is not None
    _task7_atomic_restore_present_target(
        context,
        target,
        raw,
        mode,
        uid,
        gid,
        index,
    )


def _task7_recovery_phase_for_kind(kind: str) -> str:
    if kind == "install":
        return "recovering"
    if kind == "manual_rollback":
        return "manual_rollback"
    raise OperationStateError("Task 7 recovery kind is unsupported")


def _task7_build_recovery_entry(
    context: OperationsContext,
    previous: dict[str, object],
    kind: str,
    runtime_directory_created: bool,
    runtime_baseline: dict[str, object],
    live_receipts: dict[str, dict[str, object]],
) -> dict[str, object]:
    epoch = _task7_state_epoch(context, previous)
    phase = _task7_recovery_phase_for_kind(kind)
    target = _TARGET_ORDER[0]
    snapshot = previous["snapshot"]
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    restore = targets[target]
    assert isinstance(restore, dict)
    current = live_receipts[target]
    previous_hash = current["sha256"] if current["present"] else None
    intended_hash = restore["sha256"] if restore["present"] else None
    evidence_payload = {
        "kind": kind,
        "operation_id": context.operation_id,
        "runtime_directory_created": runtime_directory_created,
        "runtime_baseline": copy.deepcopy(runtime_baseline),
        "snapshot_manifest_sha256": snapshot["manifest_sha256"],
    }
    state = copy.deepcopy(previous)
    state["recovery"] = {
        "kind": kind,
        "next_target_index": 0,
        "current_target": target,
        "previous_sha256": previous_hash,
        "intended_sha256": intended_hash,
        "started_epoch": epoch,
        "completed_epoch": None,
        "evidence_sha256": _task7_evidence_sha256(
            "recovery-start",
            evidence_payload,
        ),
        "runtime_directory_created": runtime_directory_created,
        "runtime_baseline": copy.deepcopy(runtime_baseline),
        "restored_epoch": None,
        "restore_evidence_sha256": None,
    }
    _task7_append_history(
        state,
        phase,
        epoch,
        "recovery-entry",
        evidence_payload,
    )
    validate_operation_state(state, context.paths.operation_dir, previous)
    validate_operation_state_for_context(state, context)
    return state


def _task7_resume_recovery_state(
    context: OperationsContext,
    previous: dict[str, object],
) -> dict[str, object]:
    recovery = previous["recovery"]
    assert isinstance(recovery, dict)
    kind = str(recovery["kind"])
    phase = _task7_recovery_phase_for_kind(kind)
    epoch = _task7_state_epoch(context, previous)
    state = copy.deepcopy(previous)
    _task7_append_history(
        state,
        phase,
        epoch,
        "recovery-resume",
        {
            "kind": kind,
            "started_epoch": recovery["started_epoch"],
            "evidence_sha256": recovery["evidence_sha256"],
        },
    )
    validate_operation_state(state, context.paths.operation_dir, previous)
    validate_operation_state_for_context(state, context)
    return state


def _task7_advance_recovery_cursor(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    next_index: int,
) -> dict[str, object]:
    updated = copy.deepcopy(state)
    recovery = updated["recovery"]
    assert isinstance(recovery, dict)
    if next_index == len(_TARGET_ORDER):
        cursor = (None, None, None)
    else:
        target = _TARGET_ORDER[next_index]
        current = _task7_capture_target_receipt(context, target)
        snapshot = updated["snapshot"]
        assert isinstance(snapshot, dict)
        targets = snapshot["targets"]
        assert isinstance(targets, dict)
        restore = targets[target]
        assert isinstance(restore, dict)
        cursor = (
            target,
            current["sha256"] if current["present"] else None,
            restore["sha256"] if restore["present"] else None,
        )
    recovery.update(
        {
            "next_target_index": next_index,
            "current_target": cursor[0],
            "previous_sha256": cursor[1],
            "intended_sha256": cursor[2],
        }
    )
    _task7_write_state(context, binding, updated)
    _atomic_event_hook("task7_after_cursor_state", index=next_index)
    return updated


def _task7_require_snapshot_restored(
    context: OperationsContext,
    state: dict[str, object],
) -> None:
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    for target in _TARGET_ORDER:
        if _task7_capture_target_receipt(context, target) != targets[target]:
            raise OperationStateError(
                f"recovered target differs from immutable snapshot: {target}"
            )


def _task7_record_recovery_validation(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    prior_runtime: dict[str, object],
) -> dict[str, object]:
    epoch = _task7_state_epoch(context, state)
    updated = copy.deepcopy(state)
    recovery = updated["recovery"]
    assert isinstance(recovery, dict)
    recovery["restored_epoch"] = epoch
    recovery["restore_evidence_sha256"] = _task7_evidence_sha256(
        "recovery-validation",
        {
            "kind": recovery["kind"],
            "snapshot_manifest_sha256": updated["snapshot"]["manifest_sha256"],
            "protected_pids": prior_runtime["pids"],
        },
    )
    _task7_write_state(context, binding, updated)
    _atomic_event_hook("task7_after_recovery_validation_state")
    return updated


def _task7_complete_recovery(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
) -> dict[str, object]:
    epoch = _task7_state_epoch(context, state)
    updated = copy.deepcopy(state)
    recovery = updated["recovery"]
    assert isinstance(recovery, dict)
    recovery["completed_epoch"] = epoch
    _task7_append_history(
        updated,
        "rolled_back",
        epoch,
        "recovery-complete",
        {
            "kind": recovery["kind"],
            "restore_evidence_sha256": recovery["restore_evidence_sha256"],
        },
    )
    _task7_write_state(context, binding, updated)
    return updated


def _task7_mark_recovery_required(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
) -> dict[str, object] | None:
    try:
        state = _task7_load_state(context)
        if state["phase"] == "recovery_required":
            return state
        if state["phase"] not in {"recovering", "manual_rollback"}:
            return None
        recovery = state["recovery"]
        assert isinstance(recovery, dict)
        epoch = _task7_state_epoch(context, state)
        updated = copy.deepcopy(state)
        _task7_append_history(
            updated,
            "recovery_required",
            epoch,
            "recovery-required",
            {
                "kind": recovery["kind"],
                "next_target_index": recovery["next_target_index"],
            },
        )
        _task7_write_state(context, binding, updated)
        return updated
    except Exception:
        return None


def install_host_configuration(context: OperationsContext) -> dict[str, object]:
    primary_error: Exception | None = None
    receipt_persistence_error: Exception | None = None
    pending_rclone_group: dict[str, object] | None = None
    pending_secondary_errors: list[tuple[str, BaseException]] = []
    audit_incomplete = False
    marker_written = False
    completed_state: dict[str, object] | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        if state["phase"] != "snapshotted":
            raise OperationStateError(
                "installation cannot continue from an incomplete operation; run "
                + _task7_recovery_command(context)
            )
        locks: MigrationLocks | None = None
        release_issues: list[_Task7LockReleaseIssue] = []
        try:
            with _open_verified_transaction_material(
                context,
                frozenset({"snapshotted"}),
            ) as material:
                if material.source.state != state:
                    raise OperationStateError(
                        "operation state changed before transaction proof"
                    )
                expected_bytes = _task7_install_bytes(material)
                _task7_require_no_install_temporaries(context)
                runtime_directory_created = not _task7_runtime_directory_preexists(
                    context
                )
                prior_runtime = state["prior_runtime"]
                assert isinstance(prior_runtime, dict)
                with _open_task7_live_target_proofs(context) as live_proofs:
                    _task7_require_snapshot_live_targets(state, live_proofs)
                    if _capture_prior_runtime(context) != prior_runtime:
                        raise OperationStateError(
                            "protected runtime or timer changed before installation"
                        )
                    _revalidate_transaction_material(material)
                    _task7_revalidate_live_target_proofs(live_proofs)
                    installing = _task7_build_installing_state(
                        context,
                        state,
                        expected_bytes,
                        runtime_directory_created=runtime_directory_created,
                    )

                    def revalidate_install_entry() -> None:
                        _revalidate_transaction_material(material)
                        _task7_revalidate_live_target_proofs(live_proofs)
                        _task7_require_snapshot_live_targets(state, live_proofs)
                        _task7_require_no_install_temporaries(context)
                        if (
                            not _task7_runtime_directory_preexists(context)
                        ) != runtime_directory_created:
                            raise OperationStateError(
                                "runtime directory presence changed before installation"
                            )
                        if _capture_prior_runtime(context) != prior_runtime:
                            raise OperationStateError(
                                "protected runtime or timer changed before installation"
                            )
                        _revalidate_transaction_material(material)

                    _task7_write_state(
                        context,
                        binding,
                        installing,
                        pre_replace_validator=revalidate_install_entry,
                    )
                    marker_written = True
                    state = installing
                    _atomic_event_hook("task7_after_installing_state")
                    revalidate_install_entry()
                    locks = _task7_acquire_migration_locks(
                        context,
                        runtime_directory_created=runtime_directory_created,
                    )
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "operation state changed while migration locks were acquired"
                        )
                    _revalidate_transaction_material(material)
                    _task7_revalidate_live_target_proofs(live_proofs)
                    if _capture_prior_runtime(context) != prior_runtime:
                        raise OperationStateError(
                            "protected runtime or timer changed before guard quiesce"
                        )
                    _require_backup_service_inactive(context)
                    _quiesce_backup_timer(context, prior_runtime)
                    _task7_require_protected_pid_parity(context, prior_runtime)
                    _revalidate_transaction_material(material)
                    _task7_revalidate_live_target_proofs(live_proofs)

                snapshot = state["snapshot"]
                assert isinstance(snapshot, dict)
                snapshot_targets = snapshot["targets"]
                assert isinstance(snapshot_targets, dict)
                for index, target in enumerate(_TARGET_ORDER):
                    install = state["install"]
                    assert isinstance(install, dict)
                    if (
                        install["next_target_index"] != index
                        or install["current_target"] != target
                    ):
                        raise OperationStateError(
                            "install cursor changed before target replacement"
                        )
                    _task7_atomic_install_target(
                        context,
                        target,
                        expected_bytes[target],
                        _task7_install_mode(target),
                        index,
                        snapshot_targets[target],
                    )
                    state = _task7_advance_install_cursor(
                        context,
                        binding,
                        state,
                        expected_bytes,
                        index + 1,
                    )
                _task7_systemctl(context, "daemon-reload")
                _atomic_event_hook("task7_after_daemon_reload")
                assert locks is not None
                state = _task7_run_preflight_with_rclone_audit(
                    context,
                    binding,
                    state,
                    locks.runtime_fd,
                )
                installed_hashes = _task7_verify_installed_targets(
                    context,
                    expected_bytes,
                )
                _task7_require_protected_pid_parity(context, prior_runtime)
                _require_backup_service_inactive(context)
                state = _task7_record_install_validation(
                    context,
                    binding,
                    state,
                    installed_hashes,
                    prior_runtime,
                )
                owned_locks = locks
                locks = None
                release_issues.extend(_task7_release_locks(owned_locks))
                if release_issues:
                    raise release_issues[0].error
                _require_backup_service_inactive(context)
                _restore_backup_timer(context, prior_runtime)
                completed_state = _task7_complete_install(
                    context,
                    binding,
                    state,
                    installed_hashes,
                )
        except Exception as exc:
            if (
                isinstance(exc, _Task7RcloneEvidencePersistenceError)
                and exc.preflight_error is not None
                and not isinstance(exc.preflight_error, Exception)
            ):
                try:
                    _task7_persist_pending_rclone_group(
                        context,
                        binding,
                        exc.group,
                    )
                except Exception:
                    pass
                if locks is not None:
                    _task7_emergency_close_locks(locks)
                    locks = None
                raise exc.preflight_error
            primary_error = exc
            if isinstance(exc, _Task7RcloneAuditIncompleteError):
                primary_error = exc.primary_error
                audit_incomplete = True
                if exc.secondary_error is not None:
                    pending_secondary_errors.append(
                        ("rclone_audit_capture", exc.secondary_error)
                    )
            if isinstance(exc, _Task7RcloneEvidencePersistenceError):
                pending_rclone_group = copy.deepcopy(exc.group)
                if exc.preflight_error is not None:
                    primary_error = (
                        exc.preflight_error
                        if isinstance(exc.preflight_error, Exception)
                        else exc
                    )
                    pending_secondary_errors.append(
                        ("rclone_audit_persistence", exc.persistence_error)
                    )
            if locks is not None:
                owned_locks = locks
                locks = None
                release_issues.extend(_task7_release_locks(owned_locks))
            if marker_written:
                try:
                    _task7_record_primary_failure(
                        context,
                        binding,
                        primary_error,
                    )
                    for stage, secondary_error in pending_secondary_errors:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            stage,
                            secondary_error,
                        )
                    for stage, release_error in release_issues:
                        if release_error is exc:
                            continue
                        _task7_append_secondary_error(
                            context,
                            binding,
                            stage,
                            release_error,
                        )
                except Exception as persistence_error:
                    receipt_persistence_error = persistence_error
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            raise
    if primary_error is not None:
        if receipt_persistence_error is not None:
            raise primary_error from receipt_persistence_error
        if audit_incomplete:
            raise primary_error
        if marker_written:
            try:
                _task7_run_recovery(
                    context,
                    manual_request=False,
                    pending_rclone_group=pending_rclone_group,
                )
            except Exception:
                pass
        raise primary_error
    if completed_state is None:
        raise OperationStateError("installation did not produce a terminal receipt")
    return completed_state


def _task7_run_recovery(
    context: OperationsContext,
    *,
    manual_request: bool,
    pending_rclone_group: dict[str, object] | None = None,
) -> dict[str, object]:
    terminal: dict[str, object] | None = None
    raised: Exception | None = None
    with _open_operation_transaction(context) as binding:
        initial = _task7_load_state(context)
        if pending_rclone_group is not None:
            initial = _task7_persist_pending_rclone_group(
                context,
                binding,
                pending_rclone_group,
            )
        initial = _task7_finalize_pending_rclone_audit(
            context,
            binding,
            initial,
        )
        stable_phases = {
            "installed",
            "probed",
            "dry_run_recorded",
            "policy_enabled",
            "observed",
        }
        recoverable_phases = {
            "installing",
            "recovering",
            "manual_rollback",
            "recovery_required",
        }
        if manual_request:
            if initial["phase"] not in stable_phases:
                raise OperationStateError(
                    "manual rollback requires an installed stable phase"
                )
            kind = "manual_rollback"
        else:
            if initial["phase"] not in recoverable_phases:
                raise OperationStateError(
                    "recover requires an incomplete install or rollback state"
                )
            recovery = initial["recovery"]
            if initial["phase"] == "installing":
                kind = "install"
            else:
                assert isinstance(recovery, dict)
                kind = str(recovery["kind"])
            if kind not in {"install", "manual_rollback"}:
                raise OperationStateError(
                    "Task 7 recover cannot resume this transaction kind"
                )
        if kind == "manual_rollback":
            if manual_request:
                runtime_baseline = _capture_prior_runtime(context)
            else:
                existing_recovery = initial["recovery"]
                assert isinstance(existing_recovery, dict)
                runtime_baseline = copy.deepcopy(
                    existing_recovery["runtime_baseline"]
                )
        else:
            immutable_runtime = initial["prior_runtime"]
            assert isinstance(immutable_runtime, dict)
            runtime_baseline = copy.deepcopy(immutable_runtime)
        assert isinstance(runtime_baseline, dict)
        locks: MigrationLocks | None = None
        recovery_started = False
        release_issues: list[_Task7LockReleaseIssue] = []
        failure_stage = "recovery"
        failure_already_recorded = False
        try:
            with _open_verified_transaction_material(
                context,
                frozenset({str(initial["phase"])}),
            ) as material:
                if material.source.state != initial:
                    raise OperationStateError(
                        "operation state changed before recovery proof"
                    )
                expected_staged = _task7_install_bytes(material)
                live_receipts = _task7_require_recoverable_live_targets(
                    context,
                    initial,
                )
                runtime_directory_created = not _task7_runtime_directory_preexists(
                    context
                )
                _revalidate_transaction_material(material)
                locks = _task7_acquire_migration_locks(
                    context,
                    runtime_directory_created=runtime_directory_created,
                )
                if _task7_load_state(context) != initial:
                    raise OperationStateError(
                        "operation state changed while recovery locks were acquired"
                    )
                _revalidate_transaction_material(material)
                _task7_require_recoverable_live_targets(context, initial)
                _require_backup_service_inactive(context)
                if manual_request:
                    if _capture_prior_runtime(context) != runtime_baseline:
                        raise OperationStateError(
                            "manual rollback runtime changed before durable entry"
                        )
                else:
                    _task7_require_protected_pid_parity(
                        context,
                        runtime_baseline,
                    )
                if initial["phase"] in stable_phases or initial["phase"] == "installing":
                    state = _task7_build_recovery_entry(
                        context,
                        initial,
                        kind,
                        runtime_directory_created,
                        runtime_baseline,
                        live_receipts,
                    )
                    _task7_write_state(context, binding, state)
                elif initial["phase"] == "recovery_required":
                    state = _task7_resume_recovery_state(context, initial)
                    _task7_write_state(context, binding, state)
                else:
                    state = initial
                recovery_started = True
                _atomic_event_hook("task7_after_recovery_state")
                _revalidate_transaction_material(material)
                recovery_receipt = state["recovery"]
                assert isinstance(recovery_receipt, dict)
                if recovery_receipt["runtime_baseline"] != runtime_baseline:
                    raise OperationStateError(
                        "durable recovery runtime baseline changed"
                    )
                _quiesce_backup_timer(context, runtime_baseline)
                _require_backup_service_inactive(context)
                _task7_require_protected_pid_parity(context, runtime_baseline)
                start_index = int(recovery_receipt["next_target_index"])
                for index in range(start_index, len(_TARGET_ORDER)):
                    target = _TARGET_ORDER[index]
                    current_recovery = state["recovery"]
                    assert isinstance(current_recovery, dict)
                    if (
                        current_recovery["next_target_index"] != index
                        or current_recovery["current_target"] != target
                    ):
                        raise OperationStateError(
                            "recovery cursor changed before target restoration"
                        )
                    _task7_restore_target(
                        context,
                        state,
                        material,
                        expected_staged,
                        target,
                        index,
                    )
                    state = _task7_advance_recovery_cursor(
                        context,
                        binding,
                        state,
                        index + 1,
                    )
                _task7_systemctl(context, "daemon-reload")
                _atomic_event_hook("task7_after_recovery_daemon_reload")
                _task7_require_snapshot_restored(context, state)
                _atomic_event_hook("task7_before_recovery_pid_validation")
                _task7_require_protected_pid_parity(context, runtime_baseline)
                _require_backup_service_inactive(context)
                recovery_receipt = state["recovery"]
                assert isinstance(recovery_receipt, dict)
                if recovery_receipt["restored_epoch"] is None:
                    state = _task7_record_recovery_validation(
                        context,
                        binding,
                        state,
                        runtime_baseline,
                    )
                owned_locks = locks
                locks = None
                release_issues.extend(_task7_release_locks(owned_locks))
                current_after_release = _task7_load_state(context)
                uncertain_release = next(
                    (issue for issue in release_issues if issue.release_uncertain),
                    None,
                )
                if uncertain_release is not None:
                    primary_release_error: Exception | None = None
                    if current_after_release["failure"] is None:
                        _task7_record_primary_failure(
                            context,
                            binding,
                            uncertain_release.error,
                        )
                        primary_release_error = uncertain_release.error
                    for issue in release_issues:
                        if issue.error is primary_release_error:
                            continue
                        _task7_append_secondary_error(
                            context,
                            binding,
                            issue.stage,
                            issue.error,
                        )
                    release_issues.clear()
                    failure_stage = uncertain_release.stage
                    failure_already_recorded = True
                    raise uncertain_release.error
                if release_issues and current_after_release["failure"] is None:
                    failure_stage = release_issues[0].stage
                    raise release_issues[0].error
                for stage, release_error in release_issues:
                    _task7_append_secondary_error(
                        context,
                        binding,
                        stage,
                        release_error,
                    )
                release_issues.clear()
                state = _task7_load_state(context)
                _require_backup_service_inactive(context)
                try:
                    _restore_backup_timer(context, runtime_baseline)
                except Exception as restore_error:
                    failure_stage = "timer_restore"
                    current_failure_state = _task7_load_state(context)
                    if current_failure_state["failure"] is None:
                        _task7_record_primary_failure(
                            context,
                            binding,
                            restore_error,
                        )
                    else:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "timer_restore",
                            restore_error,
                        )
                    failure_already_recorded = True
                    _atomic_event_hook("task7_after_timer_restore_failure_state")
                    try:
                        _quiesce_backup_timer(context, runtime_baseline)
                    except Exception as quiesce_error:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "timer_quiesce",
                            quiesce_error,
                        )
                    raise
                terminal = _task7_complete_recovery(context, binding, state)
        except Exception as exc:
            raised = exc
            if locks is not None:
                owned_locks = locks
                locks = None
                release_issues.extend(_task7_release_locks(owned_locks))
            if recovery_started:
                if not failure_already_recorded:
                    current = _task7_load_state(context)
                    if current["failure"] is None:
                        _task7_record_primary_failure(context, binding, exc)
                    else:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            failure_stage,
                            exc,
                        )
                for stage, release_error in release_issues:
                    if release_error is exc:
                        continue
                    _task7_append_secondary_error(
                        context,
                        binding,
                        stage,
                        release_error,
                    )
                _task7_mark_recovery_required(context, binding)
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            raise
    if raised is not None:
        raise raised
    if terminal is None:
        raise OperationStateError("recovery did not produce a terminal receipt")
    return terminal


def _task8_guard_recovery_primary_error(transaction_kind: str) -> OperationStateError:
    if transaction_kind not in {"dry_run", "observe"}:
        raise OperationStateError("guard recovery transaction kind is invalid")
    return OperationStateError(
        f"interrupted Task 8 {transaction_kind} transaction requires guard recovery"
    )


def _task8_build_guard_recovery_required(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    transaction = previous.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") not in {
        "dry_run",
        "observe",
    }:
        raise OperationStateError(
            "guard recovery requires an interrupted dry-run or observation transaction"
        )
    expected_phase = _TASK8_ENTRY_PHASES[str(transaction["kind"])]
    prior_recovery = previous.get("recovery")
    recovery_is_replaceable = prior_recovery is None or (
        isinstance(prior_recovery, dict)
        and prior_recovery.get("completed_epoch") is not None
    )
    if previous.get("phase") != expected_phase or not recovery_is_replaceable:
        raise OperationStateError(
            "guard recovery-required entry must start from the interrupted transaction"
        )
    runtime_baseline = transaction.get("runtime_baseline")
    _validate_prior_runtime(
        runtime_baseline,
        "active_transaction.runtime_baseline",
    )
    assert isinstance(runtime_baseline, dict)
    epoch = _task7_state_epoch(context, previous)
    payload = {
        "kind": "guard",
        "operation_id": context.operation_id,
        "transaction_kind": transaction["kind"],
        "prior_stable_phase": transaction["prior_stable_phase"],
        "transaction_started_epoch": transaction["started_epoch"],
        "runtime_baseline": copy.deepcopy(runtime_baseline),
    }
    updated = copy.deepcopy(previous)
    updated["recovery"] = {
        "kind": "guard",
        "next_target_index": 0,
        "current_target": None,
        "previous_sha256": None,
        "intended_sha256": None,
        "started_epoch": epoch,
        "completed_epoch": None,
        "evidence_sha256": _task7_evidence_sha256(
            "guard-recovery-start",
            payload,
        ),
        "runtime_directory_created": False,
        "runtime_baseline": copy.deepcopy(runtime_baseline),
        "restored_epoch": None,
        "restore_evidence_sha256": None,
    }
    _task7_append_history(
        updated,
        "recovery_required",
        epoch,
        "guard-recovery-required",
        payload,
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "guard recovery-required",
    )
    return _task7_load_state(context)


def _task8_enter_recovering_guard(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    recovery = previous.get("recovery")
    transaction = previous.get("active_transaction")
    if (
        previous.get("phase") != "recovery_required"
        or not isinstance(recovery, dict)
        or recovery.get("kind") != "guard"
        or not isinstance(transaction, dict)
        or transaction.get("kind") not in {"dry_run", "observe"}
    ):
        raise OperationStateError("guard recovery resume state is invalid")
    epoch = _task7_state_epoch(context, previous)
    updated = copy.deepcopy(previous)
    _task7_append_history(
        updated,
        "recovering_guard",
        epoch,
        "guard-recovery-resume",
        {
            "kind": "guard",
            "transaction_kind": transaction["kind"],
            "started_epoch": recovery["started_epoch"],
            "evidence_sha256": recovery["evidence_sha256"],
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "guard recovery resume",
    )
    return _task7_load_state(context)


def _task8_guard_progress(state: dict[str, object]) -> dict[str, object]:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict):
        raise OperationStateError("guard recovery requires an active transaction")
    guard = transaction.get("guard")
    if not isinstance(guard, dict):
        raise OperationStateError("guard recovery progress is invalid")
    return guard


def _task8_reconcile_guard_for_recovery(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    revalidate: Callable[[], None] | None = None,
    allow_restored_shortcut: bool = True,
) -> MigrationLocks | None:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict):
        raise OperationStateError("guard recovery requires an active transaction")
    runtime_baseline = transaction.get("runtime_baseline")
    _validate_prior_runtime(
        runtime_baseline,
        "active_transaction.runtime_baseline",
    )
    assert isinstance(runtime_baseline, dict)
    guard = _task8_guard_progress(state)

    def checkpoint(action: str) -> None:
        if revalidate is not None:
            revalidate()
        _task8_force_checkpoint(context, binding, state, action)
        if revalidate is not None:
            revalidate()

    if guard.get("timer_restored") is True and allow_restored_shortcut:
        checkpoint("recovery_final_runtime_readback")
        if _capture_prior_runtime(context) == runtime_baseline:
            checkpoint("recovery_final_service_check")
            _require_backup_service_inactive(context)
            return None

    _quiesce_backup_timer(
        context,
        runtime_baseline,
        before_action=checkpoint,
    )
    guard = _task8_guard_progress(state)
    if guard["timer_stopped"] is False:
        _task8_advance_guard(context, binding, state, "timer_stopped")
        if revalidate is not None:
            revalidate()
    guard = _task8_guard_progress(state)
    if guard["service_inactive_verified"] is False:
        _task8_advance_guard(context, binding, state, "service_inactive_verified")
        if revalidate is not None:
            revalidate()

    lock_milestones = {
        "legacy_lock_acquire": "legacy_lock_acquired",
        "runtime_lock_acquire": "runtime_lock_acquired",
    }

    def acquired(action: str, descriptor: int) -> None:
        del descriptor
        try:
            milestone = lock_milestones[action]
        except KeyError as exc:
            raise OperationStateError(
                "guard recovery lock acquisition action is invalid"
            ) from exc
        current_guard = _task8_guard_progress(state)
        if current_guard[milestone] is False:
            _task8_advance_guard(context, binding, state, milestone)
            if revalidate is not None:
                revalidate()

    return acquire_migration_locks(
        context,
        before_action=checkpoint,
        after_action=acquired,
    )


def _task8_release_recovery_guard_locks(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    locks: MigrationLocks,
    *,
    revalidate: Callable[[], None] | None = None,
) -> None:
    guard = _task8_guard_progress(state)
    if guard.get("runtime_lock_acquired") is not True:
        raise OperationStateError("guard recovery locks are not ready for release")

    def checkpoint(action: str) -> None:
        if revalidate is not None:
            revalidate()
        _task8_force_checkpoint(context, binding, state, action)
        if revalidate is not None:
            revalidate()

    issues = _release_migration_locks(
        locks,
        before_action=checkpoint,
    )
    if issues:
        raise _Task8GuardReleaseError(issues)
    guard = _task8_guard_progress(state)
    if guard["locks_released"] is False:
        _task8_advance_guard(context, binding, state, "locks_released")
        if revalidate is not None:
            revalidate()


def _task8_restore_recovery_guard_timer(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    revalidate: Callable[[], None] | None = None,
) -> None:
    guard = _task8_guard_progress(state)
    if guard.get("locks_released") is not True:
        raise OperationStateError(
            "guard recovery lock release is incomplete; timer restore is blocked"
        )
    transaction = state.get("active_transaction")
    assert isinstance(transaction, dict)
    runtime_baseline = transaction.get("runtime_baseline")
    _validate_prior_runtime(
        runtime_baseline,
        "active_transaction.runtime_baseline",
    )
    assert isinstance(runtime_baseline, dict)

    def checkpoint(action: str) -> None:
        if revalidate is not None:
            revalidate()
        _task8_force_checkpoint(context, binding, state, action)
        if revalidate is not None:
            revalidate()

    checkpoint("pre_restore_service_check")
    _require_backup_service_inactive(context)
    _restore_backup_timer(
        context,
        runtime_baseline,
        before_action=checkpoint,
    )
    guard = _task8_guard_progress(state)
    if guard["timer_restored"] is False:
        _task8_advance_guard(context, binding, state, "timer_restored")
        if revalidate is not None:
            revalidate()


def _task8_complete_guard_recovery(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    if previous.get("phase") != "recovering_guard":
        raise OperationStateError("guard recovery completion phase is invalid")
    recovery = previous.get("recovery")
    transaction = previous.get("active_transaction")
    if (
        not isinstance(recovery, dict)
        or recovery.get("kind") != "guard"
        or recovery.get("completed_epoch") is not None
        or not isinstance(transaction, dict)
        or transaction.get("prior_stable_phase") not in {"probed", "policy_enabled"}
    ):
        raise OperationStateError("guard recovery completion state is invalid")
    guard = _task8_guard_progress(previous)
    if not all(guard.values()):
        raise OperationStateError(
            "guard recovery cannot complete before exact runtime restoration"
        )
    epoch = _task7_state_epoch(context, previous)
    stable_phase = str(transaction["prior_stable_phase"])
    updated = copy.deepcopy(previous)
    updated_recovery = updated["recovery"]
    assert isinstance(updated_recovery, dict)
    updated_recovery["completed_epoch"] = epoch
    updated["active_transaction"] = None
    _task7_append_history(
        updated,
        stable_phase,
        epoch,
        "guard-recovery-complete",
        {
            "kind": "guard",
            "transaction_kind": transaction["kind"],
            "started_epoch": recovery["started_epoch"],
            "guard": guard,
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "guard recovery completion",
    )
    return _task7_load_state(context)


def _task8_mark_guard_recovery_required(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
) -> dict[str, object] | None:
    state = _task7_load_state(context)
    if state["phase"] == "recovery_required":
        return state
    if state["phase"] != "recovering_guard":
        return None
    recovery = state.get("recovery")
    transaction = state.get("active_transaction")
    if (
        not isinstance(recovery, dict)
        or recovery.get("kind") != "guard"
        or not isinstance(transaction, dict)
    ):
        raise OperationStateError("guard recovery-required state is invalid")
    epoch = _task7_state_epoch(context, state)
    updated = copy.deepcopy(state)
    _task7_append_history(
        updated,
        "recovery_required",
        epoch,
        "guard-recovery-required",
        {
            "kind": "guard",
            "transaction_kind": transaction["kind"],
            "started_epoch": recovery["started_epoch"],
            "guard": transaction["guard"],
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, state)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "guard recovery-required",
    )
    return _task7_load_state(context)


def _task8_probe_recovery_primary_error() -> OperationStateError:
    return OperationStateError(
        "interrupted Task 8 probe transaction requires probe recovery"
    )


def _task8_build_probe_recovery_required(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    transaction = previous.get("active_transaction")
    prior_recovery = previous.get("recovery")
    recovery_is_replaceable = prior_recovery is None or (
        isinstance(prior_recovery, dict)
        and prior_recovery.get("completed_epoch") is not None
    )
    if (
        previous.get("phase") != "probing"
        or not recovery_is_replaceable
        or not isinstance(transaction, dict)
        or transaction.get("kind") != "probe"
    ):
        raise OperationStateError(
            "probe recovery-required entry must start from probing"
        )
    runtime_baseline = transaction.get("runtime_baseline")
    _validate_prior_runtime(
        runtime_baseline,
        "active_transaction.runtime_baseline",
    )
    assert isinstance(runtime_baseline, dict)
    epoch = _task7_state_epoch(context, previous)
    probe = transaction.get("probe")
    if not isinstance(probe, dict):
        raise OperationStateError("probe recovery identity is missing")
    payload = {
        "kind": "probe",
        "operation_id": context.operation_id,
        "transaction_started_epoch": transaction["started_epoch"],
        "prefix": probe["prefix"],
        "runtime_baseline": copy.deepcopy(runtime_baseline),
    }
    updated = copy.deepcopy(previous)
    updated["recovery"] = {
        "kind": "probe",
        "next_target_index": 0,
        "current_target": None,
        "previous_sha256": None,
        "intended_sha256": None,
        "started_epoch": epoch,
        "completed_epoch": None,
        "evidence_sha256": _task7_evidence_sha256(
            "probe-recovery-start",
            payload,
        ),
        "runtime_directory_created": False,
        "runtime_baseline": copy.deepcopy(runtime_baseline),
        "restored_epoch": None,
        "restore_evidence_sha256": None,
    }
    _task7_append_history(
        updated,
        "recovery_required",
        epoch,
        "probe-recovery-required",
        payload,
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "probe recovery-required",
    )
    return _task7_load_state(context)


def _task8_enter_recovering_probe(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    recovery = previous.get("recovery")
    transaction = previous.get("active_transaction")
    if (
        previous.get("phase") != "recovery_required"
        or not isinstance(recovery, dict)
        or recovery.get("kind") != "probe"
        or not isinstance(transaction, dict)
        or transaction.get("kind") != "probe"
    ):
        raise OperationStateError("probe recovery resume state is invalid")
    epoch = _task7_state_epoch(context, previous)
    updated = copy.deepcopy(previous)
    _task7_append_history(
        updated,
        "recovering_probe",
        epoch,
        "probe-recovery-resume",
        {
            "kind": "probe",
            "started_epoch": recovery["started_epoch"],
            "evidence_sha256": recovery["evidence_sha256"],
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "probe recovery resume",
    )
    return _task7_load_state(context)


def _task8_probe_objects(state: dict[str, object]) -> list[dict[str, object]]:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        raise OperationStateError("probe recovery requires an active probe transaction")
    probe = transaction.get("probe")
    if not isinstance(probe, dict):
        raise OperationStateError("probe recovery identity is missing")
    objects = probe.get("objects")
    if not isinstance(objects, list):
        raise OperationStateError("probe recovery object identity is invalid")
    return objects


def _task8_advance_probe_object(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    name: str,
    field: str,
) -> dict[str, object]:
    if field not in {"created", "verified", "cleaned"}:
        raise OperationStateError("probe recovery progress field is invalid")
    objects = _task8_probe_objects(state)
    matches = [index for index, item in enumerate(objects) if item.get("name") == name]
    if len(matches) != 1:
        raise OperationStateError("probe recovery object name is not unique")
    index = matches[0]
    if objects[index].get(field) is not False:
        raise OperationStateError("probe recovery progress may advance only once")
    updated = copy.deepcopy(state)
    updated_objects = _task8_probe_objects(updated)
    updated_objects[index][field] = True
    validate_operation_state(updated, context.paths.operation_dir, state)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        f"probe {name} {field} progress",
    )
    durable = _task7_load_state(context)
    state.clear()
    state.update(durable)
    return state


def _task8_current_probe_last_success_purpose(
    state: dict[str, object],
) -> str | None:
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    if not groups:
        return None
    group = groups[-1]
    if (
        not isinstance(group, dict)
        or group["group_id"] == "install"
        or group["after"] is None
        or group.get("outcome") != "success"
    ):
        return None
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        return None
    entries = _task8_attempt_entries(state, "probe")
    if not entries or entries[-1]["epoch"] != transaction["started_epoch"]:
        return None
    kind, started_epoch, attempt_ordinal, _group_ordinal = (
        _task8_group_identity(group)
    )
    if (
        kind != "probe"
        or started_epoch != transaction["started_epoch"]
        or attempt_ordinal != len(entries) - 1
    ):
        return None
    return str(group["purpose"])


def _task8_reconcile_probe_success_progress(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
) -> dict[str, object]:
    while True:
        purpose = _task8_current_probe_last_success_purpose(state)
        if purpose is None:
            return state
        action: tuple[str, str] | None = None
        for item in _task8_probe_objects(state):
            name = str(item["name"])
            if (
                purpose == f"probe-verify:{name}"
                and item["created"] is True
                and item["verified"] is False
            ):
                action = (name, "verified")
                break
            if (
                purpose == f"probe-cleanup:{name}"
                and item["created"] is True
                and item["verified"] is True
                and item["cleaned"] is False
            ):
                action = (name, "cleaned")
                break
            if purpose == f"probe-adopt:{name}":
                if item["created"] is False:
                    action = (name, "created")
                    break
                if item["verified"] is False:
                    action = (name, "verified")
                    break
        if action is None:
            return state
        with _task8_open_exact_guard_recovery_material(context, state):
            _task8_advance_probe_object(
                context,
                binding,
                state,
                action[0],
                action[1],
            )
        _task8_require_probe_purpose_chain(state)


def _task8_probe_completed_events(
    state: dict[str, object],
) -> list[tuple[str, str]]:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        raise OperationStateError("probe recovery requires an active transaction")
    started_epoch = int(transaction["started_epoch"])
    entries = _task8_attempt_entries(state, "probe")
    if not entries or entries[-1]["epoch"] != started_epoch:
        raise OperationStateError("probe recovery attempt identity is invalid")
    attempt_ordinal = len(entries) - 1
    ordered: list[tuple[int, str, str]] = []
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    for group in groups:
        assert isinstance(group, dict)
        if group["group_id"] == "install" or group["after"] is None:
            continue
        kind, group_started, group_attempt, group_ordinal = (
            _task8_group_identity(group)
        )
        if (
            kind == "probe"
            and group_started == started_epoch
            and group_attempt == attempt_ordinal
        ):
            outcome = group.get("outcome")
            if outcome not in _TASK8_RCLONE_OUTCOMES:
                raise OperationStateError(
                    "completed probe rclone event outcome is invalid"
                )
            ordered.append(
                (group_ordinal, str(group["purpose"]), str(outcome))
            )
    ordered.sort(key=lambda item: item[0])
    return [(purpose, outcome) for _ordinal, purpose, outcome in ordered]


def _task8_probe_completed_purposes(state: dict[str, object]) -> set[str]:
    return {
        purpose
        for purpose, outcome in _task8_probe_completed_events(state)
        if outcome == "success"
    }


def _task8_probe_indeterminate_purposes(
    state: dict[str, object],
) -> set[str]:
    return {
        purpose
        for purpose, outcome in _task8_probe_completed_events(state)
        if outcome == "indeterminate"
    }


def _task8_probe_completed_purpose_sequence(
    state: dict[str, object],
) -> list[str]:
    return [
        purpose
        for purpose, outcome in _task8_probe_completed_events(state)
        if outcome == "success"
    ]


def _task8_require_probe_event_chain(
    objects: list[dict[str, object]],
    events: list[tuple[str, str]],
) -> None:
    object_by_name = {str(item["name"]): item for item in objects}
    if len(object_by_name) != len(objects):
        raise OperationStateError("probe recovery object names are not unique")
    progress = {}
    for name in object_by_name:
        progress[name] = {
            "create_intent": 0,
            "create_success": 0,
            "create_indeterminate": 0,
            "create_index": -1,
            "verify_success": 0,
            "adopt_success": 0,
            "cleanup_success": 0,
            "cleanup_indeterminate_indices": [],
            "inventory_success_indices": [],
        }
    create_purposes = {
        f"probe-create:{name}:strict-no-existing": name
        for name in object_by_name
    }
    verify_purposes = {
        f"probe-verify:{name}": name for name in object_by_name
    }
    adopt_purposes = {
        f"probe-adopt:{name}": name for name in object_by_name
    }
    cleanup_purposes = {
        f"probe-cleanup:{name}": name for name in object_by_name
    }
    root_observed = False
    precreate_attempted = False
    precreate_succeeded = False
    owned_inventory_succeeded = False
    prefix_empty_seen = False
    recovery_namespace_stage: str | None = None

    for index, (purpose, outcome) in enumerate(events):
        succeeded = outcome == "success"
        if prefix_empty_seen:
            if purpose == "probe-recovery-root":
                prefix_empty_seen = False
                recovery_namespace_stage = "root" if succeeded else None
                continue
            if purpose != "probe-prefix-empty":
                raise OperationStateError(
                    "probe prefix-empty purpose must be the terminal ownership event"
                )
            continue
        if purpose == "probe-prefix-empty":
            if succeeded and any(
                item["created"] and not item["cleaned"] for item in objects
            ):
                raise OperationStateError(
                    "probe prefix-empty purpose precedes durable object cleanup"
                )
            if succeeded:
                prefix_empty_seen = True
            continue
        if purpose == "probe-recovery-root":
            recovery_namespace_stage = "root" if succeeded else None
            continue
        if purpose == "probe-recovery-root-absence":
            if recovery_namespace_stage != "root":
                raise OperationStateError(
                    "probe recovery root-absence proof lacks a fresh root inventory"
                )
            recovery_namespace_stage = None
            if succeeded:
                prefix_empty_seen = True
                for item_progress in progress.values():
                    item_progress["inventory_success_indices"].append(index)
            continue
        if purpose == "probe-recovery-parent":
            if recovery_namespace_stage != "root":
                raise OperationStateError(
                    "probe recovery parent proof lacks a fresh root inventory"
                )
            recovery_namespace_stage = "parent" if succeeded else None
            continue
        if purpose == "probe-recovery-parent-absence":
            if recovery_namespace_stage != "parent":
                raise OperationStateError(
                    "probe recovery parent-absence proof lacks a fresh parent inventory"
                )
            recovery_namespace_stage = None
            if succeeded:
                prefix_empty_seen = True
                for item_progress in progress.values():
                    item_progress["inventory_success_indices"].append(index)
            continue
        if purpose == "probe-precreate-root":
            if root_observed or precreate_attempted or index != 0:
                raise OperationStateError(
                    "probe namespace-root purpose is duplicated or out of order"
                )
            root_observed = succeeded
            continue
        if purpose in {
            "probe-precreate-root-absence",
            "probe-precreate-parent-absence",
        }:
            if precreate_attempted or not root_observed or index != 1:
                raise OperationStateError(
                    "probe namespace absence purpose lacks its root observation"
                )
            precreate_attempted = True
            precreate_succeeded = succeeded
            continue
        if purpose == "probe-precreate-absence":
            if precreate_attempted or root_observed or index != 0:
                raise OperationStateError(
                    "probe pre-create ownership purpose is duplicated or out of order"
                )
            precreate_attempted = True
            precreate_succeeded = succeeded
            continue
        if purpose == "probe-recovery-inventory":
            if not precreate_succeeded:
                raise OperationStateError(
                    "probe recovery inventory precedes pre-create ownership"
                )
            if recovery_namespace_stage != "parent":
                raise OperationStateError(
                    "probe recovery inventory purpose lacks a fresh candidate-presence proof"
                )
            recovery_namespace_stage = None
            if succeeded:
                for item_progress in progress.values():
                    item_progress["inventory_success_indices"].append(index)
            continue
        if purpose == "probe-owned-inventory":
            if (
                not precreate_succeeded
                or owned_inventory_succeeded
                or not all(
                    item_progress["create_success"]
                    for item_progress in progress.values()
                )
                or not all(item["created"] is True for item in objects)
            ):
                raise OperationStateError(
                    "probe owned inventory lacks both durable strict creates"
                )
            if succeeded:
                owned_inventory_succeeded = True
            continue
        if purpose in create_purposes:
            name = create_purposes[purpose]
            if not precreate_succeeded:
                raise OperationStateError(
                    "probe creation ownership purpose precedes pre-create proof"
                )
            if progress[name]["create_intent"]:
                raise OperationStateError(
                    "probe creation ownership purpose is duplicated"
                )
            progress[name]["create_intent"] = 1
            progress[name]["create_index"] = index
            if succeeded:
                progress[name]["create_success"] = 1
            else:
                progress[name]["create_indeterminate"] = 1
            continue
        if purpose in adopt_purposes:
            name = adopt_purposes[purpose]
            item_progress = progress[name]
            inventory_indices = item_progress["inventory_success_indices"]
            if (
                not precreate_succeeded
                or item_progress["create_intent"] != 1
                or not inventory_indices
                or inventory_indices[-1] <= item_progress["create_index"]
            ):
                raise OperationStateError(
                    "probe adopt purpose lacks ordered ambiguous-create evidence"
                )
            if succeeded:
                if item_progress["adopt_success"]:
                    raise OperationStateError(
                        "probe adopt success purpose is duplicated"
                    )
                item_progress["adopt_success"] = 1
            continue
        if purpose in verify_purposes:
            name = verify_purposes[purpose]
            item = object_by_name[name]
            item_progress = progress[name]
            if (
                not precreate_succeeded
                or not (
                    item_progress["create_success"]
                    or item_progress["adopt_success"]
                )
            ):
                raise OperationStateError(
                    "probe verification purpose precedes creation ownership"
                )
            if item["created"] is not True:
                raise OperationStateError(
                    "probe verification purpose lacks durable creation ownership"
                )
            if succeeded:
                item_progress["verify_success"] += 1
            continue
        if purpose in cleanup_purposes:
            name = cleanup_purposes[purpose]
            item = object_by_name[name]
            item_progress = progress[name]
            if (
                not precreate_succeeded
                or not (
                    item_progress["create_success"]
                    or item_progress["adopt_success"]
                )
                or not (
                    item_progress["verify_success"]
                    or item_progress["adopt_success"]
                )
            ):
                raise OperationStateError(
                    "probe cleanup purpose precedes verified creation ownership"
                )
            if item["created"] is not True or item["verified"] is not True:
                raise OperationStateError(
                    "probe cleanup purpose lacks durable verified ownership"
                )
            if succeeded:
                if item_progress["cleanup_success"]:
                    raise OperationStateError(
                        "probe cleanup success purpose is duplicated"
                    )
                item_progress["cleanup_success"] = 1
            else:
                item_progress["cleanup_indeterminate_indices"].append(index)
            continue
        raise OperationStateError("probe ownership purpose is unknown or unsafe")

    if any(item["created"] or item["verified"] or item["cleaned"] for item in objects):
        if not precreate_succeeded:
            raise OperationStateError("probe pre-create ownership purpose is missing")
    for name, item in object_by_name.items():
        item_progress = progress[name]
        if item["created"] and not (
            item_progress["create_success"] or item_progress["adopt_success"]
        ):
            raise OperationStateError(
                "durable probe creation lacks exact ownership purpose"
            )
        if item["verified"] and not (
            item_progress["verify_success"] or item_progress["adopt_success"]
        ):
            raise OperationStateError(
                "durable probe verification lacks ordered purpose evidence"
            )
        if item["cleaned"] and not item_progress["cleanup_success"]:
            ambiguous_cleanup_proven_absent = any(
                inventory_index > cleanup_index
                for cleanup_index in item_progress[
                    "cleanup_indeterminate_indices"
                ]
                for inventory_index in item_progress[
                    "inventory_success_indices"
                ]
            )
            if not ambiguous_cleanup_proven_absent:
                raise OperationStateError(
                    "durable probe cleanup lacks ordered purpose evidence"
                )


def _task8_require_normal_probe_success_sequence(
    events: list[tuple[str, str]],
) -> None:
    if len(events) != len(_TASK8_NORMAL_PROBE_TAIL_PURPOSES) + 2:
        raise OperationStateError(
            "completed probe receipt has an incomplete normal purpose sequence"
        )
    head = tuple(events[:2])
    if head not in {
        (
            ("probe-precreate-root", "success"),
            ("probe-precreate-root-absence", "success"),
        ),
        (
            ("probe-precreate-root", "success"),
            ("probe-precreate-parent-absence", "success"),
        ),
    }:
        raise OperationStateError(
            "completed probe receipt lacks a two-level namespace absence proof"
        )
    expected_tail = [
        (purpose, "success") for purpose in _TASK8_NORMAL_PROBE_TAIL_PURPOSES
    ]
    if events[2:] != expected_tail:
        raise OperationStateError(
            "completed probe receipt has an unsafe normal purpose order"
        )


def _task8_require_probe_purpose_chain(
    state: dict[str, object],
    *,
    next_purpose: str | None = None,
) -> None:
    events = _task8_probe_completed_events(state)
    if next_purpose is not None:
        _require_string(next_purpose, "next probe purpose", nonempty=True)
        events.append((next_purpose, "success"))
    _task8_require_probe_event_chain(_task8_probe_objects(state), events)


def _task8_probe_remote_cleanup_complete(state: dict[str, object]) -> bool:
    _task8_require_probe_purpose_chain(state)
    for item in _task8_probe_objects(state):
        if item["created"] and not item["cleaned"]:
            return False
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    if not groups:
        return False
    last_group = groups[-1]
    if (
        not isinstance(last_group, dict)
        or last_group["purpose"]
        not in {
            "probe-prefix-empty",
            "probe-recovery-root-absence",
            "probe-recovery-parent-absence",
        }
        or last_group["after"] is None
        or last_group.get("outcome") != "success"
        or last_group["evidence_sha256"] is None
    ):
        return False
    transaction = state.get("active_transaction")
    assert isinstance(transaction, dict)
    entries = _task8_attempt_entries(state, "probe")
    if not entries or entries[-1]["epoch"] != transaction["started_epoch"]:
        return False
    try:
        kind, started_epoch, attempt_ordinal, _group_ordinal = (
            _task8_group_identity(last_group)
        )
    except OperationStateError:
        return False
    return (
        kind == "probe"
        and started_epoch == transaction["started_epoch"]
        and attempt_ordinal == len(entries) - 1
    )


def _task8_complete_probe_recovery(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    if previous.get("phase") != "recovering_probe":
        raise OperationStateError("probe recovery completion phase is invalid")
    recovery = previous.get("recovery")
    if (
        not isinstance(recovery, dict)
        or recovery.get("kind") != "probe"
        or recovery.get("completed_epoch") is not None
    ):
        raise OperationStateError("probe recovery completion receipt is invalid")
    if not _task8_probe_remote_cleanup_complete(previous):
        raise OperationStateError("probe remote prefix cleanup is not proven")
    guard = _task8_guard_progress(previous)
    if not all(guard.values()):
        raise OperationStateError(
            "probe recovery cannot complete before exact runtime restoration"
        )
    epoch = _task7_state_epoch(context, previous)
    updated = copy.deepcopy(previous)
    updated_recovery = updated["recovery"]
    assert isinstance(updated_recovery, dict)
    updated_recovery["completed_epoch"] = epoch
    updated["active_transaction"] = None
    _task7_append_history(
        updated,
        "installed",
        epoch,
        "probe-recovery-complete",
        {
            "kind": "probe",
            "started_epoch": recovery["started_epoch"],
            "prefix_empty": True,
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "probe recovery completion",
    )
    return _task7_load_state(context)


def _task8_mark_probe_recovery_required(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
) -> dict[str, object] | None:
    state = _task7_load_state(context)
    if state["phase"] == "recovery_required":
        return state
    if state["phase"] != "recovering_probe":
        return None
    recovery = state.get("recovery")
    transaction = state.get("active_transaction")
    if (
        not isinstance(recovery, dict)
        or recovery.get("kind") != "probe"
        or not isinstance(transaction, dict)
        or transaction.get("kind") != "probe"
    ):
        raise OperationStateError("probe recovery-required state is invalid")
    epoch = _task7_state_epoch(context, state)
    updated = copy.deepcopy(state)
    _task7_append_history(
        updated,
        "recovery_required",
        epoch,
        "probe-recovery-required",
        {
            "kind": "probe",
            "started_epoch": recovery["started_epoch"],
            "probe": transaction["probe"],
            "guard": transaction["guard"],
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, state)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "probe recovery-required",
    )
    return _task7_load_state(context)


@contextlib.contextmanager
def _task8_open_exact_guard_recovery_material(
    context: OperationsContext,
    state: dict[str, object],
    *,
    helper_route: str = "source",
):
    phase = str(state["phase"])
    with _open_verified_transaction_material(
        context,
        frozenset({phase}),
        helper_route=helper_route,
    ) as material:
        if material.source.state != state:
            raise OperationStateError(
                "operation state changed before exact guard recovery proof"
            )
        _revalidate_transaction_material(material)
        yield material
        _revalidate_transaction_material(material)


def _task8_next_rclone_group_id(state: dict[str, object]) -> str:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict):
        raise OperationStateError("Task 8 rclone audit requires an active transaction")
    kind = str(transaction["kind"])
    started_epoch = int(transaction["started_epoch"])
    entries = _task8_attempt_entries(state, kind)
    if not entries or entries[-1]["epoch"] != started_epoch:
        raise OperationStateError("Task 8 rclone audit attempt identity is invalid")
    attempt_ordinal = len(entries) - 1
    group_ordinal = 0
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    for group in groups:
        assert isinstance(group, dict)
        if group["group_id"] == "install":
            continue
        group_kind, group_started, group_attempt, _group_ordinal = (
            _task8_group_identity(group)
        )
        if (
            group_kind == kind
            and group_started == started_epoch
            and group_attempt == attempt_ordinal
        ):
            group_ordinal += 1
    return (
        f"task8:{kind}:{started_epoch}:{attempt_ordinal}:{group_ordinal}"
    )


def _task8_begin_rclone_audit(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    purpose: str,
) -> dict[str, object]:
    _require_string(purpose, "Task 8 rclone purpose", nonempty=True)
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    if groups and isinstance(groups[-1], dict) and groups[-1]["after"] is None:
        raise OperationStateError(
            "a pending rclone audit must be completed before another command"
        )
    effective = state.get("effective_config")
    if (
        not isinstance(effective, dict)
        or effective.get("RCLONE_CONFIG") != _RCLONE_CONFIG_PATH
    ):
        raise OperationStateError("Task 8 rclone audit path is not fixed")
    group_id = _task8_next_rclone_group_id(state)
    pending = {
        "group_id": group_id,
        "purpose": purpose,
        "before": _task7_capture_file_audit(context, _RCLONE_CONFIG_PATH),
        "after": None,
        "outcome": None,
        "evidence_sha256": None,
    }
    if group_id.startswith(
        ("task8:dry_run:", "task8:policy:", "task8:observe:")
    ):
        pending["result_sha256"] = None
    _reject_residual_secrets(pending)
    updated = copy.deepcopy(state)
    updated_groups = updated["rclone_evidence_groups"]
    assert isinstance(updated_groups, list)
    updated_groups.append(pending)
    validate_operation_state(updated, context.paths.operation_dir, state)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "Task 8 pending rclone audit",
    )
    durable = _task7_load_state(context)
    state.clear()
    state.update(durable)
    return state


def _task8_complete_rclone_audit(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    outcome: str,
    result_sha256: str | None = None,
) -> dict[str, object]:
    if outcome not in _TASK8_RCLONE_OUTCOMES:
        raise OperationStateError("Task 8 rclone audit outcome is invalid")
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list) and groups
    pending = groups[-1]
    assert isinstance(pending, dict)
    if pending["after"] is not None:
        raise OperationStateError("Task 8 rclone audit is not pending")
    is_result_bearing = str(pending["group_id"]).startswith(
        ("task8:dry_run:", "task8:policy:", "task8:observe:")
    )
    if is_result_bearing and outcome == "success":
        _require_hash(result_sha256, "Task 8 rclone result_sha256")
    elif result_sha256 is not None:
        raise OperationStateError(
            "Task 8 rclone result digest is valid only for successful result-bearing groups"
        )
    payload = {
        "group_id": pending["group_id"],
        "purpose": pending["purpose"],
        "before": pending["before"],
        "after": _task7_capture_file_audit(context, _RCLONE_CONFIG_PATH),
        "outcome": outcome,
    }
    if is_result_bearing:
        payload["result_sha256"] = result_sha256
    completed = {
        **payload,
        "evidence_sha256": None,
    }
    completed["evidence_sha256"] = _task8_group_evidence_sha256(completed)
    updated = copy.deepcopy(state)
    updated_groups = updated["rclone_evidence_groups"]
    assert isinstance(updated_groups, list)
    updated_groups[-1] = completed
    validate_operation_state(updated, context.paths.operation_dir, state)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "Task 8 completed rclone audit",
    )
    durable = _task7_load_state(context)
    state.clear()
    state.update(durable)
    return state


def _task8_run_audited_rclone_command(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    *,
    purpose: str,
    argv: tuple[str, ...],
    label: str,
    runtime_lock_fd: int,
    helper_route: str = "source",
) -> object:
    if type(runtime_lock_fd) is not int or runtime_lock_fd < 0:
        raise OperationStateError(
            "Task 8 rclone command requires the owned runtime lock descriptor"
        )
    _task8_validate_probe_recovery_purpose_argv(
        state,
        purpose,
        argv,
    )
    with _task8_open_exact_guard_recovery_material(
        context,
        state,
        helper_route=helper_route,
    ):
        _task8_begin_rclone_audit(
            context,
            binding,
            state,
            purpose,
        )
    completed: subprocess.CompletedProcess[str] | None = None
    command_error: Exception | None = None
    validated_output: object = None
    with _task8_open_exact_guard_recovery_material(
        context,
        state,
        helper_route=helper_route,
    ):
        try:
            completed = _checked_command(
                context,
                argv,
                (runtime_lock_fd,),
                label,
            )
            if completed.stderr:
                command_error = OperationStateError(
                    f"{label} returned unexpected stderr"
                )
            else:
                validated_output = _task8_validate_probe_recovery_output(
                    state,
                    purpose,
                    completed.stdout,
                )
        except Exception as exc:
            command_error = exc
        except BaseException:
            if completed is not None:
                _scrub_completed_process(completed)
            raise
        try:
            _task8_complete_rclone_audit(
                context,
                binding,
                state,
                outcome=(
                    "success" if command_error is None else "indeterminate"
                ),
            )
        except BaseException as audit_error:
            if completed is not None:
                _scrub_completed_process(completed)
            if not isinstance(audit_error, Exception):
                raise
            if command_error is not None:
                raise _Task7RcloneAuditIncompleteError(
                    command_error,
                    audit_error,
                ) from audit_error
            raise
    if command_error is not None:
        if completed is not None:
            _scrub_completed_process(completed)
        raise command_error
    if completed is None:
        raise OperationStateError(f"{label} returned no command result")
    _scrub_completed_process(completed)
    return validated_output


def _task8_decode_strict_json_list(raw: str, label: str) -> list[object]:
    if type(raw) is not str:
        raise OperationStateError(f"{label} is not text")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except OperationStateError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise OperationStateError(f"{label} is not strict JSON") from exc
    if type(value) is not list:
        raise OperationStateError(f"{label} must be a JSON list")
    if len(value) > _MAX_BACKUP_ENTRIES:
        raise OperationStateError(f"{label} exceeds the entry limit")
    return value


def _task8_decode_probe_namespace_entries(
    raw: str,
    label: str,
) -> list[tuple[str, bool]]:
    value = _task8_decode_strict_json_list(raw, label)
    observed: list[tuple[str, bool]] = []
    casefold_names: set[str] = set()
    for index, entry_value in enumerate(value):
        if type(entry_value) is not dict:
            raise OperationStateError(f"{label} entry is invalid")
        entry = entry_value
        for field in ("Path", "Name", "Size", "IsDir"):
            if field not in entry:
                raise OperationStateError(
                    f"{label} entry {index} is missing {field}"
                )
        path = entry["Path"]
        name = entry["Name"]
        size = entry["Size"]
        is_dir = entry["IsDir"]
        if type(path) is not str or type(name) is not str or path != name:
            raise OperationStateError(f"{label} path/name is unsafe")
        try:
            encoded = name.encode("ascii")
        except UnicodeEncodeError as exc:
            raise OperationStateError(f"{label} name is not ASCII") from exc
        if (
            not encoded
            or name in {".", ".."}
            or "/" in name
            or "\\" in name
            or any(byte < 0x21 or byte > 0x7E for byte in encoded)
        ):
            raise OperationStateError(f"{label} name is unsafe")
        if type(is_dir) is not bool or type(size) is not int or size < -1:
            raise OperationStateError(f"{label} metadata is invalid")
        if (is_dir and size != -1) or (not is_dir and size < 0):
            raise OperationStateError(f"{label} type and size disagree")
        folded = name.casefold()
        if folded in casefold_names:
            raise OperationStateError(
                f"{label} contains duplicate or case-colliding names"
            )
        casefold_names.add(folded)
        observed.append((name, is_dir))
    return observed


def _task8_decode_probe_namespace_root(raw: str) -> bool:
    parent_seen = False
    for name, is_dir in _task8_decode_probe_namespace_entries(
        raw,
        "remote probe namespace root inventory",
    ):
        if name.casefold() != _PROBE_NAMESPACE_PARENT.casefold():
            continue
        if name != _PROBE_NAMESPACE_PARENT or not is_dir:
            raise OperationStateError(
                "remote probe namespace parent case-collides or is not a directory"
            )
        parent_seen = True
    return parent_seen


def _task8_probe_namespace_identity(prefix: str) -> str:
    validated = _validate_probe_prefix(prefix, "remote probe prefix")
    marker = _PROBE_NAMESPACE_ROOT + _PROBE_NAMESPACE_PARENT + "/"
    if not validated.startswith(marker):
        raise OperationStateError("remote probe prefix is outside its fixed namespace")
    identity = validated[len(marker) : -1]
    if not identity or "/" in identity or "\\" in identity:
        raise OperationStateError("remote probe namespace identity is invalid")
    return identity


def _task8_decode_probe_namespace_parent_presence(
    raw: str,
    prefix: str,
) -> bool:
    identity = _task8_probe_namespace_identity(prefix)
    candidate_seen = False
    for name, is_dir in _task8_decode_probe_namespace_entries(
        raw,
        "remote probe namespace parent inventory",
    ):
        if name.casefold() == identity.casefold():
            if name != identity or not is_dir:
                raise OperationStateError(
                    "remote probe namespace case-collides or is not a directory"
                )
            candidate_seen = True
    return candidate_seen


def _task8_decode_probe_namespace_parent(raw: str, prefix: str) -> None:
    if _task8_decode_probe_namespace_parent_presence(raw, prefix):
        raise OperationStateError(
            "remote probe namespace already exists or case-collides"
        )


def _task8_decode_probe_inventory(
    raw: str,
    expected: dict[str, dict[str, object]],
) -> dict[str, int]:
    value = _task8_decode_strict_json_list(raw, "remote probe inventory")
    observed: dict[str, int] = {}
    casefold_names: set[str] = set()
    for index, entry_value in enumerate(value):
        if type(entry_value) is not dict:
            raise OperationStateError("remote probe inventory entry is invalid")
        entry = entry_value
        for field in ("Path", "Name", "Size", "IsDir"):
            if field not in entry:
                raise OperationStateError(
                    f"remote probe inventory entry {index} is missing {field}"
                )
        path = entry["Path"]
        name = entry["Name"]
        size = entry["Size"]
        is_dir = entry["IsDir"]
        if type(path) is not str or type(name) is not str or path != name:
            raise OperationStateError("remote probe object path/name is unsafe")
        try:
            encoded_name = name.encode("ascii")
        except UnicodeEncodeError as exc:
            raise OperationStateError("remote probe object name is not ASCII") from exc
        if (
            not encoded_name
            or name in {".", ".."}
            or "/" in name
            or "\\" in name
            or any(byte < 0x21 or byte > 0x7E for byte in encoded_name)
        ):
            raise OperationStateError("remote probe object name is nested or unsafe")
        if type(is_dir) is not bool or is_dir:
            raise OperationStateError("remote probe prefix contains a directory")
        if type(size) is not int or size < 0:
            raise OperationStateError("remote probe object size is invalid")
        folded = name.casefold()
        if name in observed or folded in casefold_names:
            raise OperationStateError(
                "remote probe prefix contains duplicate or case-colliding objects"
            )
        if name not in expected:
            raise OperationStateError("remote probe prefix contains an untracked object")
        if size != expected[name]["expected_size"]:
            raise OperationStateError("remote probe object size differs from state")
        observed[name] = size
        casefold_names.add(folded)
    return observed


def _task8_require_probe_hashsum(
    raw: str,
    *,
    name: str,
    expected_sha256: str,
) -> None:
    pattern = re.compile(
        r"\A(?P<digest>[0-9a-f]{64})  (?P<name>[^\r\n]+)\n\Z",
        re.ASCII,
    )
    match = pattern.fullmatch(raw)
    if (
        match is None
        or match.group("name") != name
        or match.group("digest") != expected_sha256
    ):
        raise OperationStateError("remote probe object SHA-256 differs from state")


def _task8_probe_rclone_argv(
    command: str,
    target: str,
    *extra: str,
) -> tuple[str, ...]:
    return (
        _RCLONE_EXECUTABLE,
        "--config",
        _RCLONE_CONFIG_PATH,
        command,
        target,
        *extra,
    )


def _task8_probe_create_rclone_argv(
    source: str,
    prefix: str,
    name: str,
) -> tuple[str, ...]:
    return (
        _RCLONE_EXECUTABLE,
        "--config",
        _RCLONE_CONFIG_PATH,
        "--ignore-existing",
        "--error-on-no-transfer",
        "copyto",
        source,
        f"{prefix}{name}",
    )


def _task8_require_normal_probe_next_purpose(
    state: dict[str, object],
    purpose: str,
) -> None:
    events = _task8_probe_completed_events(state)
    if any(outcome != "success" for _purpose, outcome in events):
        raise OperationStateError(
            "normal probe cannot continue after an indeterminate rclone result"
        )
    purposes = [item_purpose for item_purpose, _outcome in events]
    if not purposes:
        expected = {"probe-precreate-root"}
    elif purposes == ["probe-precreate-root"]:
        expected = {
            "probe-precreate-root-absence",
            "probe-precreate-parent-absence",
        }
    else:
        if len(purposes) < 2 or purposes[:2] not in [
            ["probe-precreate-root", "probe-precreate-root-absence"],
            ["probe-precreate-root", "probe-precreate-parent-absence"],
        ]:
            raise OperationStateError(
                "normal probe namespace absence sequence is invalid"
            )
        tail_index = len(purposes) - 2
        if tail_index >= len(_TASK8_NORMAL_PROBE_TAIL_PURPOSES):
            expected = set()
        else:
            if purposes[2:] != list(
                _TASK8_NORMAL_PROBE_TAIL_PURPOSES[:tail_index]
            ):
                raise OperationStateError(
                    "normal probe purpose order is invalid"
                )
            expected = {_TASK8_NORMAL_PROBE_TAIL_PURPOSES[tail_index]}
    if purpose not in expected:
        raise OperationStateError(
            "normal probe purpose is duplicated or out of order"
        )


def _task8_validate_probe_recovery_purpose_argv(
    state: dict[str, object],
    purpose: str,
    argv: tuple[str, ...],
) -> None:
    phase = state.get("phase")
    if phase not in {"probing", "recovering_probe"}:
        raise OperationStateError(
            "probe command binding requires probing or recovering_probe"
        )
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        raise OperationStateError(
            "probe command binding requires an active probe transaction"
        )
    probe = transaction.get("probe")
    if not isinstance(probe, dict):
        raise OperationStateError("probe command binding is missing identity")
    prefix = _validate_probe_prefix(
        probe["prefix"],
        "active_transaction.probe.prefix",
        operation_id=str(state["operation_id"]),
    )
    expected_by_purpose: dict[str, tuple[str, ...]] = {
        "probe-prefix-empty": _task8_probe_rclone_argv(
            "lsjson",
            prefix,
            "--recursive",
        ),
    }
    for item in _task8_probe_objects(state):
        name = str(item["name"])
        expected_by_purpose[f"probe-verify:{name}"] = _task8_probe_rclone_argv(
            "hashsum",
            "SHA-256",
            f"{prefix}{name}",
            "--download",
        )
        expected_by_purpose[f"probe-adopt:{name}"] = _task8_probe_rclone_argv(
            "hashsum",
            "SHA-256",
            f"{prefix}{name}",
            "--download",
        )
        expected_by_purpose[f"probe-cleanup:{name}"] = _task8_probe_rclone_argv(
            "deletefile",
            f"{prefix}{name}",
        )
    if phase == "probing":
        expected_by_purpose.update(
            {
                "probe-precreate-root": _task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT,
                ),
                "probe-precreate-root-absence": _task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT,
                ),
                "probe-precreate-parent-absence": _task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT + _PROBE_NAMESPACE_PARENT + "/",
                ),
                "probe-owned-inventory": _task8_probe_rclone_argv(
                    "lsjson",
                    prefix,
                    "--recursive",
                ),
            }
        )
        for item in _task8_probe_objects(state):
            name = str(item["name"])
            create_purpose = f"probe-create:{name}:strict-no-existing"
            if purpose == create_purpose:
                source = str(_task8_probe_source_path(state, name))
                expected_by_purpose[create_purpose] = (
                    _task8_probe_create_rclone_argv(source, prefix, name)
                )
        _task8_require_normal_probe_next_purpose(state, purpose)
    else:
        expected_by_purpose.update(
            {
                "probe-recovery-root": _task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT,
                ),
                "probe-recovery-root-absence": _task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT,
                ),
                "probe-recovery-parent": _task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT + _PROBE_NAMESPACE_PARENT + "/",
                ),
                "probe-recovery-parent-absence": _task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT + _PROBE_NAMESPACE_PARENT + "/",
                ),
                "probe-recovery-inventory": _task8_probe_rclone_argv(
                    "lsjson",
                    prefix,
                    "--recursive",
                ),
            }
        )
        allowed_recovery = {
            "probe-recovery-root",
            "probe-recovery-root-absence",
            "probe-recovery-parent",
            "probe-recovery-parent-absence",
            "probe-recovery-inventory",
            "probe-prefix-empty",
            *(f"probe-verify:{item['name']}" for item in _task8_probe_objects(state)),
            *(f"probe-adopt:{item['name']}" for item in _task8_probe_objects(state)),
            *(f"probe-cleanup:{item['name']}" for item in _task8_probe_objects(state)),
        }
        if purpose not in allowed_recovery:
            expected_by_purpose.pop(purpose, None)
    expected = expected_by_purpose.get(purpose)
    if expected is None:
        raise OperationStateError(
            "probe purpose is not bound to an allowed command"
        )
    if type(argv) is not tuple or argv != expected:
        raise OperationStateError(
            "probe purpose and argv target binding differ"
        )
    if phase == "recovering_probe":
        _task8_require_probe_purpose_chain(state, next_purpose=purpose)


def _task8_validate_probe_recovery_output(
    state: dict[str, object],
    purpose: str,
    raw: str,
) -> object:
    objects = _task8_probe_objects(state)
    expected = {str(item["name"]): item for item in objects}
    if purpose == "probe-precreate-root":
        return _task8_decode_probe_namespace_root(raw)
    if purpose == "probe-precreate-root-absence":
        if _task8_decode_probe_namespace_root(raw):
            raise OperationStateError(
                "remote probe namespace parent appeared before creation"
            )
        return None
    if purpose == "probe-precreate-parent-absence":
        transaction = state.get("active_transaction")
        assert isinstance(transaction, dict)
        probe = transaction.get("probe")
        assert isinstance(probe, dict)
        _task8_decode_probe_namespace_parent(raw, str(probe["prefix"]))
        return None
    if purpose == "probe-recovery-root":
        return _task8_decode_probe_namespace_root(raw)
    if purpose == "probe-recovery-root-absence":
        if _task8_decode_probe_namespace_root(raw):
            raise OperationStateError(
                "remote probe namespace parent appeared during recovery"
            )
        return None
    if purpose in {
        "probe-recovery-parent",
        "probe-recovery-parent-absence",
    }:
        transaction = state.get("active_transaction")
        assert isinstance(transaction, dict)
        probe = transaction.get("probe")
        assert isinstance(probe, dict)
        present = _task8_decode_probe_namespace_parent_presence(
            raw,
            str(probe["prefix"]),
        )
        if purpose == "probe-recovery-parent-absence":
            if present:
                raise OperationStateError(
                    "remote probe candidate appeared during recovery"
                )
            return None
        return present
    if purpose in {
        "probe-recovery-inventory",
        "probe-owned-inventory",
        "probe-prefix-empty",
    }:
        observed = _task8_decode_probe_inventory(raw, expected)
        if purpose == "probe-prefix-empty" and observed:
            raise OperationStateError("remote probe prefix is not empty")
        if purpose == "probe-owned-inventory" and set(observed) != set(expected):
            raise OperationStateError(
                "remote probe owned inventory is incomplete"
            )
        return observed
    for name, item in expected.items():
        if purpose == f"probe-create:{name}:strict-no-existing":
            if raw:
                raise OperationStateError(
                    "remote probe create returned unexpected output"
                )
            return None
        if purpose in {f"probe-verify:{name}", f"probe-adopt:{name}"}:
            _task8_require_probe_hashsum(
                raw,
                name=name,
                expected_sha256=str(item["expected_sha256"]),
            )
            return None
        if purpose == f"probe-cleanup:{name}":
            if raw:
                raise OperationStateError(
                    "remote probe delete returned unexpected output"
                )
            return None
    raise OperationStateError("probe recovery output purpose is unsupported")


def _task8_run_normal_probe_remote_objects(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    runtime_lock_fd: int,
) -> dict[str, object]:
    if state.get("phase") != "probing":
        raise OperationStateError("normal remote probe requires probing state")
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        raise OperationStateError("normal remote probe transaction is invalid")
    probe = transaction.get("probe")
    if not isinstance(probe, dict):
        raise OperationStateError("normal remote probe identity is missing")
    prefix = _validate_probe_prefix(
        probe["prefix"],
        "active_transaction.probe.prefix",
        operation_id=context.operation_id,
    )
    material = {
        str(item["name"]): item
        for item in _task8_derive_probe_material(context.operation_id, prefix)
    }
    objects = _task8_probe_objects(state)
    if [str(item["name"]) for item in objects] != list(material):
        raise OperationStateError("normal remote probe objects are out of order")

    parent_exists = _task8_run_audited_rclone_command(
        context,
        binding,
        state,
        purpose="probe-precreate-root",
        argv=_task8_probe_rclone_argv(
            "lsjson",
            _PROBE_NAMESPACE_ROOT,
        ),
        label="remote probe namespace root inventory",
        runtime_lock_fd=runtime_lock_fd,
        helper_route="installed",
    )
    if type(parent_exists) is not bool:
        raise OperationStateError("remote probe namespace result is invalid")
    if parent_exists:
        absence_purpose = "probe-precreate-parent-absence"
        absence_target = _PROBE_NAMESPACE_ROOT + _PROBE_NAMESPACE_PARENT + "/"
    else:
        absence_purpose = "probe-precreate-root-absence"
        absence_target = _PROBE_NAMESPACE_ROOT
    _task8_run_audited_rclone_command(
        context,
        binding,
        state,
        purpose=absence_purpose,
        argv=_task8_probe_rclone_argv("lsjson", absence_target),
        label="remote probe pre-create namespace absence proof",
        runtime_lock_fd=runtime_lock_fd,
        helper_route="installed",
    )

    for item in _task8_probe_objects(state):
        name = str(item["name"])
        contents = material[name]["contents"]
        assert isinstance(contents, bytes)
        _task8_force_checkpoint(
            context,
            binding,
            state,
            f"probe_source_create:{name}",
        )
        with _task8_open_probe_source(
            context,
            state,
            name,
            contents,
        ) as source:
            _task8_run_audited_rclone_command(
                context,
                binding,
                state,
                purpose=f"probe-create:{name}:strict-no-existing",
                argv=_task8_probe_create_rclone_argv(str(source), prefix, name),
                label=f"remote probe strict create: {name}",
                runtime_lock_fd=runtime_lock_fd,
                helper_route="installed",
            )
        with _task8_open_exact_guard_recovery_material(
            context,
            state,
            helper_route="installed",
        ):
            _task8_advance_probe_object(
                context,
                binding,
                state,
                name,
                "created",
            )

    owned = _task8_run_audited_rclone_command(
        context,
        binding,
        state,
        purpose="probe-owned-inventory",
        argv=_task8_probe_rclone_argv(
            "lsjson",
            prefix,
            "--recursive",
        ),
        label="remote probe owned-prefix inventory",
        runtime_lock_fd=runtime_lock_fd,
        helper_route="installed",
    )
    if not isinstance(owned, dict) or set(owned) != set(material):
        raise OperationStateError("remote probe owned-prefix inventory is incomplete")

    for item in _task8_probe_objects(state):
        name = str(item["name"])
        _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose=f"probe-verify:{name}",
            argv=_task8_probe_rclone_argv(
                "hashsum",
                "SHA-256",
                f"{prefix}{name}",
                "--download",
            ),
            label=f"remote probe object hash verification: {name}",
            runtime_lock_fd=runtime_lock_fd,
            helper_route="installed",
        )
        with _task8_open_exact_guard_recovery_material(
            context,
            state,
            helper_route="installed",
        ):
            _task8_advance_probe_object(
                context,
                binding,
                state,
                name,
                "verified",
            )

    for item in _task8_probe_objects(state):
        name = str(item["name"])
        _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose=f"probe-cleanup:{name}",
            argv=_task8_probe_rclone_argv("deletefile", f"{prefix}{name}"),
            label=f"remote probe object cleanup: {name}",
            runtime_lock_fd=runtime_lock_fd,
            helper_route="installed",
        )
        with _task8_open_exact_guard_recovery_material(
            context,
            state,
            helper_route="installed",
        ):
            _task8_advance_probe_object(
                context,
                binding,
                state,
                name,
                "cleaned",
            )

    final = _task8_run_audited_rclone_command(
        context,
        binding,
        state,
        purpose="probe-prefix-empty",
        argv=_task8_probe_rclone_argv(
            "lsjson",
            prefix,
            "--recursive",
        ),
        label="remote probe final empty-prefix proof",
        runtime_lock_fd=runtime_lock_fd,
        helper_route="installed",
    )
    if not isinstance(final, dict) or final:
        raise OperationStateError("remote probe prefix is not empty after cleanup")
    if not _task8_probe_remote_cleanup_complete(state):
        raise OperationStateError("normal remote probe cleanup is incomplete")
    return state


def _task8_current_probe_groups(
    state: dict[str, object],
) -> list[dict[str, object]]:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        raise OperationStateError("probe group selection requires an active probe")
    entries = _task8_attempt_entries(state, "probe")
    if not entries or entries[-1]["epoch"] != transaction["started_epoch"]:
        raise OperationStateError("probe group attempt identity is invalid")
    attempt_ordinal = len(entries) - 1
    selected: list[tuple[int, dict[str, object]]] = []
    groups = state.get("rclone_evidence_groups")
    if not isinstance(groups, list):
        raise OperationStateError("probe rclone evidence stream is invalid")
    for group in groups:
        assert isinstance(group, dict)
        if group["group_id"] == "install":
            continue
        kind, started_epoch, group_attempt, group_ordinal = (
            _task8_group_identity(group)
        )
        if (
            kind == "probe"
            and started_epoch == transaction["started_epoch"]
            and group_attempt == attempt_ordinal
        ):
            if group["after"] is None or group.get("outcome") != "success":
                raise OperationStateError(
                    "successful probe completion requires all-success rclone evidence"
                )
            selected.append((group_ordinal, group))
    selected.sort(key=lambda item: item[0])
    if [ordinal for ordinal, _group in selected] != list(range(len(selected))):
        raise OperationStateError("probe rclone group order is incomplete")
    return [group for _ordinal, group in selected]


def _task8_complete_normal_probe(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    if previous.get("phase") != "probing":
        raise OperationStateError("normal probe completion phase is invalid")
    if not _task8_probe_remote_cleanup_complete(previous):
        raise OperationStateError("normal probe prefix cleanup is not proven")
    guard = _task8_guard_progress(previous)
    if not all(guard.values()):
        raise OperationStateError(
            "normal probe cannot complete before exact runtime restoration"
        )
    events = _task8_probe_completed_events(previous)
    _task8_require_normal_probe_success_sequence(events)
    groups = _task8_current_probe_groups(previous)
    transaction = previous.get("active_transaction")
    assert isinstance(transaction, dict)
    probe_transaction = transaction.get("probe")
    assert isinstance(probe_transaction, dict)
    objects = _task8_probe_objects(previous)
    epoch = _task7_state_epoch(context, previous)
    updated = copy.deepcopy(previous)
    receipt: dict[str, object] = {
        "prefix": probe_transaction["prefix"],
        "owned_names": [str(item["name"]) for item in objects],
        "cleanup_proven": True,
        "evidence_sha256": "0" * 64,
    }
    updated["probe"] = receipt
    updated["active_transaction"] = None
    _task7_append_history(
        updated,
        "probed",
        epoch,
        "probe-complete-placeholder",
        {"operation_id": context.operation_id},
    )
    entries = _task8_attempt_entries(previous, "probe")
    entry = entries[-1]
    history = updated["phase_history"]
    assert isinstance(history, list)
    completed = history[-1]
    assert isinstance(completed, dict)
    evidence = _task8_probe_completion_evidence_sha256(
        updated,
        entry,
        completed,
        receipt,
        groups,
    )
    receipt["evidence_sha256"] = evidence
    completed["evidence_sha256"] = evidence
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "normal probe completion",
    )
    return _task7_load_state(context)


def _task8_complete_normal_dry_run(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
    *,
    inventory_names: list[str],
    casefold_names: list[str],
    plan: dict[str, list[str]],
) -> dict[str, object]:
    if previous.get("phase") != "dry_run_recording":
        raise OperationStateError("normal dry-run completion phase is invalid")
    transaction = previous.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "dry_run":
        raise OperationStateError("normal dry-run transaction identity is invalid")
    guard = _task8_guard_progress(previous)
    if not all(guard.values()):
        raise OperationStateError(
            "normal dry-run cannot complete before exact runtime restoration"
        )
    validated_inventory = _task8_validate_canonical_remote_names(
        inventory_names,
        "normal dry-run inventory",
        require_sorted=True,
    )
    if casefold_names != [name.casefold() for name in validated_inventory]:
        raise OperationStateError("normal dry-run casefold inventory is invalid")
    for field in ("keep_names", "protected_names", "delete_names"):
        if field not in plan:
            raise OperationStateError("normal dry-run plan is incomplete")
    delete_names = list(plan["delete_names"])
    receipt: dict[str, object] = {
        "inventory_names": list(validated_inventory),
        "casefold_names": list(casefold_names),
        "keep_names": list(plan["keep_names"]),
        "protected_names": list(plan["protected_names"]),
        "delete_names": delete_names,
        "candidate_sha256": _task8_remote_dry_run_candidate_sha256(
            delete_names
        ),
        "evidence_sha256": "0" * 64,
    }
    updated = copy.deepcopy(previous)
    updated["dry_run"] = receipt
    updated["active_transaction"] = None
    epoch = _task7_state_epoch(context, previous)
    _task7_append_history(
        updated,
        "dry_run_recorded",
        epoch,
        "dry-run-complete-placeholder",
        {"operation_id": context.operation_id},
    )
    entries = _task8_attempt_entries(updated, "dry_run")
    if not entries:
        raise OperationStateError("normal dry-run completion entry is missing")
    entry = entries[-1]
    history = updated["phase_history"]
    assert isinstance(history, list)
    completed = history[-1]
    assert isinstance(completed, dict)
    evidence = _task8_dry_run_completion_evidence_sha256(
        updated,
        entry,
        completed,
        receipt,
    )
    receipt["evidence_sha256"] = evidence
    completed["evidence_sha256"] = evidence
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "normal dry-run completion",
    )
    return _task7_load_state(context)


def _task8_complete_normal_observation(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
    *,
    run_epoch: int,
    journal_sha256: str,
    local_sha256: str,
    remote_sha256: str,
    pre_replace_validator: PreReplaceValidator | None = None,
) -> dict[str, object]:
    if previous.get("phase") != "observing":
        raise OperationStateError("normal observation completion phase is invalid")
    transaction = previous.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "observe":
        raise OperationStateError("normal observation transaction identity is invalid")
    if not all(_task8_guard_progress(previous).values()):
        raise OperationStateError(
            "normal observation cannot complete before exact runtime restoration"
        )
    if type(run_epoch) is not int:
        raise OperationStateError("normal observation run epoch is invalid")
    for label, value in (
        ("journal", journal_sha256),
        ("local", local_sha256),
        ("remote", remote_sha256),
    ):
        _require_hash(value, f"normal observation {label} digest")
    entries = _task8_attempt_entries(previous, "observe")
    if not entries or entries[-1]["epoch"] != transaction["started_epoch"]:
        raise OperationStateError("normal observation entry identity is invalid")
    entry = entries[-1]
    groups = _task8_require_completed_observation_groups(previous, entry)
    updated = copy.deepcopy(previous)
    receipt: dict[str, object] = {
        "run_epoch": run_epoch,
        "journal_sha256": journal_sha256,
        "local_sha256": local_sha256,
        "remote_sha256": remote_sha256,
        "evidence_sha256": "0" * 64,
    }
    updated["observation"] = receipt
    updated["active_transaction"] = None
    epoch = max(_task7_state_epoch(context, previous), run_epoch)
    _task7_append_history(
        updated,
        "observed",
        epoch,
        "observation-complete-placeholder",
        {"operation_id": context.operation_id},
    )
    history = updated["phase_history"]
    assert isinstance(history, list)
    completed = history[-1]
    assert isinstance(completed, dict)
    evidence = _task8_observation_completion_evidence_sha256(
        updated,
        entry,
        completed,
        receipt,
        groups,
    )
    receipt["evidence_sha256"] = evidence
    completed["evidence_sha256"] = evidence
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "normal observation completion",
        pre_replace_validator=pre_replace_validator,
    )
    return _task7_load_state(context)


def _task8_record_applied_policy(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
    *,
    applied_target: dict[str, object],
    pre_replace_validator: PreReplaceValidator,
) -> dict[str, object]:
    if previous.get("phase") != "policy_enabling" or previous.get("policy") is not None:
        raise OperationStateError("applied policy receipt phase is invalid")
    transaction = previous.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "policy":
        raise OperationStateError("applied policy transaction identity is invalid")
    required_guard = {
        "timer_stopped": True,
        "service_inactive_verified": True,
        "legacy_lock_acquired": True,
        "runtime_lock_acquired": True,
        "locks_released": False,
        "timer_restored": False,
    }
    if transaction.get("guard") != required_guard:
        raise OperationStateError(
            "applied policy receipt requires stopped runtime and both held locks"
        )
    entries = _task8_attempt_entries(previous, "policy")
    if not entries or entries[-1]["epoch"] != transaction["started_epoch"]:
        raise OperationStateError("applied policy entry identity is invalid")
    entry = entries[-1]
    group = _task8_require_completed_policy_inventory_group(previous, entry)
    enabled_epoch = _task7_state_epoch(context, previous)
    policy: dict[str, object] = {
        "environment_sha256": transaction["policy_environment_sha256"],
        "enabled_epoch": enabled_epoch,
        "runtime_baseline_sha256": _task8_policy_runtime_baseline_sha256(
            transaction["runtime_baseline"]
        ),
        "applied_target": copy.deepcopy(applied_target),
        "applied_evidence_sha256": "0" * 64,
    }
    updated = copy.deepcopy(previous)
    updated["policy"] = policy
    policy["applied_evidence_sha256"] = _task8_policy_applied_evidence_sha256(
        updated,
        entry,
        policy,
        group,
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "applied policy receipt",
        pre_replace_validator=pre_replace_validator,
    )
    _atomic_event_hook("task8_policy_after_applied_receipt")
    return _task7_load_state(context)


def _task8_complete_enabled_policy(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
    *,
    pre_replace_validator: PreReplaceValidator,
) -> dict[str, object]:
    if previous.get("phase") not in {"policy_enabling", "recovering_policy"}:
        raise OperationStateError("policy completion phase is invalid")
    transaction = previous.get("active_transaction")
    policy = previous.get("policy")
    if (
        not isinstance(transaction, dict)
        or transaction.get("kind") != "policy"
        or not isinstance(policy, dict)
    ):
        raise OperationStateError("policy completion identity is incomplete")
    guard = transaction.get("guard")
    if not isinstance(guard, dict) or not all(guard.values()):
        raise OperationStateError(
            "policy cannot complete before exact runtime restoration"
        )
    updated = copy.deepcopy(previous)
    epoch = max(
        _task7_state_epoch(context, previous),
        int(policy["enabled_epoch"]),
    )
    if previous["phase"] == "recovering_policy":
        recovery = updated.get("recovery")
        if not isinstance(recovery, dict) or recovery.get("kind") != "policy":
            raise OperationStateError("committed policy recovery receipt is missing")
        cursor = (
            recovery["current_target"],
            recovery["previous_sha256"],
            recovery["intended_sha256"],
        )
        if recovery["next_target_index"] != 1 or any(
            value is not None for value in cursor
        ):
            raise OperationStateError(
                "committed policy recovery cursor is not terminal"
            )
        recovery["completed_epoch"] = epoch
    updated["active_transaction"] = None
    _task7_append_history(
        updated,
        "policy_enabled",
        epoch,
        "policy-complete-placeholder",
        {"operation_id": context.operation_id},
    )
    entries = _task8_attempt_entries(updated, "policy")
    if not entries:
        raise OperationStateError("policy completion entry is missing")
    entry = entries[-1]
    group = _task8_require_completed_policy_inventory_group(updated, entry)
    history = updated["phase_history"]
    assert isinstance(history, list)
    completed = history[-1]
    assert isinstance(completed, dict)
    completed["evidence_sha256"] = _task8_policy_completion_evidence_sha256(
        updated,
        entry,
        completed,
        policy,
        group,
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "policy completion",
        pre_replace_validator=pre_replace_validator,
    )
    _atomic_event_hook("task8_policy_after_completion")
    return _task7_load_state(context)


def _task8_build_policy_recovery_entry(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
    *,
    live_receipt: dict[str, object],
) -> dict[str, object]:
    if previous.get("phase") != "policy_enabling":
        raise OperationStateError("policy recovery entry phase is invalid")
    transaction = previous.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "policy":
        raise OperationStateError("policy recovery transaction identity is invalid")
    prior_recovery = previous.get("recovery")
    if prior_recovery is not None and (
        not isinstance(prior_recovery, dict)
        or prior_recovery.get("completed_epoch") is None
    ):
        raise OperationStateError("an incomplete recovery attempt cannot be replaced")
    disabled_sha256 = _target_staged_hash(previous, _TARGET_ORDER[-1])
    enabled_sha256 = transaction.get("policy_environment_sha256")
    _require_hash(enabled_sha256, "policy recovery enabled environment digest")
    committed = isinstance(previous.get("policy"), dict)
    allowed = {enabled_sha256} if committed else {disabled_sha256, enabled_sha256}
    live_sha256 = live_receipt.get("sha256")
    if live_sha256 not in allowed:
        raise OperationStateError(
            "policy recovery live environment lacks authorized provenance"
        )
    intended_sha256 = enabled_sha256 if committed else disabled_sha256
    epoch = _task7_state_epoch(context, previous)
    policy = previous.get("policy")
    if isinstance(policy, dict):
        epoch = max(epoch, int(policy["enabled_epoch"]))
    payload = {
        "kind": "policy",
        "operation_id": context.operation_id,
        "transaction_started_epoch": transaction["started_epoch"],
        "committed": committed,
        "live_sha256": live_sha256,
        "intended_sha256": intended_sha256,
        "runtime_baseline": transaction["runtime_baseline"],
        "policy_applied_evidence_sha256": (
            previous["policy"]["applied_evidence_sha256"]
            if committed
            else None
        ),
    }
    updated = copy.deepcopy(previous)
    updated["recovery"] = {
        "kind": "policy",
        "next_target_index": 0,
        "current_target": _TARGET_ORDER[-1],
        "previous_sha256": live_sha256,
        "intended_sha256": intended_sha256,
        "started_epoch": epoch,
        "completed_epoch": None,
        "evidence_sha256": _task7_evidence_sha256(
            "policy-recovery-start",
            payload,
        ),
        "runtime_directory_created": False,
        "runtime_baseline": copy.deepcopy(transaction["runtime_baseline"]),
        "restored_epoch": None,
        "restore_evidence_sha256": None,
    }
    _task7_append_history(
        updated,
        "recovering_policy",
        epoch,
        "policy-recovery-start",
        payload,
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "policy recovery entry",
    )
    _atomic_event_hook("task8_policy_after_recovery_state")
    return _task7_load_state(context)


def _task8_enter_recovering_policy(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
) -> dict[str, object]:
    recovery = previous.get("recovery")
    if (
        previous.get("phase") != "recovery_required"
        or not isinstance(recovery, dict)
        or recovery.get("kind") != "policy"
        or recovery.get("completed_epoch") is not None
    ):
        raise OperationStateError("policy recovery resume state is invalid")
    epoch = _task7_state_epoch(context, previous)
    updated = copy.deepcopy(previous)
    _task7_append_history(
        updated,
        "recovering_policy",
        epoch,
        "policy-recovery-resume",
        {
            "operation_id": context.operation_id,
            "started_epoch": recovery["started_epoch"],
            "evidence_sha256": recovery["evidence_sha256"],
            "next_target_index": recovery["next_target_index"],
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "policy recovery resume",
    )
    _atomic_event_hook("task8_policy_after_recovery_state")
    return _task7_load_state(context)


def _task8_advance_policy_recovery_cursor(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
    *,
    pre_replace_validator: PreReplaceValidator,
) -> dict[str, object]:
    recovery = previous.get("recovery")
    if (
        previous.get("phase") != "recovering_policy"
        or not isinstance(recovery, dict)
        or recovery.get("kind") != "policy"
        or recovery.get("next_target_index") != 0
        or recovery.get("current_target") != _TARGET_ORDER[-1]
    ):
        raise OperationStateError("policy recovery cursor is not at the environment")
    updated = copy.deepcopy(previous)
    updated_recovery = updated["recovery"]
    assert isinstance(updated_recovery, dict)
    updated_recovery.update(
        {
            "next_target_index": 1,
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
        }
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "policy recovery cursor",
        pre_replace_validator=pre_replace_validator,
    )
    _atomic_event_hook("task8_policy_after_recovery_cursor_state")
    return _task7_load_state(context)


def _task8_mark_policy_recovery_required(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
) -> dict[str, object]:
    previous = _task7_load_state(context)
    recovery = previous.get("recovery")
    if (
        previous.get("phase") != "recovering_policy"
        or not isinstance(recovery, dict)
        or recovery.get("kind") != "policy"
        or recovery.get("completed_epoch") is not None
    ):
        raise OperationStateError("policy recovery-required state is invalid")
    epoch = _task7_state_epoch(context, previous)
    updated = copy.deepcopy(previous)
    _task7_append_history(
        updated,
        "recovery_required",
        epoch,
        "policy-recovery-required",
        {
            "operation_id": context.operation_id,
            "started_epoch": recovery["started_epoch"],
            "evidence_sha256": recovery["evidence_sha256"],
            "next_target_index": recovery["next_target_index"],
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "policy recovery-required",
    )
    return _task7_load_state(context)


def _task8_complete_policy_rollback(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    previous: dict[str, object],
    *,
    pre_replace_validator: PreReplaceValidator,
) -> dict[str, object]:
    recovery = previous.get("recovery")
    transaction = previous.get("active_transaction")
    if (
        previous.get("phase") != "recovering_policy"
        or previous.get("policy") is not None
        or not isinstance(recovery, dict)
        or recovery.get("kind") != "policy"
        or recovery.get("next_target_index") != 1
        or any(
            recovery.get(field) is not None
            for field in (
                "current_target",
                "previous_sha256",
                "intended_sha256",
            )
        )
        or not isinstance(transaction, dict)
        or transaction.get("kind") != "policy"
        or not all(transaction["guard"].values())
    ):
        raise OperationStateError("policy rollback completion state is invalid")
    epoch = _task7_state_epoch(context, previous)
    updated = copy.deepcopy(previous)
    updated_recovery = updated["recovery"]
    assert isinstance(updated_recovery, dict)
    updated_recovery["completed_epoch"] = epoch
    for field in ("probe", "dry_run", "policy", "observation", "active_transaction"):
        updated[field] = None
    _task7_append_history(
        updated,
        "installed",
        epoch,
        "policy-recovery-complete",
        {
            "operation_id": context.operation_id,
            "started_epoch": recovery["started_epoch"],
            "recovery_evidence_sha256": recovery["evidence_sha256"],
        },
    )
    validate_operation_state(updated, context.paths.operation_dir, previous)
    _task7_write_required_receipt_state(
        context,
        binding,
        updated,
        "policy rollback completion",
        pre_replace_validator=pre_replace_validator,
    )
    _atomic_event_hook("task8_policy_after_recovery_completion")
    return _task7_load_state(context)


def _task8_recover_probe_remote_objects(
    context: OperationsContext,
    binding: _OperationDirectoryBinding,
    state: dict[str, object],
    runtime_lock_fd: int,
) -> dict[str, object]:
    _task8_require_probe_purpose_chain(state)
    transaction = state.get("active_transaction")
    assert isinstance(transaction, dict)
    probe = transaction.get("probe")
    if not isinstance(probe, dict):
        raise OperationStateError("probe recovery identity is missing")
    prefix = _validate_probe_prefix(
        probe["prefix"],
        "active_transaction.probe.prefix",
        operation_id=context.operation_id,
    )
    objects = _task8_probe_objects(state)
    expected = {str(item["name"]): item for item in objects}
    if len(expected) != len(objects):
        raise OperationStateError("probe recovery object names are not unique")

    purposes = _task8_probe_completed_purposes(state)
    indeterminate = _task8_probe_indeterminate_purposes(state)
    precreate_owned = (
        "probe-precreate-absence" in purposes
        or (
            "probe-precreate-root" in purposes
            and bool(
                {
                    "probe-precreate-root-absence",
                    "probe-precreate-parent-absence",
                }
                & purposes
            )
        )
    )

    parent_present = _task8_run_audited_rclone_command(
        context,
        binding,
        state,
        purpose="probe-recovery-root",
        argv=_task8_probe_rclone_argv("lsjson", _PROBE_NAMESPACE_ROOT),
        label="remote probe recovery namespace-root inventory",
        runtime_lock_fd=runtime_lock_fd,
    )
    if type(parent_present) is not bool:
        raise OperationStateError("remote probe recovery root result is invalid")
    prefix_present = False
    if not parent_present:
        _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose="probe-recovery-root-absence",
            argv=_task8_probe_rclone_argv("lsjson", _PROBE_NAMESPACE_ROOT),
            label="remote probe recovery namespace-root absence proof",
            runtime_lock_fd=runtime_lock_fd,
        )
        observed: dict[str, int] = {}
    else:
        candidate_present = _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose="probe-recovery-parent",
            argv=_task8_probe_rclone_argv(
                "lsjson",
                _PROBE_NAMESPACE_ROOT + _PROBE_NAMESPACE_PARENT + "/",
            ),
            label="remote probe recovery namespace-parent inventory",
            runtime_lock_fd=runtime_lock_fd,
        )
        if type(candidate_present) is not bool:
            raise OperationStateError(
                "remote probe recovery parent result is invalid"
            )
        if not candidate_present:
            _task8_run_audited_rclone_command(
                context,
                binding,
                state,
                purpose="probe-recovery-parent-absence",
                argv=_task8_probe_rclone_argv(
                    "lsjson",
                    _PROBE_NAMESPACE_ROOT + _PROBE_NAMESPACE_PARENT + "/",
                ),
                label="remote probe recovery candidate absence proof",
                runtime_lock_fd=runtime_lock_fd,
            )
            observed = {}
        else:
            if not precreate_owned:
                raise OperationStateError(
                    "remote probe candidate exists without pre-create ownership"
                )
            prefix_present = True
            inventory_result = _task8_run_audited_rclone_command(
                context,
                binding,
                state,
                purpose="probe-recovery-inventory",
                argv=_task8_probe_rclone_argv(
                    "lsjson",
                    prefix,
                    "--recursive",
                ),
                label="remote probe prefix inventory",
                runtime_lock_fd=runtime_lock_fd,
            )
            if not isinstance(inventory_result, dict):
                raise OperationStateError("remote probe inventory result is invalid")
            observed = inventory_result

    present_names: list[str] = []
    adopt_names: list[str] = []
    missing_cleanup_names: list[str] = []
    for item in _task8_probe_objects(state):
        name = str(item["name"])
        present = name in observed
        if item["cleaned"]:
            if present:
                raise OperationStateError(
                    "remote probe object reappeared after durable cleanup"
                )
            continue
        if not item["created"]:
            if present:
                create_purpose = f"probe-create:{name}:strict-no-existing"
                if (
                    not precreate_owned
                    or (
                        create_purpose not in indeterminate
                        and create_purpose not in purposes
                    )
                ):
                    raise OperationStateError(
                        "remote probe object lacks durable creation ownership"
                    )
                adopt_names.append(name)
            continue
        if (
            f"probe-create:{name}:strict-no-existing" not in purposes
            and f"probe-adopt:{name}" not in purposes
        ):
            raise OperationStateError(
                "remote probe object lacks strict creation ownership evidence"
            )
        if present:
            present_names.append(name)
            continue
        if not item["verified"] or (
            f"probe-cleanup:{name}" not in purposes
            and f"probe-cleanup:{name}" not in indeterminate
        ):
            raise OperationStateError(
                "missing remote probe object lacks durable cleanup ownership"
            )
        missing_cleanup_names.append(name)

    for name in missing_cleanup_names:
        with _task8_open_exact_guard_recovery_material(context, state):
            _task8_advance_probe_object(
                context,
                binding,
                state,
                name,
                "cleaned",
            )

    for name in present_names:
        _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose=f"probe-verify:{name}",
            argv=_task8_probe_rclone_argv(
                "hashsum",
                "SHA-256",
                f"{prefix}{name}",
                "--download",
            ),
            label=f"remote probe object hash verification: {name}",
            runtime_lock_fd=runtime_lock_fd,
        )
        current = next(
            candidate
            for candidate in _task8_probe_objects(state)
            if candidate["name"] == name
        )
        if current["verified"] is False:
            with _task8_open_exact_guard_recovery_material(context, state):
                _task8_advance_probe_object(
                    context,
                    binding,
                    state,
                    name,
                    "verified",
                )

    for name in adopt_names:
        _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose=f"probe-adopt:{name}",
            argv=_task8_probe_rclone_argv(
                "hashsum",
                "SHA-256",
                f"{prefix}{name}",
                "--download",
            ),
            label=f"remote probe ambiguous-create adoption: {name}",
            runtime_lock_fd=runtime_lock_fd,
        )
        state = _task8_reconcile_probe_success_progress(
            context,
            binding,
            state,
        )

    for name in [*present_names, *adopt_names]:
        current = next(
            item for item in _task8_probe_objects(state) if item["name"] == name
        )
        if not (
            current["created"] is True
            and current["verified"] is True
            and current["cleaned"] is False
        ):
            raise OperationStateError(
                "remote probe object is not durably owned for cleanup"
            )
        _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose=f"probe-cleanup:{name}",
            argv=_task8_probe_rclone_argv("deletefile", f"{prefix}{name}"),
            label=f"remote probe object cleanup: {name}",
            runtime_lock_fd=runtime_lock_fd,
        )
        with _task8_open_exact_guard_recovery_material(context, state):
            _task8_advance_probe_object(
                context,
                binding,
                state,
                name,
                "cleaned",
            )
    if prefix_present:
        final_result = _task8_run_audited_rclone_command(
            context,
            binding,
            state,
            purpose="probe-prefix-empty",
            argv=_task8_probe_rclone_argv("lsjson", prefix, "--recursive"),
            label="remote probe final empty-prefix proof",
            runtime_lock_fd=runtime_lock_fd,
        )
        if not isinstance(final_result, dict) or final_result:
            raise OperationStateError("remote probe prefix is not empty after cleanup")
    if not _task8_probe_remote_cleanup_complete(state):
        raise OperationStateError("remote probe prefix cleanup is incomplete")
    return state


def _task8_run_guard_recovery(context: OperationsContext) -> dict[str, object]:
    terminal: dict[str, object] | None = None
    raised: Exception | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        validate_operation_state(state, context.paths.operation_dir)
        phase = str(state["phase"])
        if phase not in {
            "dry_run_recording",
            "observing",
            "recovery_required",
            "recovering_guard",
        }:
            raise OperationStateError(
                "guard recovery requires an interrupted dry-run or observation state"
            )
        recovery = state.get("recovery")
        if phase in {"recovery_required", "recovering_guard"} and (
            not isinstance(recovery, dict) or recovery.get("kind") != "guard"
        ):
            raise OperationStateError("guard recovery receipt is missing or invalid")

        if phase in {"dry_run_recording", "observing"}:
            with _task8_open_exact_guard_recovery_material(context, state):
                transaction = state["active_transaction"]
                assert isinstance(transaction, dict)
                _task7_record_primary_failure(
                    context,
                    binding,
                    _task8_guard_recovery_primary_error(str(transaction["kind"])),
                )
            state = _task7_load_state(context)
            with _task8_open_exact_guard_recovery_material(context, state):
                state = _task8_build_guard_recovery_required(
                    context,
                    binding,
                    state,
                )
        if state["phase"] == "recovery_required":
            with _task8_open_exact_guard_recovery_material(context, state):
                state = _task8_enter_recovering_guard(
                    context,
                    binding,
                    state,
                )
        if state["phase"] != "recovering_guard":
            raise OperationStateError(
                "guard recovery did not reach its recovery phase"
            )

        locks: MigrationLocks | None = None
        recovery_started = True
        failure_stage = "guard_recovery"
        guard_actions_started = False
        reconcile_restored_milestone = bool(
            _task8_guard_progress(state)["timer_restored"]
        )
        try:
            with _task8_open_exact_guard_recovery_material(context, state):
                state = _task7_finalize_pending_rclone_audit(
                    context,
                    binding,
                    state,
                )
                if _task7_load_state(context) != state:
                    raise OperationStateError(
                        "guard recovery state changed while finalizing local rclone audit"
                    )

            with _task8_open_exact_guard_recovery_material(
                context,
                state,
            ) as material:

                def revalidate_guard_state() -> None:
                    _revalidate_transaction_material(material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "guard recovery state changed during verified execution"
                        )

                failure_stage = "guard_reconcile"
                guard_actions_started = True
                locks = _task8_reconcile_guard_for_recovery(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_guard_state,
                )
                revalidate_guard_state()
                if locks is not None:
                    failure_stage = "release_migration_locks"
                    owned_locks = locks
                    locks = None
                    try:
                        _task8_release_recovery_guard_locks(
                            context,
                            binding,
                            state,
                            owned_locks,
                            revalidate=revalidate_guard_state,
                        )
                    except BaseException:
                        _task7_emergency_close_locks(owned_locks)
                        raise
                    revalidate_guard_state()
                    failure_stage = "timer_restore"
                    _task8_restore_recovery_guard_timer(
                        context,
                        binding,
                        state,
                        revalidate=revalidate_guard_state,
                    )
                revalidate_guard_state()
                failure_stage = "guard_recovery_completion"
                terminal = _task8_complete_guard_recovery(
                    context,
                    binding,
                    state,
                )
        except Exception as exc:
            raised = exc
            release_issues: list[_Task7LockReleaseIssue] = []
            release_error: Exception | None = None
            if isinstance(exc, _Task8GuardReleaseError):
                release_issues.extend(exc.issues)
            if locks is not None:
                owned_locks = locks
                locks = None
                try:
                    _task8_release_recovery_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                    )
                except _Task8GuardReleaseError as lock_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_issues.extend(lock_error.issues)
                except Exception as lock_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_error = lock_error
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
            if recovery_started:
                current = _task7_load_state(context)
                if current["failure"] is None:
                    _task7_record_primary_failure(context, binding, exc)
                elif not isinstance(exc, _Task8GuardReleaseError):
                    _task7_append_secondary_error(
                        context,
                        binding,
                        failure_stage,
                        exc,
                    )
                for issue in release_issues:
                    _task7_append_secondary_error(
                        context,
                        binding,
                        issue.stage,
                        issue.error,
                    )
                if release_error is not None and release_error is not exc:
                    _task7_append_secondary_error(
                        context,
                        binding,
                        "release_migration_locks",
                        release_error,
                    )
                current = _task7_load_state(context)
                current_transaction = current.get("active_transaction")
                current_guard = (
                    current_transaction.get("guard")
                    if isinstance(current_transaction, dict)
                    else None
                )
                if (
                    guard_actions_started
                    and failure_stage != "guard_recovery_completion"
                    and isinstance(current_guard, dict)
                    and (
                        current_guard.get("timer_restored") is False
                        or reconcile_restored_milestone
                    )
                ):
                    runtime_baseline = current_transaction.get("runtime_baseline")
                    assert isinstance(runtime_baseline, dict)

                    def checkpoint(action: str) -> None:
                        _task8_force_checkpoint(
                            context,
                            binding,
                            current,
                            action,
                        )

                    try:
                        _quiesce_backup_timer(
                            context,
                            runtime_baseline,
                            before_action=checkpoint,
                        )
                    except Exception as quiesce_error:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "timer_quiesce",
                            quiesce_error,
                        )
                _task8_mark_guard_recovery_required(context, binding)
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            raise
    if raised is not None:
        raise raised
    if terminal is None:
        raise OperationStateError("guard recovery did not produce a terminal receipt")
    return terminal


def _task8_run_probe_recovery(context: OperationsContext) -> dict[str, object]:
    terminal: dict[str, object] | None = None
    raised: Exception | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        phase = str(state["phase"])
        if phase not in {"probing", "recovery_required", "recovering_probe"}:
            raise OperationStateError(
                "probe recovery requires an interrupted probe state"
            )
        recovery = state.get("recovery")
        if phase in {"recovery_required", "recovering_probe"} and (
            not isinstance(recovery, dict) or recovery.get("kind") != "probe"
        ):
            raise OperationStateError("probe recovery receipt is missing or invalid")

        if phase == "probing":
            with _task8_open_exact_guard_recovery_material(context, state):
                _task7_record_primary_failure(
                    context,
                    binding,
                    _task8_probe_recovery_primary_error(),
                )
            state = _task7_load_state(context)
            with _task8_open_exact_guard_recovery_material(context, state):
                state = _task8_build_probe_recovery_required(
                    context,
                    binding,
                    state,
                )
        if state["phase"] == "recovery_required":
            with _task8_open_exact_guard_recovery_material(context, state):
                state = _task8_enter_recovering_probe(
                    context,
                    binding,
                    state,
                )
        if state["phase"] != "recovering_probe":
            raise OperationStateError(
                "probe recovery did not reach its recovery phase"
            )

        locks: MigrationLocks | None = None
        failure_stage = "probe_recovery"
        guard_actions_started = False
        reconcile_restored_milestone = bool(
            _task8_guard_progress(state)["timer_restored"]
        )
        try:
            with _task8_open_exact_guard_recovery_material(context, state):
                state = _task7_finalize_pending_rclone_audit(
                    context,
                    binding,
                    state,
                )
                if _task7_load_state(context) != state:
                    raise OperationStateError(
                        "probe recovery state changed while finalizing local rclone audit"
                    )
                _task8_require_probe_purpose_chain(state)
            state = _task8_reconcile_probe_success_progress(
                context,
                binding,
                state,
            )
            _task8_require_probe_purpose_chain(state)

            with _task8_open_exact_guard_recovery_material(
                context,
                state,
            ) as material:

                def revalidate_probe_state() -> None:
                    _revalidate_transaction_material(material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "probe recovery state changed during verified execution"
                        )

                failure_stage = "probe_guard_reconcile"
                guard_actions_started = True
                locks = _task8_reconcile_guard_for_recovery(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_probe_state,
                    allow_restored_shortcut=False,
                )
                revalidate_probe_state()

            if locks is not None:
                failure_stage = "probe_local_source_cleanup"
                with _task8_open_exact_guard_recovery_material(context, state):
                    _task8_cleanup_interrupted_probe_sources(
                        context,
                        binding,
                        state,
                    )
                failure_stage = "probe_remote_cleanup"
                state = _task8_recover_probe_remote_objects(
                    context,
                    binding,
                    state,
                    locks.runtime_fd,
                )
                with _task8_open_exact_guard_recovery_material(
                    context,
                    state,
                ) as material:

                    def revalidate_probe_finish() -> None:
                        _revalidate_transaction_material(material)
                        if _task7_load_state(context) != state:
                            raise OperationStateError(
                                "probe recovery state changed during finalization"
                            )

                    failure_stage = "release_migration_locks"
                    owned_locks = locks
                    locks = None
                    try:
                        _task8_release_recovery_guard_locks(
                            context,
                            binding,
                            state,
                            owned_locks,
                            revalidate=revalidate_probe_finish,
                        )
                    except BaseException:
                        _task7_emergency_close_locks(owned_locks)
                        raise
                    revalidate_probe_finish()
                    failure_stage = "timer_restore"
                    _task8_restore_recovery_guard_timer(
                        context,
                        binding,
                        state,
                        revalidate=revalidate_probe_finish,
                    )
                    revalidate_probe_finish()
                    failure_stage = "probe_recovery_completion"
                    terminal = _task8_complete_probe_recovery(
                        context,
                        binding,
                        state,
                    )
            else:
                failure_stage = "probe_recovery_completion"
                with _task8_open_exact_guard_recovery_material(context, state):
                    terminal = _task8_complete_probe_recovery(
                        context,
                        binding,
                        state,
                    )
        except Exception as exc:
            operational_error = exc
            audit_capture_error: Exception | None = None
            if isinstance(exc, _Task7RcloneAuditIncompleteError):
                operational_error = exc.primary_error
                if isinstance(exc.secondary_error, Exception):
                    audit_capture_error = exc.secondary_error
            raised = operational_error
            release_issues: list[_Task7LockReleaseIssue] = []
            release_error: Exception | None = None
            if isinstance(exc, _Task8GuardReleaseError):
                release_issues.extend(exc.issues)
            if locks is not None:
                owned_locks = locks
                locks = None
                try:
                    _task8_release_recovery_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                    )
                except _Task8GuardReleaseError as lock_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_issues.extend(lock_error.issues)
                except Exception as lock_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_error = lock_error
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
            current = _task7_load_state(context)
            if current["failure"] is None:
                _task7_record_primary_failure(
                    context,
                    binding,
                    operational_error,
                )
            elif not isinstance(exc, _Task8GuardReleaseError):
                _task7_append_secondary_error(
                    context,
                    binding,
                    failure_stage,
                    operational_error,
                )
            if audit_capture_error is not None:
                _task7_append_secondary_error(
                    context,
                    binding,
                    "rclone_audit_capture",
                    audit_capture_error,
                )
            for issue in release_issues:
                _task7_append_secondary_error(
                    context,
                    binding,
                    issue.stage,
                    issue.error,
                )
            if release_error is not None and release_error is not exc:
                _task7_append_secondary_error(
                    context,
                    binding,
                    "release_migration_locks",
                    release_error,
                )
            current = _task7_load_state(context)
            current_transaction = current.get("active_transaction")
            current_guard = (
                current_transaction.get("guard")
                if isinstance(current_transaction, dict)
                else None
            )
            if (
                guard_actions_started
                and failure_stage != "probe_recovery_completion"
                and isinstance(current_guard, dict)
                and (
                    current_guard.get("timer_restored") is False
                    or reconcile_restored_milestone
                )
            ):
                runtime_baseline = current_transaction.get("runtime_baseline")
                assert isinstance(runtime_baseline, dict)

                def checkpoint(action: str) -> None:
                    _task8_force_checkpoint(
                        context,
                        binding,
                        current,
                        action,
                    )

                try:
                    _quiesce_backup_timer(
                        context,
                        runtime_baseline,
                        before_action=checkpoint,
                    )
                except Exception as quiesce_error:
                    _task7_append_secondary_error(
                        context,
                        binding,
                        "timer_quiesce",
                        quiesce_error,
                    )
            _task8_mark_probe_recovery_required(context, binding)
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            raise
    if raised is not None:
        raise raised
    if terminal is None:
        raise OperationStateError("probe recovery did not produce a terminal receipt")
    return terminal


def _task8_policy_recovery_primary_error() -> OperationStateError:
    return OperationStateError(
        "interrupted Task 8 policy transaction requires policy recovery"
    )


def _task8_run_policy_recovery(
    context: OperationsContext,
    *,
    helper_route: str,
) -> dict[str, object]:
    if helper_route not in {"source", "installed"}:
        raise OperationStateError("policy recovery helper route is invalid")
    terminal: dict[str, object] | None = None
    raised: Exception | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        recovery = state.get("recovery")
        is_policy_state = state["phase"] in {
            "policy_enabling",
            "recovering_policy",
        } or (
            state["phase"] == "recovery_required"
            and isinstance(recovery, dict)
            and recovery.get("kind") == "policy"
        )
        if not is_policy_state:
            raise OperationStateError(
                "policy recovery requires an incomplete policy transaction"
            )

        locks: MigrationLocks | None = None
        recovery_started = False
        guard_actions_started = False
        failure_stage = "policy_recovery"
        release_errors: list[tuple[str, BaseException]] = []
        live_target_stack: contextlib.ExitStack | None = None
        live_target_proofs: dict[str, _SnapshotTargetProof] | None = None
        committed: bool | None = None

        def close_live_target_proofs() -> BaseException | None:
            nonlocal live_target_stack, live_target_proofs
            stack = live_target_stack
            live_target_stack = None
            live_target_proofs = None
            if stack is None:
                return None
            try:
                stack.close()
            except BaseException as exc:
                return exc
            return None

        try:
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route=helper_route,
            ):
                state = _task7_finalize_pending_rclone_audit(
                    context,
                    binding,
                    state,
                )
                if state["phase"] == "policy_enabling" and state["failure"] is None:
                    _task7_record_primary_failure(
                        context,
                        binding,
                        _task8_policy_recovery_primary_error(),
                    )
                    state = _task7_load_state(context)

            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route=helper_route,
            ) as entry_material:
                expected_install_bytes = _task7_install_bytes(entry_material)
                disabled_environment = expected_install_bytes[_TARGET_ORDER[-1]]
                enabled_environment = _task8_enabled_environment_bytes(
                    disabled_environment
                )
                host_stage = state.get("host_stage")
                install = state.get("install")
                assert isinstance(host_stage, dict) and isinstance(install, dict)
                installed_hashes = install.get("installed_hashes")
                assert isinstance(installed_hashes, dict)
                if (
                    hashlib.sha256(disabled_environment).hexdigest()
                    != host_stage.get("environment_sha256")
                    or hashlib.sha256(enabled_environment).hexdigest()
                    != host_stage.get("enabled_environment_sha256")
                    or installed_hashes.get(_TARGET_ORDER[-1])
                    != host_stage.get("environment_sha256")
                ):
                    raise OperationStateError(
                        "policy recovery staged environment provenance is invalid"
                    )
                if state["phase"] == "policy_enabling":
                    _raw, live_receipt = _task8_capture_allowed_policy_environment(
                        context,
                        (disabled_environment, enabled_environment),
                    )
                    state = _task8_build_policy_recovery_entry(
                        context,
                        binding,
                        state,
                        live_receipt=live_receipt,
                    )
                elif state["phase"] == "recovery_required":
                    state = _task8_enter_recovering_policy(
                        context,
                        binding,
                        state,
                    )
                if state["phase"] != "recovering_policy":
                    raise OperationStateError(
                        "policy recovery did not reach recovering_policy"
                    )
                recovery_started = True
                _revalidate_transaction_material(entry_material)

            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route=helper_route,
            ) as material:
                expected_install_bytes = _task7_install_bytes(material)
                disabled_environment = expected_install_bytes[_TARGET_ORDER[-1]]
                enabled_environment = _task8_enabled_environment_bytes(
                    disabled_environment
                )
                committed = isinstance(state.get("policy"), dict)
                intended_environment = (
                    enabled_environment if committed else disabled_environment
                )
                allowed_environment = (
                    (enabled_environment,)
                    if committed
                    else (disabled_environment, enabled_environment)
                )
                live_target_stack = contextlib.ExitStack()
                live_target_proofs = live_target_stack.enter_context(
                    _open_task8_policy_immutable_target_proofs(context)
                )

                def validate_recovery_state() -> None:
                    _revalidate_transaction_material(material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "policy recovery state changed during verified execution"
                        )
                    assert live_target_proofs is not None
                    _task8_validate_live_installed_immutable_target_proofs(
                        state,
                        expected_install_bytes,
                        live_target_proofs,
                    )

                def validate_recovery_commit_state() -> None:
                    validate_recovery_state()
                    _task8_require_exact_policy_environment(
                        context,
                        intended_environment,
                    )

                validate_recovery_state()
                failure_stage = "policy_recovery_guard"
                guard_actions_started = True
                locks = _task8_reconcile_guard_for_recovery(
                    context,
                    binding,
                    state,
                    revalidate=validate_recovery_state,
                    allow_restored_shortcut=False,
                )
                if locks is None:
                    raise OperationStateError(
                        "policy recovery requires freshly acquired migration locks"
                    )
                validate_recovery_state()

                failure_stage = "policy_recovery_temp_cleanup"
                _task8_cleanup_policy_environment_temp(
                    context,
                    expected=enabled_environment,
                    action="enable",
                )
                _task8_cleanup_policy_environment_temp(
                    context,
                    expected=disabled_environment,
                    action="restore",
                )

                recovery_receipt = state.get("recovery")
                assert isinstance(recovery_receipt, dict)
                if recovery_receipt["next_target_index"] == 0:
                    live_environment, live_receipt = (
                        _task8_capture_allowed_policy_environment(
                            context,
                            allowed_environment,
                        )
                    )
                    live_sha256 = live_receipt["sha256"]
                    if live_sha256 not in {
                        recovery_receipt["previous_sha256"],
                        recovery_receipt["intended_sha256"],
                    }:
                        raise OperationStateError(
                            "policy recovery live environment changed from its durable cursor"
                        )
                    if live_environment != intended_environment:
                        if committed:
                            raise OperationStateError(
                                "committed policy environment cannot be rolled back during recovery"
                            )
                        failure_stage = "policy_environment_restore"
                        _task8_atomic_replace_policy_environment(
                            context,
                            expected_previous=live_environment,
                            intended=intended_environment,
                            action="restore",
                            revalidate=validate_recovery_state,
                        )
                    _task8_require_exact_policy_environment(
                        context,
                        intended_environment,
                    )
                    _task8_durably_adopt_policy_environment(
                        context,
                        expected=intended_environment,
                        revalidate=validate_recovery_state,
                    )
                    validate_recovery_state()
                    state = _task8_advance_policy_recovery_cursor(
                        context,
                        binding,
                        state,
                        pre_replace_validator=validate_recovery_commit_state,
                    )
                else:
                    _task8_require_exact_policy_environment(
                        context,
                        intended_environment,
                    )
                    validate_recovery_state()

            failure_stage = "policy_recovery_lock_release"
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route=helper_route,
            ) as finish_material:

                def revalidate_recovery_finish() -> None:
                    _revalidate_transaction_material(finish_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "policy recovery state changed during finalization"
                        )
                    assert live_target_proofs is not None
                    _task8_validate_live_installed_immutable_target_proofs(
                        state,
                        expected_install_bytes,
                        live_target_proofs,
                    )
                    _task8_require_exact_policy_environment(
                        context,
                        intended_environment,
                    )

                owned_locks = locks
                locks = None
                assert owned_locks is not None
                try:
                    _task8_release_recovery_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                        revalidate=revalidate_recovery_finish,
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
                failure_stage = "policy_recovery_timer_restore"
                _task8_restore_recovery_guard_timer(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_recovery_finish,
                )
                revalidate_recovery_finish()
                failure_stage = "policy_recovery_completion"
                terminal = (
                    _task8_complete_enabled_policy(
                        context,
                        binding,
                        state,
                        pre_replace_validator=revalidate_recovery_finish,
                    )
                    if committed
                    else _task8_complete_policy_rollback(
                        context,
                        binding,
                        state,
                        pre_replace_validator=revalidate_recovery_finish,
                    )
                )
                proof_close_error = close_live_target_proofs()
                if proof_close_error is not None:
                    raise proof_close_error
        except Exception as exc:
            operational_error: Exception = exc
            if isinstance(exc, _Task8GuardReleaseError):
                operational_error = OperationStateError(
                    "policy recovery migration lock release failed"
                )
                release_errors.extend(
                    (issue.stage, issue.error) for issue in exc.issues
                )
            raised = operational_error
            if locks is not None:
                owned_locks = locks
                locks = None
                try:
                    _task8_release_recovery_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                    )
                except _Task8GuardReleaseError as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.extend(
                        (issue.stage, issue.error)
                        for issue in release_error.issues
                    )
                except Exception as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.append(
                        ("release_migration_locks", release_error)
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
            proof_close_error = close_live_target_proofs()
            if proof_close_error is not None:
                if not isinstance(proof_close_error, Exception):
                    raise proof_close_error
                release_errors.append(
                    ("immutable_target_proof_close", proof_close_error)
                )
            current = _task7_load_state(context)
            expected_terminal_phase = (
                "policy_enabled" if committed is True else "installed"
            )
            current_recovery = current.get("recovery")
            exact_terminal = (
                committed is not None
                and current["phase"] == expected_terminal_phase
                and current.get("active_transaction") is None
                and isinstance(current_recovery, dict)
                and current_recovery.get("kind") == "policy"
                and current_recovery.get("completed_epoch") is not None
                and (
                    isinstance(current.get("policy"), dict)
                    if committed
                    else current.get("policy") is None
                )
            )
            if exact_terminal:
                terminal = current
                raised = None
            elif recovery_started:
                if current["failure"] is None:
                    _task7_record_primary_failure(
                        context,
                        binding,
                        operational_error,
                    )
                else:
                    _task7_append_secondary_error(
                        context,
                        binding,
                        failure_stage,
                        operational_error,
                    )
                for stage, release_error in release_errors:
                    _task7_append_secondary_error(
                        context,
                        binding,
                        stage,
                        release_error,
                    )
                current = _task7_load_state(context)
                if guard_actions_started:
                    transaction = current.get("active_transaction")
                    assert isinstance(transaction, dict)
                    runtime_baseline = transaction.get("runtime_baseline")
                    assert isinstance(runtime_baseline, dict)

                    def checkpoint(action: str) -> None:
                        _task8_force_checkpoint(
                            context,
                            binding,
                            current,
                            action,
                        )

                    try:
                        _quiesce_backup_timer(
                            context,
                            runtime_baseline,
                            before_action=checkpoint,
                        )
                    except Exception as quiesce_error:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "timer_quiesce",
                            quiesce_error,
                        )
                _task8_mark_policy_recovery_required(
                    context,
                    binding,
                )
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            close_live_target_proofs()
            raise
    if raised is not None:
        raise raised
    if terminal is None:
        raise OperationStateError("policy recovery did not produce a terminal receipt")
    return terminal


def recover_host_configuration(context: OperationsContext) -> dict[str, object]:
    state = _task7_load_state(context)
    phase = str(state["phase"])
    recovery = state.get("recovery")
    is_probe_recovery = phase in {"probing", "recovering_probe"}
    if phase == "recovery_required" and isinstance(recovery, dict):
        is_probe_recovery = recovery.get("kind") == "probe"
    if is_probe_recovery:
        return _task8_run_probe_recovery(context)
    is_guard_recovery = phase in {"dry_run_recording", "observing", "recovering_guard"}
    if phase == "recovery_required" and isinstance(recovery, dict):
        is_guard_recovery = recovery.get("kind") == "guard"
    if is_guard_recovery:
        return _task8_run_guard_recovery(context)
    is_policy_recovery = phase in {"policy_enabling", "recovering_policy"}
    if phase == "recovery_required" and isinstance(recovery, dict):
        is_policy_recovery = recovery.get("kind") == "policy"
    if is_policy_recovery:
        return _task8_run_policy_recovery(context, helper_route="source")
    return _task7_run_recovery(context, manual_request=False)


def rollback_host_configuration(context: OperationsContext) -> dict[str, object]:
    return _task7_run_recovery(context, manual_request=True)


def probe_remote_storage(context: OperationsContext) -> dict[str, object]:
    completed_state: dict[str, object] | None = None
    raised: Exception | None = None
    persistence_error: Exception | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        if state["phase"] != "installed" or state.get("active_transaction") is not None:
            raise OperationStateError(
                "remote probe requires the exact installed stable phase; run "
                + _task7_recovery_command(context)
                + " for an incomplete operation"
            )
        locks: MigrationLocks | None = None
        entered = False
        guard_actions_started = False
        failure_stage = "probe_entry"
        audit_capture_error: Exception | None = None
        source_cleanup_error: Exception | None = None
        release_errors: list[tuple[str, BaseException]] = []
        try:
            with _open_verified_transaction_material(
                context,
                frozenset({"installed"}),
                helper_route="installed",
            ) as material:
                if material.source.state != state:
                    raise OperationStateError(
                        "operation state changed before normal probe proof"
                    )
                runtime_baseline = _capture_prior_runtime(context)
                token = secrets.token_hex(16)
                if re.fullmatch(r"[0-9a-f]{32}", token, re.ASCII) is None:
                    raise OperationStateError("remote probe token generator is invalid")
                prefix = (
                    _PROBE_NAMESPACE_ROOT
                    + _PROBE_NAMESPACE_PARENT
                    + "/"
                    + context.operation_id
                    + "-"
                    + token
                    + "/"
                )
                probe_identity = {
                    "prefix": prefix,
                    "objects": [
                        {
                            **item,
                            "created": False,
                            "verified": False,
                            "cleaned": False,
                        }
                        for item in _task8_probe_identity_objects(
                            context.operation_id,
                            prefix,
                        )
                    ],
                }
                _revalidate_transaction_material(material)
                if _capture_prior_runtime(context) != runtime_baseline:
                    raise OperationStateError(
                        "protected runtime changed before normal probe entry"
                    )

                def revalidate_probe_entry() -> None:
                    _revalidate_transaction_material(material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "operation state changed during normal probe entry"
                        )
                    if _capture_prior_runtime(context) != runtime_baseline:
                        raise OperationStateError(
                            "protected runtime changed during normal probe entry"
                        )
                    _revalidate_transaction_material(material)

                state = _task8_enter_guarded_transaction(
                    context,
                    binding,
                    state,
                    kind="probe",
                    runtime_baseline=runtime_baseline,
                    policy_environment_sha256=None,
                    probe=probe_identity,
                    pre_replace_validator=revalidate_probe_entry,
                )
                entered = True
                _revalidate_transaction_material(material)

            failure_stage = "probe_guard_acquire"
            guard_actions_started = True
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as guard_material:

                def revalidate_probe_guard() -> None:
                    _revalidate_transaction_material(guard_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "normal probe state changed during guard acquisition"
                        )

                locks = _task8_acquire_guard(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_probe_guard,
                )
                revalidate_probe_guard()

            failure_stage = "probe_remote_execution"
            assert locks is not None
            state = _task8_run_normal_probe_remote_objects(
                context,
                binding,
                state,
                locks.runtime_fd,
            )

            failure_stage = "release_migration_locks"
            assert locks is not None
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as finish_material:

                def revalidate_probe_finish() -> None:
                    _revalidate_transaction_material(finish_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "normal probe state changed during guard finalization"
                        )

                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                        revalidate=revalidate_probe_finish,
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise

                failure_stage = "timer_restore"
                _task8_restore_guard_timer(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_probe_finish,
                )
                revalidate_probe_finish()
                failure_stage = "probe_completion"
                completed_state = _task8_complete_normal_probe(
                    context,
                    binding,
                    state,
                )
        except Exception as exc:
            effective_error: Exception = exc
            if isinstance(exc, _Task8ProbeSourceCleanupError):
                effective_error = exc.primary_error
                source_cleanup_error = exc.cleanup_error
            operational_error: Exception = effective_error
            if isinstance(effective_error, _Task8GuardReleaseError):
                operational_error = OperationStateError(
                    "normal probe migration lock release failed"
                )
                release_errors.extend(
                    (issue.stage, issue.error) for issue in effective_error.issues
                )
            if isinstance(effective_error, _Task7RcloneAuditIncompleteError):
                operational_error = effective_error.primary_error
                if isinstance(effective_error.secondary_error, Exception):
                    audit_capture_error = effective_error.secondary_error
            raised = operational_error
            if locks is not None:
                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                    )
                except _Task8GuardReleaseError as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.extend(
                        (issue.stage, issue.error)
                        for issue in release_error.issues
                    )
                except Exception as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.append(("release_migration_locks", release_error))
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
            try:
                current = _task7_load_state(context)
                if current["phase"] == "probed":
                    completed_state = current
                    raised = None
                else:
                    entered = entered or current["phase"] == "probing"
                if (
                    current["phase"] != "probed"
                    and entered
                    and current["phase"] == "probing"
                ):
                    if current["failure"] is None:
                        _task7_record_primary_failure(
                            context,
                            binding,
                            operational_error,
                        )
                    else:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            failure_stage,
                            operational_error,
                        )
                    if audit_capture_error is not None:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "rclone_audit_capture",
                            audit_capture_error,
                        )
                    if source_cleanup_error is not None:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "probe_source_cleanup",
                            source_cleanup_error,
                        )
                    for stage, release_error in release_errors:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            stage,
                            release_error,
                        )
                    current = _task7_load_state(context)
                    if guard_actions_started:
                        transaction = current.get("active_transaction")
                        assert isinstance(transaction, dict)
                        runtime_baseline = transaction.get("runtime_baseline")
                        assert isinstance(runtime_baseline, dict)

                        def checkpoint(action: str) -> None:
                            _task8_force_checkpoint(
                                context,
                                binding,
                                current,
                                action,
                            )

                        try:
                            _quiesce_backup_timer(
                                context,
                                runtime_baseline,
                                before_action=checkpoint,
                            )
                        except Exception as quiesce_error:
                            _task7_append_secondary_error(
                                context,
                                binding,
                                "timer_quiesce",
                                quiesce_error,
                            )
                            current = _task7_load_state(context)
                    _task8_build_probe_recovery_required(
                        context,
                        binding,
                        current,
                    )
            except Exception as receipt_error:
                persistence_error = receipt_error
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            raise
    if raised is not None:
        if persistence_error is not None:
            raise raised from persistence_error
        raise raised
    if completed_state is None:
        raise OperationStateError("remote probe did not produce a terminal receipt")
    return completed_state


def record_remote_dry_run(context: OperationsContext) -> dict[str, object]:
    completed_state: dict[str, object] | None = None
    raised: Exception | None = None
    persistence_error: Exception | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        if state["phase"] != "probed" or state.get("active_transaction") is not None:
            raise OperationStateError(
                "remote dry-run requires the exact probed stable phase; run "
                + _task7_recovery_command(context)
                + " for an incomplete operation"
            )
        effective = state.get("effective_config")
        if (
            not isinstance(effective, dict)
            or effective.get("REMOTE_PRUNE_ENABLED") != "0"
        ):
            raise OperationStateError(
                "remote dry-run requires REMOTE_PRUNE_ENABLED to remain disabled"
            )

        def verify_installed_targets(
            material: _VerifiedTransactionMaterial,
            expected_state: dict[str, object],
        ) -> dict[str, bytes]:
            expected_bytes = _task7_install_bytes(material)
            observed_hashes = _task7_verify_installed_targets(
                context,
                expected_bytes,
            )
            install = expected_state.get("install")
            if not isinstance(install, dict):
                raise OperationStateError("installed target receipt is missing")
            installed_hashes = install.get("installed_hashes")
            if observed_hashes != installed_hashes:
                raise OperationStateError(
                    "installed target hashes differ from the frozen install receipt"
                )
            return expected_bytes

        locks: MigrationLocks | None = None
        entered = False
        guard_actions_started = False
        failure_stage = "dry_run_entry"
        audit_capture_error: Exception | None = None
        release_errors: list[tuple[str, BaseException]] = []
        before_inventory: list[str] | None = None
        before_casefold: list[str] | None = None
        plan: dict[str, list[str]] | None = None
        expected_install_bytes: dict[str, bytes] | None = None
        live_target_stack: contextlib.ExitStack | None = None
        live_target_proofs: dict[str, _SnapshotTargetProof] | None = None

        def close_live_target_proofs() -> BaseException | None:
            nonlocal live_target_stack, live_target_proofs
            stack = live_target_stack
            live_target_stack = None
            live_target_proofs = None
            if stack is None:
                return None
            try:
                stack.close()
            except BaseException as exc:
                return exc
            return None

        try:
            with _open_verified_transaction_material(
                context,
                frozenset({"probed"}),
                helper_route="installed",
            ) as material:
                if material.source.state != state:
                    raise OperationStateError(
                        "operation state changed before normal dry-run proof"
                    )
                expected_install_bytes = verify_installed_targets(material, state)
                runtime_baseline = _capture_prior_runtime(context)
                _revalidate_transaction_material(material)
                if _capture_prior_runtime(context) != runtime_baseline:
                    raise OperationStateError(
                        "protected runtime changed before normal dry-run entry"
                    )

                def revalidate_dry_run_entry() -> None:
                    _revalidate_transaction_material(material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "operation state changed during normal dry-run entry"
                        )
                    verify_installed_targets(material, state)
                    if _capture_prior_runtime(context) != runtime_baseline:
                        raise OperationStateError(
                            "protected runtime changed during normal dry-run entry"
                        )
                    _revalidate_transaction_material(material)

                state = _task8_enter_guarded_transaction(
                    context,
                    binding,
                    state,
                    kind="dry_run",
                    runtime_baseline=runtime_baseline,
                    policy_environment_sha256=None,
                    probe=None,
                    pre_replace_validator=revalidate_dry_run_entry,
                )
                entered = True
                _revalidate_transaction_material(material)

            assert expected_install_bytes is not None
            live_target_stack = contextlib.ExitStack()
            live_target_proofs = live_target_stack.enter_context(
                _open_task7_live_target_proofs(context)
            )

            def validate_live_targets() -> None:
                assert expected_install_bytes is not None
                assert live_target_proofs is not None
                _task8_validate_live_installed_target_proofs(
                    state,
                    expected_install_bytes,
                    live_target_proofs,
                )

            validate_live_targets()

            failure_stage = "dry_run_guard_acquire"
            guard_actions_started = True
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as guard_material:

                def revalidate_dry_run_guard() -> None:
                    _revalidate_transaction_material(guard_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "normal dry-run state changed during guard acquisition"
                        )
                    validate_live_targets()

                locks = _task8_acquire_guard(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_dry_run_guard,
                )
                revalidate_dry_run_guard()

            assert locks is not None
            failure_stage = "dry_run_inventory_before"
            validate_live_targets()
            before_result = _task8_run_normal_dry_run_audited_command(
                context,
                binding,
                state,
                purpose="dry-run-inventory-before",
                runtime_lock_fd=locks.runtime_fd,
            )
            validate_live_targets()
            if not isinstance(before_result, tuple):
                raise OperationStateError("remote dry-run before inventory is invalid")
            before_inventory, before_casefold = before_result
            plan = _task8_run_remote_retention_planner(
                context,
                state,
                before_inventory,
                runtime_lock_fd=locks.runtime_fd,
            )

            failure_stage = "dry_run_runtime"
            validate_live_targets()
            runtime_candidates = _task8_run_normal_dry_run_audited_command(
                context,
                binding,
                state,
                purpose="dry-run-runtime",
                runtime_lock_fd=locks.runtime_fd,
                expected_delete_names=plan["delete_names"],
            )
            validate_live_targets()
            if runtime_candidates != plan["delete_names"]:
                raise OperationStateError(
                    "remote dry-run candidates differ from the independent plan"
                )

            failure_stage = "dry_run_inventory_after"
            validate_live_targets()
            after_result = _task8_run_normal_dry_run_audited_command(
                context,
                binding,
                state,
                purpose="dry-run-inventory-after",
                runtime_lock_fd=locks.runtime_fd,
            )
            validate_live_targets()
            if not isinstance(after_result, tuple):
                raise OperationStateError("remote dry-run after inventory is invalid")
            after_inventory, after_casefold = after_result
            if (
                after_inventory != before_inventory
                or after_casefold != before_casefold
            ):
                raise OperationStateError(
                    "remote dry-run inventory changed during the guarded review"
                )

            failure_stage = "release_migration_locks"
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as finish_material:
                verify_installed_targets(finish_material, state)

                def revalidate_dry_run_finish() -> None:
                    _revalidate_transaction_material(finish_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "normal dry-run state changed during guard finalization"
                        )
                    validate_live_targets()

                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                        revalidate=revalidate_dry_run_finish,
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
                failure_stage = "timer_restore"
                _task8_restore_guard_timer(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_dry_run_finish,
                )
                revalidate_dry_run_finish()
                verify_installed_targets(finish_material, state)
                validate_live_targets()
                failure_stage = "installed_target_proof_close"
                proof_close_error = close_live_target_proofs()
                if proof_close_error is not None:
                    raise proof_close_error
                failure_stage = "dry_run_completion"
                assert before_inventory is not None
                assert before_casefold is not None
                assert plan is not None
                completed_state = _task8_complete_normal_dry_run(
                    context,
                    binding,
                    state,
                    inventory_names=before_inventory,
                    casefold_names=before_casefold,
                    plan=plan,
                )
        except Exception as exc:
            operational_error: Exception = exc
            if isinstance(exc, _Task8GuardReleaseError):
                operational_error = OperationStateError(
                    "normal dry-run migration lock release failed"
                )
                release_errors.extend(
                    (issue.stage, issue.error) for issue in exc.issues
                )
            if isinstance(exc, _Task7RcloneAuditIncompleteError):
                operational_error = exc.primary_error
                if isinstance(exc.secondary_error, Exception):
                    audit_capture_error = exc.secondary_error
            raised = operational_error
            if locks is not None:
                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                    )
                except _Task8GuardReleaseError as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.extend(
                        (issue.stage, issue.error)
                        for issue in release_error.issues
                    )
                except Exception as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.append(
                        ("release_migration_locks", release_error)
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
            proof_close_error = close_live_target_proofs()
            if proof_close_error is not None:
                if not isinstance(proof_close_error, Exception):
                    raise proof_close_error
                release_errors.append(
                    ("installed_target_proof_close", proof_close_error)
                )
            try:
                current = _task7_load_state(context)
                if current["phase"] == "dry_run_recorded":
                    completed_state = current
                    raised = None
                else:
                    entered = entered or current["phase"] == "dry_run_recording"
                if (
                    current["phase"] != "dry_run_recorded"
                    and entered
                    and current["phase"] == "dry_run_recording"
                ):
                    if current["failure"] is None:
                        _task7_record_primary_failure(
                            context,
                            binding,
                            operational_error,
                        )
                    else:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            failure_stage,
                            operational_error,
                        )
                    if audit_capture_error is not None:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "rclone_audit_capture",
                            audit_capture_error,
                        )
                    for stage, release_error in release_errors:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            stage,
                            release_error,
                        )
                    current = _task7_load_state(context)
                    if guard_actions_started:
                        transaction = current.get("active_transaction")
                        assert isinstance(transaction, dict)
                        runtime_baseline = transaction.get("runtime_baseline")
                        assert isinstance(runtime_baseline, dict)

                        def checkpoint(action: str) -> None:
                            _task8_force_checkpoint(
                                context,
                                binding,
                                current,
                                action,
                            )

                        try:
                            _quiesce_backup_timer(
                                context,
                                runtime_baseline,
                                before_action=checkpoint,
                            )
                        except Exception as quiesce_error:
                            _task7_append_secondary_error(
                                context,
                                binding,
                                "timer_quiesce",
                                quiesce_error,
                            )
                            current = _task7_load_state(context)
                    _task8_build_guard_recovery_required(
                        context,
                        binding,
                        current,
                    )
            except Exception as receipt_error:
                persistence_error = receipt_error
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            close_live_target_proofs()
            raise
    if raised is not None:
        if persistence_error is not None:
            raise raised from persistence_error
        raise raised
    if completed_state is None:
        raise OperationStateError("remote dry-run did not produce a terminal receipt")
    return completed_state


def enable_remote_prune(context: OperationsContext) -> dict[str, object]:
    completed_state: dict[str, object] | None = None
    raised: Exception | None = None
    persistence_error: Exception | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        if state["phase"] != "dry_run_recorded" or state.get("active_transaction") is not None:
            raise OperationStateError(
                "remote prune enablement requires the exact dry_run_recorded stable phase; run "
                + _task7_recovery_command(context)
                + " for an incomplete operation"
            )
        effective = state.get("effective_config")
        if (
            not isinstance(effective, dict)
            or effective.get("REMOTE_PRUNE_ENABLED") != "0"
        ):
            raise OperationStateError(
                "remote prune enablement requires REMOTE_PRUNE_ENABLED to remain disabled"
            )

        def verify_installed_targets(
            material: _VerifiedTransactionMaterial,
            expected_state: dict[str, object],
        ) -> dict[str, bytes]:
            expected_bytes = _task7_install_bytes(material)
            observed_hashes = _task7_verify_installed_targets(
                context,
                expected_bytes,
            )
            install = expected_state.get("install")
            if not isinstance(install, dict):
                raise OperationStateError("installed target receipt is missing")
            installed_hashes = install.get("installed_hashes")
            if observed_hashes != installed_hashes:
                raise OperationStateError(
                    "installed target hashes differ from the frozen install receipt"
                )
            return expected_bytes

        locks: MigrationLocks | None = None
        entered = False
        guard_actions_started = False
        failure_stage = "policy_entry"
        audit_capture_error: Exception | None = None
        release_errors: list[tuple[str, BaseException]] = []
        expected_install_bytes: dict[str, bytes] | None = None
        disabled_environment: bytes | None = None
        enabled_environment: bytes | None = None
        live_target_stack: contextlib.ExitStack | None = None
        live_target_proofs: dict[str, _SnapshotTargetProof] | None = None

        def close_live_target_proofs() -> BaseException | None:
            nonlocal live_target_stack, live_target_proofs
            stack = live_target_stack
            live_target_stack = None
            live_target_proofs = None
            if stack is None:
                return None
            try:
                stack.close()
            except BaseException as exc:
                return exc
            return None

        try:
            with _open_verified_transaction_material(
                context,
                frozenset({"dry_run_recorded"}),
                helper_route="installed",
            ) as material:
                if material.source.state != state:
                    raise OperationStateError(
                        "operation state changed before policy enablement proof"
                    )
                expected_install_bytes = verify_installed_targets(material, state)
                disabled_environment = expected_install_bytes[_TARGET_ORDER[-1]]
                enabled_environment = _task8_enabled_environment_bytes(
                    disabled_environment
                )
                host_stage = state.get("host_stage")
                install = state.get("install")
                assert isinstance(host_stage, dict) and isinstance(install, dict)
                installed_hashes = install.get("installed_hashes")
                assert isinstance(installed_hashes, dict)
                disabled_sha256 = hashlib.sha256(disabled_environment).hexdigest()
                enabled_sha256 = hashlib.sha256(enabled_environment).hexdigest()
                if (
                    host_stage.get("environment_sha256") != disabled_sha256
                    or host_stage.get("enabled_environment_sha256") != enabled_sha256
                    or installed_hashes.get(_TARGET_ORDER[-1]) != disabled_sha256
                ):
                    raise OperationStateError(
                        "policy environment bytes differ from frozen host-stage provenance"
                    )
                _task8_require_exact_policy_environment(
                    context,
                    disabled_environment,
                )
                runtime_baseline = _capture_prior_runtime(context)
                _revalidate_transaction_material(material)
                if _capture_prior_runtime(context) != runtime_baseline:
                    raise OperationStateError(
                        "protected runtime changed before policy transaction entry"
                    )

                def revalidate_policy_entry() -> None:
                    _revalidate_transaction_material(material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "operation state changed during policy transaction entry"
                        )
                    verify_installed_targets(material, state)
                    _task8_require_exact_policy_environment(
                        context,
                        disabled_environment,
                    )
                    if _capture_prior_runtime(context) != runtime_baseline:
                        raise OperationStateError(
                            "protected runtime changed during policy transaction entry"
                        )
                    _revalidate_transaction_material(material)

                state = _task8_enter_guarded_transaction(
                    context,
                    binding,
                    state,
                    kind="policy",
                    runtime_baseline=runtime_baseline,
                    policy_environment_sha256=enabled_sha256,
                    probe=None,
                    pre_replace_validator=revalidate_policy_entry,
                )
                entered = True
                _revalidate_transaction_material(material)

            assert expected_install_bytes is not None
            assert disabled_environment is not None
            assert enabled_environment is not None
            live_target_stack = contextlib.ExitStack()
            live_target_proofs = live_target_stack.enter_context(
                _open_task8_policy_immutable_target_proofs(context)
            )

            def validate_immutable_targets() -> None:
                assert expected_install_bytes is not None
                assert live_target_proofs is not None
                _task8_validate_live_installed_immutable_target_proofs(
                    state,
                    expected_install_bytes,
                    live_target_proofs,
                )

            validate_immutable_targets()
            _task8_require_exact_policy_environment(
                context,
                disabled_environment,
            )

            failure_stage = "policy_guard_acquire"
            guard_actions_started = True
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as guard_material:

                def revalidate_policy_guard() -> None:
                    _revalidate_transaction_material(guard_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "policy state changed during guard acquisition"
                        )
                    validate_immutable_targets()
                    _task8_require_exact_policy_environment(
                        context,
                        disabled_environment,
                    )

                locks = _task8_acquire_guard(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_policy_guard,
                )
                revalidate_policy_guard()

            assert locks is not None
            failure_stage = "policy_inventory"
            validate_immutable_targets()
            _task8_require_exact_policy_environment(
                context,
                disabled_environment,
            )
            _task8_run_normal_policy_inventory(
                context,
                binding,
                state,
                runtime_lock_fd=locks.runtime_fd,
            )
            validate_immutable_targets()
            _task8_require_exact_policy_environment(
                context,
                disabled_environment,
            )

            failure_stage = "policy_environment_replace"
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as apply_material:

                def revalidate_policy_apply() -> None:
                    _revalidate_transaction_material(apply_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "policy state changed during environment replacement"
                        )
                    validate_immutable_targets()

                applied_target = _task8_atomic_replace_policy_environment(
                    context,
                    expected_previous=disabled_environment,
                    intended=enabled_environment,
                    action="enable",
                    revalidate=revalidate_policy_apply,
                )
                revalidate_policy_apply()
            _task8_require_exact_policy_environment(
                context,
                enabled_environment,
            )
            validate_immutable_targets()

            failure_stage = "policy_applied_receipt"
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as receipt_material:
                def revalidate_policy_receipt() -> None:
                    _revalidate_transaction_material(receipt_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "policy state changed before applied receipt"
                        )
                    validate_immutable_targets()
                    _task8_require_exact_policy_environment(
                        context,
                        enabled_environment,
                    )

                revalidate_policy_receipt()
                state = _task8_record_applied_policy(
                    context,
                    binding,
                    state,
                    applied_target=applied_target,
                    pre_replace_validator=revalidate_policy_receipt,
                )

            failure_stage = "release_migration_locks"
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as finish_material:

                def revalidate_policy_finish() -> None:
                    _revalidate_transaction_material(finish_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "policy state changed during guard finalization"
                        )
                    validate_immutable_targets()
                    _task8_require_exact_policy_environment(
                        context,
                        enabled_environment,
                    )

                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                        revalidate=revalidate_policy_finish,
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
                failure_stage = "timer_restore"
                _task8_restore_guard_timer(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_policy_finish,
                )
                revalidate_policy_finish()
                failure_stage = "policy_completion"
                completed_state = _task8_complete_enabled_policy(
                    context,
                    binding,
                    state,
                    pre_replace_validator=revalidate_policy_finish,
                )
                failure_stage = "immutable_target_proof_close"
                proof_close_error = close_live_target_proofs()
                if proof_close_error is not None:
                    raise proof_close_error
        except Exception as exc:
            operational_error: Exception = exc
            if isinstance(exc, _Task8GuardReleaseError):
                operational_error = OperationStateError(
                    "policy migration lock release failed"
                )
                release_errors.extend(
                    (issue.stage, issue.error) for issue in exc.issues
                )
            if isinstance(exc, _Task7RcloneAuditIncompleteError):
                operational_error = exc.primary_error
                if isinstance(exc.secondary_error, Exception):
                    audit_capture_error = exc.secondary_error
            raised = operational_error
            if locks is not None:
                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                    )
                except _Task8GuardReleaseError as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.extend(
                        (issue.stage, issue.error)
                        for issue in release_error.issues
                    )
                except Exception as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.append(
                        ("release_migration_locks", release_error)
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
            proof_close_error = close_live_target_proofs()
            if proof_close_error is not None:
                if not isinstance(proof_close_error, Exception):
                    raise proof_close_error
                release_errors.append(
                    ("immutable_target_proof_close", proof_close_error)
                )
            try:
                current = _task7_load_state(context)
                if current["phase"] == "policy_enabled":
                    completed_state = current
                    raised = None
                elif entered and current["phase"] == "policy_enabling":
                    if current["failure"] is None:
                        _task7_record_primary_failure(
                            context,
                            binding,
                            operational_error,
                        )
                    else:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            failure_stage,
                            operational_error,
                        )
                    if audit_capture_error is not None:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "rclone_audit_capture",
                            audit_capture_error,
                        )
                    for stage, release_error in release_errors:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            stage,
                            release_error,
                        )
                    if guard_actions_started:
                        current = _task7_load_state(context)
                        transaction = current.get("active_transaction")
                        assert isinstance(transaction, dict)
                        runtime_baseline = transaction.get("runtime_baseline")
                        assert isinstance(runtime_baseline, dict)

                        def checkpoint(action: str) -> None:
                            _task8_force_checkpoint(
                                context,
                                binding,
                                current,
                                action,
                            )

                        try:
                            _quiesce_backup_timer(
                                context,
                                runtime_baseline,
                                before_action=checkpoint,
                            )
                        except Exception as quiesce_error:
                            _task7_append_secondary_error(
                                context,
                                binding,
                                "timer_quiesce",
                                quiesce_error,
                            )
            except Exception as receipt_error:
                persistence_error = receipt_error
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            close_live_target_proofs()
            raise
    if raised is not None:
        recovery_error: Exception | None = persistence_error
        current = _task7_load_state(context)
        current_recovery = current.get("recovery")
        recoverable_policy = current["phase"] in {
            "policy_enabling",
            "recovering_policy",
        } or (
            current["phase"] == "recovery_required"
            and isinstance(current_recovery, dict)
            and current_recovery.get("kind") == "policy"
        )
        if recoverable_policy:
            try:
                recovered = _task8_run_policy_recovery(
                    context,
                    helper_route="installed",
                )
            except Exception as exc:
                recovery_error = exc
            else:
                if recovered["phase"] == "policy_enabled":
                    return recovered
        if recovery_error is not None:
            raise raised from recovery_error
        raise raised
    if completed_state is None:
        raise OperationStateError("remote prune enablement produced no terminal receipt")
    return completed_state


def observe_scheduled_backup(context: OperationsContext) -> dict[str, object]:
    completed_state: dict[str, object] | None = None
    raised: Exception | None = None
    persistence_error: Exception | None = None
    with _open_operation_transaction(context) as binding:
        state = _task7_load_state(context)
        if state["phase"] != "policy_enabled" or state.get("active_transaction") is not None:
            raise OperationStateError(
                "scheduled backup observation requires the exact policy_enabled stable phase; run "
                + _task7_recovery_command(context)
                + " for an incomplete operation"
            )
        policy = state.get("policy")
        if not isinstance(policy, dict):
            raise OperationStateError("scheduled backup observation policy is missing")

        locks: MigrationLocks | None = None
        entered = False
        guard_actions_started = False
        failure_stage = "observation_entry"
        audit_capture_error: Exception | None = None
        release_errors: list[tuple[str, BaseException]] = []
        expected_install_bytes: dict[str, bytes] | None = None
        enabled_environment: bytes | None = None
        runtime_baseline: dict[str, object] | None = None
        immutable_stack: contextlib.ExitStack | None = None
        immutable_proofs: dict[str, _SnapshotTargetProof] | None = None
        local_stack: contextlib.ExitStack | None = None
        local_evidence: dict[str, object] | None = None

        def close_local_evidence() -> BaseException | None:
            nonlocal local_stack, local_evidence
            stack = local_stack
            local_stack = None
            local_evidence = None
            if stack is None:
                return None
            try:
                stack.close()
            except BaseException as exc:
                return exc
            return None

        def close_immutable_proofs() -> BaseException | None:
            nonlocal immutable_stack, immutable_proofs
            stack = immutable_stack
            immutable_stack = None
            immutable_proofs = None
            if stack is None:
                return None
            try:
                stack.close()
            except BaseException as exc:
                return exc
            return None

        def require_policy_environment() -> None:
            assert enabled_environment is not None
            observed = _task8_require_exact_policy_environment(
                context,
                enabled_environment,
            )
            current_policy = state.get("policy")
            if (
                not isinstance(current_policy, dict)
                or observed != current_policy.get("applied_target")
            ):
                raise OperationStateError(
                    "live enabled policy environment differs from its applied receipt"
                )

        try:
            with _open_verified_transaction_material(
                context,
                frozenset({"policy_enabled"}),
                helper_route="installed",
            ) as material:
                if material.source.state != state:
                    raise OperationStateError(
                        "operation state changed before scheduled observation proof"
                    )
                expected_install_bytes = _task7_install_bytes(material)
                enabled_environment = _task8_enabled_environment_bytes(
                    expected_install_bytes[_TARGET_ORDER[-1]]
                )
                if (
                    hashlib.sha256(enabled_environment).hexdigest()
                    != policy["environment_sha256"]
                ):
                    raise OperationStateError(
                        "scheduled observation enabled environment provenance is invalid"
                    )
                with _open_task8_policy_immutable_target_proofs(
                    context
                ) as entry_proofs:
                    _task8_validate_live_installed_immutable_target_proofs(
                        state,
                        expected_install_bytes,
                        entry_proofs,
                    )
                    require_policy_environment()
                    runtime_baseline = _capture_prior_runtime(context)
                    _task8_require_observation_runtime_entry(
                        state,
                        runtime_baseline,
                    )
                    _revalidate_transaction_material(material)

                    def revalidate_observation_entry() -> None:
                        _revalidate_transaction_material(material)
                        if _task7_load_state(context) != state:
                            raise OperationStateError(
                                "operation state changed during scheduled observation entry"
                            )
                        assert expected_install_bytes is not None
                        assert runtime_baseline is not None
                        _task8_validate_live_installed_immutable_target_proofs(
                            state,
                            expected_install_bytes,
                            entry_proofs,
                        )
                        require_policy_environment()
                        if _capture_prior_runtime(context) != runtime_baseline:
                            raise OperationStateError(
                                "protected runtime changed during scheduled observation entry"
                            )

                    state = _task8_enter_guarded_transaction(
                        context,
                        binding,
                        state,
                        kind="observe",
                        runtime_baseline=runtime_baseline,
                        policy_environment_sha256=None,
                        probe=None,
                        pre_replace_validator=revalidate_observation_entry,
                    )
                    entered = True
                    _revalidate_transaction_material(material)

            assert expected_install_bytes is not None
            assert enabled_environment is not None
            assert runtime_baseline is not None
            immutable_stack = contextlib.ExitStack()
            immutable_proofs = immutable_stack.enter_context(
                _open_task8_policy_immutable_target_proofs(context)
            )

            def validate_live_observation_targets() -> None:
                assert expected_install_bytes is not None
                assert immutable_proofs is not None
                _task8_validate_live_installed_immutable_target_proofs(
                    state,
                    expected_install_bytes,
                    immutable_proofs,
                )
                require_policy_environment()

            validate_live_observation_targets()
            failure_stage = "observation_guard_acquire"
            guard_actions_started = True
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as guard_material:

                def revalidate_observation_guard() -> None:
                    _revalidate_transaction_material(guard_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "observation state changed during guard acquisition"
                        )
                    validate_live_observation_targets()

                locks = _task8_acquire_guard(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_observation_guard,
                )
                revalidate_observation_guard()

            assert locks is not None
            failure_stage = "observation_timer"
            timer = _task8_capture_observation_timer(
                context,
                binding,
                state,
                runtime_lock_fd=locks.runtime_fd,
                revalidate=validate_live_observation_targets,
            )
            failure_stage = "observation_service"
            service = _task8_capture_observation_service(
                context,
                binding,
                state,
                runtime_lock_fd=locks.runtime_fd,
                revalidate=validate_live_observation_targets,
            )
            service["timer_trigger_epoch"] = timer["trigger_epoch"]
            service["timer_trigger_monotonic_usec"] = timer[
                "trigger_monotonic_usec"
            ]
            failure_stage = "observation_journal"
            journal = _task8_capture_observation_journal(
                context,
                binding,
                state,
                runtime_lock_fd=locks.runtime_fd,
                invocation_id=str(service["invocation_id"]),
                revalidate=validate_live_observation_targets,
            )
            failure_stage = "observation_local"
            local_stack = contextlib.ExitStack()
            local_evidence = local_stack.enter_context(
                _task8_open_observation_local_evidence(
                    context,
                    state,
                    runtime_lock_fd=locks.runtime_fd,
                    expected_dump_name=str(journal["dump_name"]),
                    expected_log_suffix=str(journal["log_suffix"]),
                )
            )
            correlated = _task8_validate_observation_correlation(
                state,
                service=service,
                journal=journal,
                local=local_evidence,
            )
            failure_stage = "observation_remote"
            remote = _task8_capture_observation_remote_evidence(
                context,
                binding,
                state,
                runtime_lock_fd=locks.runtime_fd,
                newest_pair=local_evidence["pairs"][0],
                journal=journal,
                revalidate=validate_live_observation_targets,
            )
            failure_stage = "observation_service_recheck"
            timer_after = _task8_capture_observation_timer(
                context,
                binding,
                state,
                runtime_lock_fd=locks.runtime_fd,
                revalidate=validate_live_observation_targets,
            )
            service_after = _task8_capture_observation_service(
                context,
                binding,
                state,
                runtime_lock_fd=locks.runtime_fd,
                revalidate=validate_live_observation_targets,
            )
            service_after["timer_trigger_epoch"] = timer_after["trigger_epoch"]
            service_after["timer_trigger_monotonic_usec"] = timer_after[
                "trigger_monotonic_usec"
            ]
            if timer_after != timer or service_after != service:
                raise OperationStateError(
                    "scheduled backup service invocation changed during observation"
                )
            validate_live_observation_targets()
            failure_stage = "observation_local_proof_close"
            local_close_error = close_local_evidence()
            if local_close_error is not None:
                raise local_close_error

            failure_stage = "release_migration_locks"
            with _task8_open_exact_guard_recovery_material(
                context,
                state,
                helper_route="installed",
            ) as finish_material:

                def revalidate_observation_finish() -> None:
                    _revalidate_transaction_material(finish_material)
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "observation state changed during guard finalization"
                        )
                    validate_live_observation_targets()

                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                        revalidate=revalidate_observation_finish,
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
                failure_stage = "timer_restore"
                _task8_restore_guard_timer(
                    context,
                    binding,
                    state,
                    revalidate=revalidate_observation_finish,
                )
                revalidate_observation_finish()
                if _capture_prior_runtime(context) != runtime_baseline:
                    raise OperationStateError(
                        "protected runtime changed after scheduled observation"
                    )
                validate_live_observation_targets()
                failure_stage = "observation_immutable_proof_close"
                immutable_close_error = close_immutable_proofs()
                if immutable_close_error is not None:
                    raise immutable_close_error

                def revalidate_observation_completion() -> None:
                    if _task7_load_state(context) != state:
                        raise OperationStateError(
                            "observation state changed before terminal receipt"
                        )
                    with _open_task8_policy_immutable_target_proofs(
                        context
                    ) as final_proofs:
                        assert expected_install_bytes is not None
                        _task8_validate_live_installed_immutable_target_proofs(
                            state,
                            expected_install_bytes,
                            final_proofs,
                        )
                    require_policy_environment()
                    if _capture_prior_runtime(context) != runtime_baseline:
                        raise OperationStateError(
                            "protected runtime changed before terminal observation receipt"
                        )

                failure_stage = "observation_completion"
                completed_state = _task8_complete_normal_observation(
                    context,
                    binding,
                    state,
                    run_epoch=int(correlated["run_epoch"]),
                    journal_sha256=str(correlated["journal_sha256"]),
                    local_sha256=str(correlated["local_sha256"]),
                    remote_sha256=str(remote["remote_sha256"]),
                    pre_replace_validator=revalidate_observation_completion,
                )
        except Exception as exc:
            operational_error: Exception = exc
            if isinstance(exc, _Task8GuardReleaseError):
                operational_error = OperationStateError(
                    "scheduled observation migration lock release failed"
                )
                release_errors.extend(
                    (issue.stage, issue.error) for issue in exc.issues
                )
            if isinstance(exc, _Task7RcloneAuditIncompleteError):
                operational_error = exc.primary_error
                if isinstance(exc.secondary_error, Exception):
                    audit_capture_error = exc.secondary_error
            raised = operational_error
            if locks is not None:
                owned_locks = locks
                locks = None
                try:
                    _task8_release_guard_locks(
                        context,
                        binding,
                        state,
                        owned_locks,
                    )
                except _Task8GuardReleaseError as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.extend(
                        (issue.stage, issue.error)
                        for issue in release_error.issues
                    )
                except Exception as release_error:
                    _task7_emergency_close_locks(owned_locks)
                    release_errors.append(
                        ("release_migration_locks", release_error)
                    )
                except BaseException:
                    _task7_emergency_close_locks(owned_locks)
                    raise
            for stage, close_error in (
                ("observation_local_proof_close", close_local_evidence()),
                ("observation_immutable_proof_close", close_immutable_proofs()),
            ):
                if close_error is not None:
                    if not isinstance(close_error, Exception):
                        raise close_error
                    release_errors.append((stage, close_error))
            try:
                current = _task7_load_state(context)
                if current["phase"] == "observed":
                    completed_state = current
                    raised = None
                else:
                    entered = entered or current["phase"] == "observing"
                if (
                    current["phase"] != "observed"
                    and entered
                    and current["phase"] == "observing"
                ):
                    if current["failure"] is None:
                        _task7_record_primary_failure(
                            context,
                            binding,
                            operational_error,
                        )
                    else:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            failure_stage,
                            operational_error,
                        )
                    if audit_capture_error is not None:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            "rclone_audit_capture",
                            audit_capture_error,
                        )
                    for stage, release_error in release_errors:
                        _task7_append_secondary_error(
                            context,
                            binding,
                            stage,
                            release_error,
                        )
                    current = _task7_load_state(context)
                    if guard_actions_started:
                        transaction = current.get("active_transaction")
                        assert isinstance(transaction, dict)
                        baseline = transaction.get("runtime_baseline")
                        assert isinstance(baseline, dict)

                        def checkpoint(action: str) -> None:
                            _task8_force_checkpoint(
                                context,
                                binding,
                                current,
                                action,
                            )

                        try:
                            _quiesce_backup_timer(
                                context,
                                baseline,
                                before_action=checkpoint,
                            )
                        except Exception as quiesce_error:
                            _task7_append_secondary_error(
                                context,
                                binding,
                                "timer_quiesce",
                                quiesce_error,
                            )
                            current = _task7_load_state(context)
                    _task8_build_guard_recovery_required(
                        context,
                        binding,
                        current,
                    )
            except Exception as receipt_error:
                persistence_error = receipt_error
        except BaseException:
            if locks is not None:
                _task7_emergency_close_locks(locks)
                locks = None
            close_local_evidence()
            close_immutable_proofs()
            raise
    if raised is not None:
        if persistence_error is not None:
            raise raised from persistence_error
        raise raised
    if completed_state is None:
        raise OperationStateError(
            "scheduled backup observation did not produce a terminal receipt"
        )
    return completed_state


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
    for command in ("install", "recover", "rollback"):
        transaction = subparsers.add_parser(command, allow_abbrev=False)
        transaction.add_argument("--operation-dir", required=True, action=_StoreOnce)
    for command in ("probe-remote", "record-dry-run", "enable-prune", "observe"):
        transaction = subparsers.add_parser(command, allow_abbrev=False)
        transaction.add_argument("--operation-dir", required=True, action=_StoreOnce)
    return parser


def _context_from_recorded_state(
    operation_dir: Path,
    state: dict[str, object],
    *,
    effective_uid: int,
) -> OperationsContext:
    reviewed = state["reviewed_source"]
    assert isinstance(reviewed, dict)
    return OperationsContext(
        operation_id=operation_dir.name,
        paths=build_operation_paths(operation_dir),
        effective_uid=effective_uid,
        command_runner=_default_command_runner,
        clock=lambda: datetime.now(timezone.utc),
        expected_commit=str(reviewed["commit"]),
        expected_archive_sha256=str(reviewed["archive_sha256"]),
        expected_manifest_sha256=str(reviewed["manifest_sha256"]),
        host_root=_PRODUCTION_HOST_ROOT,
    )


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
        elif args.command in {"install", "recover", "rollback"}:
            paths = build_operation_paths(operation_dir)
            state = load_operation_state(
                paths.state_file,
                effective_uid=effective_uid,
            )
            context = _context_from_recorded_state(
                operation_dir,
                state,
                effective_uid=effective_uid,
            )
            validate_operation_state_for_context(state, context)
            verify_running_source_helper(context)
            if args.command == "install":
                install_host_configuration(context)
            elif args.command == "recover":
                recover_host_configuration(context)
            else:
                rollback_host_configuration(context)
        elif args.command in {
            "probe-remote",
            "record-dry-run",
            "enable-prune",
            "observe",
        }:
            paths = build_operation_paths(operation_dir)
            state = load_operation_state(
                paths.state_file,
                effective_uid=effective_uid,
            )
            context = _context_from_recorded_state(
                operation_dir,
                state,
                effective_uid=effective_uid,
            )
            validate_operation_state_for_context(state, context)
            verify_running_installed_helper(context)
            handlers = {
                "probe-remote": probe_remote_storage,
                "record-dry-run": record_remote_dry_run,
                "enable-prune": enable_remote_prune,
                "observe": observe_scheduled_backup,
            }
            handlers[args.command](context)
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
