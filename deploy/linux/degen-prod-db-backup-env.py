#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


MANAGED_DEFAULTS = {
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
}
MANAGED_KEYS = frozenset((*MANAGED_DEFAULTS, "BACKUP_PREFIX"))

_KEY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*", re.ASCII)
_ASSIGNMENT_PATTERN = re.compile(
    r"[ \t]*(?P<key>[A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*(?P<value>[^ \t]+)",
    re.ASCII,
)
_LITERAL_PATTERN = re.compile(r"[A-Za-z0-9_./:@%+,?=-]+", re.ASCII)
_BACKUP_PREFIX_PATTERN = re.compile(r"[A-Za-z0-9._-]+_", re.ASCII)


@dataclass(frozen=True)
class _EnvironmentLine:
    raw: bytes
    kind: str
    key: str | None = None
    value: str | None = None
    ending: bytes = b""


@dataclass(frozen=True)
class ParsedEnvironment:
    path: Path
    raw_bytes: bytes
    values: dict[str, str]
    source_metadata: os.stat_result
    lines: tuple[_EnvironmentLine, ...]
    line_count: int
    blank_count: int
    comment_count: int
    assignment_count: int
    final_newline: bool
    line_endings: dict[str, int]


def _validate_effective_uid(effective_uid: object) -> int:
    if isinstance(effective_uid, bool) or not isinstance(effective_uid, int):
        raise TypeError("effective_uid must be a nonnegative integer")
    if effective_uid < 0:
        raise ValueError("effective_uid must be a nonnegative integer")
    return effective_uid


def _validate_file_metadata(
    metadata: object,
    *,
    effective_uid: int,
    enforce_posix_mode: bool,
) -> None:
    expected_uid = _validate_effective_uid(effective_uid)
    mode = metadata.st_mode
    if stat.S_ISLNK(mode):
        raise ValueError("environment source must not be a symlink")
    if not stat.S_ISREG(mode):
        raise ValueError("environment source must be a regular file")
    if metadata.st_nlink != 1:
        raise ValueError("environment source must have link count one")
    if metadata.st_uid != expected_uid:
        raise PermissionError(f"environment source owner must be effective uid {expected_uid}")
    if enforce_posix_mode and stat.S_IMODE(mode) != 0o600:
        raise PermissionError("environment source must have mode 0600")


def _stable_metadata(metadata: object) -> tuple[object, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        getattr(metadata, "st_gid", None),
        metadata.st_nlink,
        metadata.st_size,
        getattr(metadata, "st_atime_ns", None),
        getattr(metadata, "st_mtime_ns", None),
        getattr(metadata, "st_ctime_ns", None),
    )


def _read_stability_metadata(metadata: object, *, noatime: bool) -> tuple[object, ...]:
    stable = _stable_metadata(metadata)
    if noatime:
        return stable
    # Windows updates atime/ctime on open and has no per-open no-atime equivalent.
    return stable[:7] + stable[8:9]


def _source_open_flags() -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_BINARY", 0)
    if os.name == "posix":
        noatime = getattr(os, "O_NOATIME", None)
        if not isinstance(noatime, int) or noatime == 0:
            raise RuntimeError("POSIX O_NOATIME is required for environment reads")
        flags |= noatime
    return flags


def _read_regular_file(path: Path, *, effective_uid: int) -> tuple[bytes, os.stat_result]:
    try:
        before = os.lstat(path)
    except OSError:
        raise
    enforce_posix_mode = os.name == "posix"
    require_stable_atime = os.name == "posix"
    _validate_file_metadata(
        before,
        effective_uid=effective_uid,
        enforce_posix_mode=enforce_posix_mode,
    )

    fd = os.open(path, _source_open_flags())
    try:
        opened = os.fstat(fd)
        _validate_file_metadata(
            opened,
            effective_uid=effective_uid,
            enforce_posix_mode=enforce_posix_mode,
        )
        if _read_stability_metadata(
            before, noatime=require_stable_atime
        ) != _read_stability_metadata(opened, noatime=require_stable_atime):
            raise RuntimeError("environment source changed while it was opened")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after_read = os.fstat(fd)
        _validate_file_metadata(
            after_read,
            effective_uid=effective_uid,
            enforce_posix_mode=enforce_posix_mode,
        )
        if _read_stability_metadata(
            opened, noatime=require_stable_atime
        ) != _read_stability_metadata(after_read, noatime=require_stable_atime):
            raise RuntimeError("environment source changed while it was read")
    finally:
        os.close(fd)

    after_path = os.lstat(path)
    _validate_file_metadata(
        after_path,
        effective_uid=effective_uid,
        enforce_posix_mode=enforce_posix_mode,
    )
    if _read_stability_metadata(
        opened, noatime=require_stable_atime
    ) != _read_stability_metadata(after_path, noatime=require_stable_atime):
        raise RuntimeError("environment source metadata changed while it was read")
    return b"".join(chunks), opened


def _physical_lines(raw: bytes) -> tuple[tuple[bytes, bytes, bytes], ...]:
    if not raw:
        return ()
    result: list[tuple[bytes, bytes, bytes]] = []
    start = 0
    while start < len(raw):
        newline = raw.find(b"\n", start)
        if newline < 0:
            content = raw[start:]
            ending = b""
            full = content
            start = len(raw)
        elif newline > start and raw[newline - 1] == 0x0D:
            content = raw[start : newline - 1]
            ending = b"\r\n"
            full = raw[start : newline + 1]
            start = newline + 1
        else:
            content = raw[start:newline]
            ending = b"\n"
            full = raw[start : newline + 1]
            start = newline + 1
        if b"\r" in content:
            raise ValueError("bare carriage return is not supported")
        result.append((content, ending, full))
    return tuple(result)


def _validate_literal(value: str) -> None:
    if not _LITERAL_PATTERN.fullmatch(value):
        raise ValueError("assignment value uses unsupported literal syntax")


def parse_simple_environment(path: os.PathLike[str] | str) -> ParsedEnvironment:
    source = Path(path)
    effective_uid = _current_effective_uid()
    raw, metadata = _read_regular_file(source, effective_uid=effective_uid)
    try:
        raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError("environment source must be strict UTF-8") from exc
    for byte in raw:
        if byte in (0x09, 0x0A, 0x0D):
            continue
        if byte < 0x20 or byte == 0x7F:
            raise ValueError("environment source contains a control byte")

    values: dict[str, str] = {}
    parsed_lines: list[_EnvironmentLine] = []
    blank_count = 0
    comment_count = 0
    assignment_count = 0
    endings = {"lf": 0, "crlf": 0, "none": 0}
    for line_number, (content, ending, full) in enumerate(_physical_lines(raw), start=1):
        if ending == b"\n":
            endings["lf"] += 1
        elif ending == b"\r\n":
            endings["crlf"] += 1
        else:
            endings["none"] += 1
        text = content.decode("utf-8")
        if not text.strip(" \t"):
            blank_count += 1
            parsed_lines.append(_EnvironmentLine(full, "blank", ending=ending))
            continue
        if text.lstrip(" \t").startswith("#"):
            comment_count += 1
            parsed_lines.append(_EnvironmentLine(full, "comment", ending=ending))
            continue
        match = _ASSIGNMENT_PATTERN.fullmatch(text)
        if match is None:
            raise ValueError(f"unsupported environment syntax on line {line_number}")
        key = match.group("key")
        value = match.group("value")
        _validate_literal(value)
        if key in values:
            raise ValueError(f"duplicate assignment on line {line_number}")
        values[key] = value
        assignment_count += 1
        parsed_lines.append(
            _EnvironmentLine(full, "assignment", key=key, value=value, ending=ending)
        )

    lines = tuple(parsed_lines)
    return ParsedEnvironment(
        path=source,
        raw_bytes=raw,
        values=values,
        source_metadata=metadata,
        lines=lines,
        line_count=len(lines),
        blank_count=blank_count,
        comment_count=comment_count,
        assignment_count=assignment_count,
        final_newline=raw.endswith(b"\n"),
        line_endings=endings,
    )


def validate_effective_configuration(
    values: Mapping[str, str], *, effective_uid: int
) -> dict[str, str]:
    _validate_effective_uid(effective_uid)
    if not isinstance(values, Mapping):
        raise TypeError("values must be a mapping")
    for key, value in values.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("configuration keys and values must be strings")
        if _KEY_PATTERN.fullmatch(key) is None:
            raise ValueError("configuration key uses unsupported syntax")
        try:
            _validate_literal(value)
        except ValueError as exc:
            if key in MANAGED_KEYS:
                raise ValueError(f"{key} uses unsupported literal syntax") from exc
            raise

    effective: dict[str, str] = {}
    for key, audited_value in MANAGED_DEFAULTS.items():
        value = values.get(key, audited_value)
        if key == "REMOTE_PRUNE_ENABLED":
            if value not in {"0", "1"}:
                raise ValueError("REMOTE_PRUNE_ENABLED must be exactly 0 or 1")
        else:
            _validate_literal(value)
            if value != audited_value:
                raise ValueError(f"{key} must match its audited value")
        effective[key] = value

    if "BACKUP_PREFIX" in values:
        prefix = values["BACKUP_PREFIX"]
        if _BACKUP_PREFIX_PATTERN.fullmatch(prefix) is None:
            raise ValueError(
                "BACKUP_PREFIX must match ^[A-Za-z0-9._-]+_$"
            )
        effective["BACKUP_PREFIX"] = prefix
    return effective


def _parse_private_environment(path: os.PathLike[str] | str, effective_uid: int) -> ParsedEnvironment:
    parsed = parse_simple_environment(path)
    _validate_file_metadata(
        parsed.source_metadata,
        effective_uid=effective_uid,
        enforce_posix_mode=os.name == "posix",
    )
    return parsed


def _canonical_runtime_configuration(values: Mapping[str, str]) -> str:
    lines = [f"{key}={values[key]}" for key in MANAGED_DEFAULTS]
    if "BACKUP_PREFIX" in values:
        lines.append(f"BACKUP_PREFIX={values['BACKUP_PREFIX']}")
    return "\n".join(lines) + "\n"


def emit_runtime_configuration(path: os.PathLike[str] | str, *, effective_uid: int) -> str:
    parsed = _parse_private_environment(path, effective_uid)
    effective = validate_effective_configuration(parsed.values, effective_uid=effective_uid)
    return _canonical_runtime_configuration(effective)


def _normalize_updates(updates: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(updates, Mapping):
        raise TypeError("updates must be a mapping")
    normalized: dict[str, str] = {}
    for key, value in updates.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("update keys and values must be strings")
        if key not in MANAGED_KEYS:
            raise ValueError(f"update key is not managed: {key}")
        _validate_literal(value)
        normalized[key] = value
    return normalized


def _render_bytes(parsed: ParsedEnvironment, effective: Mapping[str, str]) -> bytes:
    managed_block = _canonical_runtime_configuration(effective).encode("ascii")
    output: list[bytes] = []
    inserted = False
    for line in parsed.lines:
        if line.kind == "assignment" and line.key in MANAGED_KEYS:
            if not inserted:
                output.append(managed_block)
                inserted = True
            continue
        output.append(line.raw)
    if not inserted:
        output.insert(0, managed_block)
    return b"".join(output)


def _set_private_mode(fd: int, path: Path) -> None:
    if hasattr(os, "fchmod"):
        os.fchmod(fd, 0o600)
    else:
        os.chmod(path, 0o600)


def _fsync_file(fd: int) -> None:
    os.fsync(fd)


def _validate_parent_metadata(
    metadata: object,
    *,
    effective_uid: int,
    enforce_posix_mode: bool,
) -> None:
    expected_uid = _validate_effective_uid(effective_uid)
    mode = metadata.st_mode
    if stat.S_ISLNK(mode):
        raise ValueError("destination parent must not be a symlink")
    if not stat.S_ISDIR(mode):
        raise ValueError("destination parent must be a directory")
    if metadata.st_uid != expected_uid:
        raise PermissionError(
            f"destination parent owner must be effective uid {expected_uid}"
        )
    if enforce_posix_mode and stat.S_IMODE(mode) & (stat.S_IWGRP | stat.S_IWOTH):
        raise PermissionError("destination parent must not be group or world writable")


def _fsync_parent_directory(path_or_fd: Path | int) -> None:
    if isinstance(path_or_fd, int):
        metadata = os.fstat(path_or_fd)
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("destination parent must be a directory")
        os.fsync(path_or_fd)
        return
    if os.name != "posix":
        return
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path_or_fd, flags)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("destination parent must be a directory")
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(fd, data[offset:])
        if written <= 0:
            raise OSError("destination write made no progress")
        offset += written


def _destination_parts(path: Path) -> tuple[Path, str]:
    if any(part == ".." for part in path.parts):
        raise ValueError("destination path traversal is not allowed")
    basename = path.name
    if not basename or basename in {".", ".."} or "/" in basename or "\\" in basename:
        raise ValueError("destination must have a safe basename")
    return path.parent, basename


def _parent_open_flags() -> int:
    flags = os.O_RDONLY
    for name in ("O_DIRECTORY", "O_CLOEXEC", "O_NOFOLLOW"):
        value = getattr(os, name, None)
        if not isinstance(value, int) or value == 0:
            raise RuntimeError(f"POSIX {name} is required for destination writes")
        flags |= value
    return flags


def _destination_open_flags() -> int:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0)
    if os.name == "posix":
        for name in ("O_CLOEXEC", "O_NOFOLLOW"):
            value = getattr(os, name, None)
            if not isinstance(value, int) or value == 0:
                raise RuntimeError(f"POSIX {name} is required for destination writes")
            flags |= value
    return flags


def _parent_identity(metadata: object) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _validate_parent_binding(
    path: Path,
    parent_fd: int,
    *,
    effective_uid: int,
    expected_identity: tuple[int, int],
) -> None:
    opened = os.fstat(parent_fd)
    named = os.lstat(path)
    _validate_parent_metadata(
        opened, effective_uid=effective_uid, enforce_posix_mode=True
    )
    _validate_parent_metadata(
        named, effective_uid=effective_uid, enforce_posix_mode=True
    )
    if (
        _parent_identity(opened) != expected_identity
        or _parent_identity(named) != expected_identity
    ):
        raise RuntimeError("destination parent path changed during render")


def _validate_destination_binding_at(
    parent_fd: int, basename: str, file_fd: int, *, effective_uid: int
) -> tuple[int, int]:
    opened = os.fstat(file_fd)
    named = os.stat(basename, dir_fd=parent_fd, follow_symlinks=False)
    _validate_file_metadata(
        opened, effective_uid=effective_uid, enforce_posix_mode=True
    )
    _validate_file_metadata(
        named, effective_uid=effective_uid, enforce_posix_mode=True
    )
    opened_identity = (opened.st_dev, opened.st_ino)
    if opened_identity != (named.st_dev, named.st_ino):
        raise RuntimeError("destination path no longer names the created file")
    return opened_identity


def _validate_destination_binding_path(
    path: Path, file_fd: int, *, effective_uid: int
) -> tuple[int, int]:
    opened = os.fstat(file_fd)
    named = os.lstat(path)
    _validate_file_metadata(
        opened, effective_uid=effective_uid, enforce_posix_mode=False
    )
    _validate_file_metadata(
        named, effective_uid=effective_uid, enforce_posix_mode=False
    )
    opened_identity = (opened.st_dev, opened.st_ino)
    if opened_identity != (named.st_dev, named.st_ino):
        raise RuntimeError("destination path no longer names the created file")
    return opened_identity


def _write_exclusive_private_posix(
    path: Path,
    parent: Path,
    basename: str,
    data: bytes,
    *,
    effective_uid: int,
) -> None:
    parent_before = os.lstat(parent)
    _validate_parent_metadata(
        parent_before, effective_uid=effective_uid, enforce_posix_mode=True
    )
    parent_fd = os.open(parent, _parent_open_flags())
    file_fd: int | None = None
    try:
        parent_identity = _parent_identity(parent_before)
        _validate_parent_binding(
            parent,
            parent_fd,
            effective_uid=effective_uid,
            expected_identity=parent_identity,
        )
        file_fd = os.open(
            basename,
            _destination_open_flags(),
            0o600,
            dir_fd=parent_fd,
        )
        os.fchmod(file_fd, 0o600)
        _validate_file_metadata(
            os.fstat(file_fd),
            effective_uid=effective_uid,
            enforce_posix_mode=True,
        )
        _write_all(file_fd, data)
        _fsync_file(file_fd)
        _validate_destination_binding_at(
            parent_fd, basename, file_fd, effective_uid=effective_uid
        )
        _fsync_parent_directory(parent_fd)
        _validate_destination_binding_at(
            parent_fd, basename, file_fd, effective_uid=effective_uid
        )
        _validate_parent_binding(
            parent,
            parent_fd,
            effective_uid=effective_uid,
            expected_identity=parent_identity,
        )
    except BaseException as primary:
        # Never unlink after exclusive creation: Python/Unix has no atomic
        # unlink-if-inode primitive. The later root-only operation-state
        # recovery transaction owns any cleanup of private 0600 evidence.
        close_error: BaseException | None = None
        if file_fd is not None:
            try:
                os.close(file_fd)
            except OSError as exc:
                close_error = exc
        try:
            os.close(parent_fd)
        except OSError as exc:
            if close_error is None:
                close_error = exc
        if close_error is not None:
            raise primary from close_error
        raise
    else:
        if file_fd is not None:
            os.close(file_fd)
        os.close(parent_fd)


def _write_exclusive_private_windows(
    path: Path, parent: Path, data: bytes, *, effective_uid: int
) -> None:
    parent_before = os.lstat(parent)
    _validate_parent_metadata(
        parent_before, effective_uid=effective_uid, enforce_posix_mode=False
    )
    fd: int | None = None
    try:
        fd = os.open(path, _destination_open_flags(), 0o600)
        _validate_destination_binding_path(path, fd, effective_uid=effective_uid)
        _set_private_mode(fd, path)
        _validate_destination_binding_path(path, fd, effective_uid=effective_uid)
        _write_all(fd, data)
        _fsync_file(fd)
        _validate_destination_binding_path(path, fd, effective_uid=effective_uid)
        _fsync_parent_directory(parent)
        _validate_destination_binding_path(path, fd, effective_uid=effective_uid)
        parent_after = os.lstat(parent)
        _validate_parent_metadata(
            parent_after, effective_uid=effective_uid, enforce_posix_mode=False
        )
        if _parent_identity(parent_before) != _parent_identity(parent_after):
            raise RuntimeError("destination parent path changed during render")
    except BaseException as primary:
        # Windows likewise defers cleanup. A pathname may have been replaced,
        # and deleting by name cannot be made conditional on the created inode.
        close_error: BaseException | None = None
        if fd is not None:
            try:
                os.close(fd)
            except OSError as exc:
                close_error = exc
            fd = None
        if close_error is not None:
            raise primary from close_error
        raise
    else:
        if fd is not None:
            os.close(fd)


def _write_exclusive_private(
    path: Path, data: bytes, *, effective_uid: int
) -> None:
    _validate_effective_uid(effective_uid)
    parent, basename = _destination_parts(path)
    if os.name == "posix":
        _write_exclusive_private_posix(
            path,
            parent,
            basename,
            data,
            effective_uid=effective_uid,
        )
    else:
        _write_exclusive_private_windows(
            path, parent, data, effective_uid=effective_uid
        )


def render_managed_environment(
    source: os.PathLike[str] | str,
    destination: os.PathLike[str] | str,
    updates: Mapping[str, str],
    *,
    effective_uid: int,
) -> None:
    parsed = _parse_private_environment(source, effective_uid)
    validate_effective_configuration(parsed.values, effective_uid=effective_uid)
    normalized_updates = _normalize_updates(updates)
    combined = dict(parsed.values)
    combined.update(normalized_updates)
    effective = validate_effective_configuration(combined, effective_uid=effective_uid)
    rendered = _render_bytes(parsed, effective)
    _write_exclusive_private(Path(destination), rendered, effective_uid=effective_uid)


def _current_effective_uid() -> int:
    if os.name != "posix":
        # Python reports st_uid == 0 for Windows files; no POSIX euid exists.
        return 0
    get_effective_uid = getattr(os, "geteuid", None)
    if not callable(get_effective_uid):
        raise RuntimeError("unable to obtain the POSIX effective UID")
    try:
        effective_uid = get_effective_uid()
    except Exception as exc:
        raise RuntimeError("unable to obtain the POSIX effective UID") from exc
    return _validate_effective_uid(effective_uid)


def _updates_from_cli(items: Sequence[str]) -> dict[str, str]:
    updates: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError("--set must use KEY=VALUE")
        key, value = item.split("=", 1)
        if _KEY_PATTERN.fullmatch(key) is None or not value:
            raise ValueError("--set must use KEY=VALUE")
        if key in updates:
            raise ValueError(f"duplicate --set key: {key}")
        if key not in MANAGED_KEYS:
            raise ValueError(f"--set key is not managed: {key}")
        _validate_literal(value)
        updates[key] = value
    return updates


def _inspect_payload(parsed: ParsedEnvironment, effective_uid: int) -> dict[str, object]:
    managed = validate_effective_configuration(parsed.values, effective_uid=effective_uid)
    return {
        "managed": managed,
        "structure": {
            "assignment_count": parsed.assignment_count,
            "blank_count": parsed.blank_count,
            "comment_count": parsed.comment_count,
            "final_newline": parsed.final_newline,
            "line_count": parsed.line_count,
            "line_endings": parsed.line_endings,
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument("--file", type=Path, required=True)

    emit_parser = subparsers.add_parser("emit")
    emit_parser.add_argument("--file", type=Path, required=True)

    render_parser = subparsers.add_parser("render")
    render_parser.add_argument("--source", type=Path, required=True)
    render_parser.add_argument("--destination", type=Path, required=True)
    render_parser.add_argument("--set", dest="updates", action="append", default=[])

    args = parser.parse_args(argv)
    try:
        effective_uid = _current_effective_uid()
        if args.command == "inspect":
            parsed = _parse_private_environment(args.file, effective_uid)
            payload = _inspect_payload(parsed, effective_uid)
            json.dump(payload, sys.stdout, sort_keys=True, separators=(",", ":"))
            sys.stdout.write("\n")
        elif args.command == "emit":
            sys.stdout.write(
                emit_runtime_configuration(args.file, effective_uid=effective_uid)
            )
        else:
            updates = _updates_from_cli(args.updates)
            render_managed_environment(
                args.source,
                args.destination,
                updates,
                effective_uid=effective_uid,
            )
    except (OSError, TypeError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
