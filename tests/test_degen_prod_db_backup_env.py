from __future__ import annotations

import ast
import importlib.util
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "deploy" / "linux" / "degen-prod-db-backup-env.py"
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


def load_helper():
    assert HELPER.is_file(), f"missing helper: {HELPER}"
    spec = importlib.util.spec_from_file_location("degen_prod_db_backup_env", HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = load_helper()


def private_file(tmp_path: Path, data: bytes, name: str = "backup.env") -> tuple[Path, int]:
    path = tmp_path / name
    path.write_bytes(data)
    path.chmod(0o600)
    return path, path.stat().st_uid


def source_metadata_state(path: Path) -> tuple[int, ...]:
    metadata = path.stat()
    return (
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_atime_ns,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_nlink,
    )


def stable_source_state(path: Path) -> tuple[bytes, tuple[int, ...]]:
    data = path.read_bytes()
    metadata = source_metadata_state(path)
    if os.name != "posix":
        metadata = metadata[:4] + metadata[5:6] + metadata[7:]
    return data, metadata


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(HELPER), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def canonical(values: dict[str, str]) -> str:
    ordered = [f"{key}={values[key]}" for key in MANAGED_DEFAULTS]
    if "BACKUP_PREFIX" in values:
        ordered.append(f"BACKUP_PREFIX={values['BACKUP_PREFIX']}")
    return "\n".join(ordered) + "\n"


def test_sources_parse_with_python_310_grammar() -> None:
    for path in (HELPER, Path(__file__)):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path), feature_version=(3, 10))


def test_parse_accepts_blank_comments_and_simple_literal_assignments(tmp_path: Path) -> None:
    raw = (
        b"\n"
        b"  # managed backup configuration\n"
        b"APP_ENV_FILE=/opt/degen/web.env\n"
        b"  UNRELATED_KEY \t= literal-value_1.2:/path\n"
    )
    path, _ = private_file(tmp_path, raw)

    parsed = module.parse_simple_environment(path)

    assert isinstance(parsed, module.ParsedEnvironment)
    assert parsed.values == {
        "APP_ENV_FILE": "/opt/degen/web.env",
        "UNRELATED_KEY": "literal-value_1.2:/path",
    }
    assert parsed.raw_bytes == raw
    assert parsed.line_count == 4
    assert parsed.blank_count == 1
    assert parsed.comment_count == 1
    assert parsed.assignment_count == 2
    assert parsed.final_newline is True


@pytest.mark.parametrize(
    ("raw", "line_endings", "final_newline"),
    [
        (b"KEY=value\nOTHER=two\n", {"lf": 2, "crlf": 0, "none": 0}, True),
        (b"KEY=value\r\nOTHER=two\r\n", {"lf": 0, "crlf": 2, "none": 0}, True),
        (b"KEY=value\nOTHER=two", {"lf": 1, "crlf": 0, "none": 1}, False),
    ],
)
def test_parse_preserves_lf_crlf_and_no_final_newline(
    tmp_path: Path,
    raw: bytes,
    line_endings: dict[str, int],
    final_newline: bool,
) -> None:
    path, _ = private_file(tmp_path, raw)

    parsed = module.parse_simple_environment(path)

    assert parsed.raw_bytes == raw
    assert parsed.values == {"KEY": "value", "OTHER": "two"}
    assert parsed.line_endings == line_endings
    assert parsed.final_newline is final_newline


@pytest.mark.parametrize(
    "raw",
    [
        b"KEY=one\n KEY = two\n",
        b"KEY=one\r\n\tKEY\t=two",
        b"UNMANAGED=one\nUNMANAGED = two\n",
    ],
)
def test_parse_rejects_semantic_duplicate_keys_across_all_assignments(
    tmp_path: Path, raw: bytes
) -> None:
    path, _ = private_file(tmp_path, raw)

    with pytest.raises(ValueError, match=r"duplicate assignment on line 2"):
        module.parse_simple_environment(path)


@pytest.mark.parametrize(
    "raw",
    [
        b"KEY='quoted'\n",
        b'KEY="quoted"\n',
        b"KEY=value\\\n",
        b"KEY=first\\\nsecond\n",
        b"export KEY=value\n",
        b"KEY=$(id)\n",
        b"KEY=${OTHER}\n",
        b"KEY=$OTHER\n",
        b"KEY=`id`\n",
        b"KEY=one;touch\n",
        b"KEY=one|other\n",
        b"KEY=one two\n",
        b"KEY=value \n",
        b"KEY=value\t\n",
        b"KEY=\n",
        b"1KEY=value\n",
        b"KEY-NAME=value\n",
        b"KEY value\n",
        b"=value\n",
        b"KEY=value\x00tail\n",
        b"KEY=value\x07\n",
        b"KEY=value\rOTHER=two\n",
        b"KEY=\xff\n",
    ],
)
def test_parse_rejects_unsupported_or_ambiguous_syntax(tmp_path: Path, raw: bytes) -> None:
    path, _ = private_file(tmp_path, raw)

    with pytest.raises(ValueError):
        module.parse_simple_environment(path)


def metadata(*, mode: int = 0o600, uid: int = 1000, nlink: int = 1):
    return SimpleNamespace(
        st_mode=stat.S_IFREG | mode,
        st_uid=uid,
        st_gid=uid,
        st_nlink=nlink,
        st_dev=1,
        st_ino=2,
        st_size=10,
        st_atime_ns=100,
        st_mtime_ns=200,
        st_ctime_ns=300,
    )


def directory_metadata(*, mode: int = 0o700, uid: int = 1000):
    return SimpleNamespace(
        st_mode=stat.S_IFDIR | mode,
        st_uid=uid,
        st_gid=uid,
        st_nlink=2,
        st_dev=10,
        st_ino=20,
    )


def test_linux_metadata_contract_accepts_only_private_owned_regular_single_link_files() -> None:
    module._validate_file_metadata(metadata(), effective_uid=1000, enforce_posix_mode=True)

    with pytest.raises(PermissionError, match="owner"):
        module._validate_file_metadata(metadata(uid=1001), effective_uid=1000, enforce_posix_mode=True)
    with pytest.raises(PermissionError, match="mode 0600"):
        module._validate_file_metadata(metadata(mode=0o640), effective_uid=1000, enforce_posix_mode=True)
    with pytest.raises(PermissionError, match="mode 0600"):
        module._validate_file_metadata(metadata(mode=0o666), effective_uid=1000, enforce_posix_mode=True)
    with pytest.raises(ValueError, match="regular file"):
        module._validate_file_metadata(
            SimpleNamespace(**{**vars(metadata()), "st_mode": stat.S_IFDIR | 0o600}),
            effective_uid=1000,
            enforce_posix_mode=True,
        )
    with pytest.raises(ValueError, match="link count"):
        module._validate_file_metadata(metadata(nlink=2), effective_uid=1000, enforce_posix_mode=True)


def test_parent_metadata_contract_requires_owned_non_symlink_non_writable_directory() -> None:
    module._validate_parent_metadata(
        directory_metadata(mode=0o700), effective_uid=1000, enforce_posix_mode=True
    )
    module._validate_parent_metadata(
        directory_metadata(mode=0o750), effective_uid=1000, enforce_posix_mode=True
    )

    with pytest.raises(PermissionError, match="owner"):
        module._validate_parent_metadata(
            directory_metadata(uid=1001), effective_uid=1000, enforce_posix_mode=True
        )
    with pytest.raises(PermissionError, match="writable"):
        module._validate_parent_metadata(
            directory_metadata(mode=0o770), effective_uid=1000, enforce_posix_mode=True
        )
    with pytest.raises(PermissionError, match="writable"):
        module._validate_parent_metadata(
            directory_metadata(mode=0o702), effective_uid=1000, enforce_posix_mode=True
        )
    with pytest.raises(ValueError, match="symlink"):
        module._validate_parent_metadata(
            SimpleNamespace(
                **{**vars(directory_metadata()), "st_mode": stat.S_IFLNK | 0o777}
            ),
            effective_uid=1000,
            enforce_posix_mode=True,
        )


def test_parent_binding_rejects_descriptor_different_from_preopen_lstat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    named = directory_metadata()
    opened = SimpleNamespace(**{**vars(named), "st_ino": named.st_ino + 1})
    monkeypatch.setattr(module.os, "fstat", lambda fd: opened)
    monkeypatch.setattr(module.os, "lstat", lambda path: opened)

    with pytest.raises(RuntimeError, match="parent path changed"):
        module._validate_parent_binding(
            Path("private-parent"),
            42,
            effective_uid=1000,
            expected_identity=(named.st_dev, named.st_ino),
        )


def test_parse_rejects_wrong_effective_uid_before_opening_or_parsing_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, uid = private_file(tmp_path, b"SECRET='unsupported-content'\n")
    before = source_metadata_state(source)
    opened = False

    def forbidden_open(*args: object, **kwargs: object) -> int:
        nonlocal opened
        opened = True
        raise AssertionError("source must not be opened after owner mismatch")

    monkeypatch.setattr(module, "_current_effective_uid", lambda: uid + 1)
    monkeypatch.setattr(module.os, "open", forbidden_open)

    with pytest.raises(PermissionError, match="owner"):
        module.parse_simple_environment(source)

    assert opened is False
    assert source_metadata_state(source) == before


def test_posix_source_open_flags_require_noatime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    noatime = 0x40000
    monkeypatch.setattr(module.os, "name", "posix")
    monkeypatch.setattr(module.os, "O_NOATIME", noatime, raising=False)

    flags = module._source_open_flags()

    assert flags & noatime == noatime
    assert flags & module.os.O_RDONLY == module.os.O_RDONLY


def test_posix_source_open_flags_fail_closed_when_noatime_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module.os, "name", "posix")
    monkeypatch.delattr(module.os, "O_NOATIME", raising=False)

    with pytest.raises(RuntimeError, match="O_NOATIME"):
        module._source_open_flags()


def test_posix_noatime_open_failure_is_not_retried_without_noatime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    noatime = 0x40000
    source_metadata = metadata(uid=1000)
    open_flags: list[int] = []
    monkeypatch.setattr(module.os, "name", "posix")
    monkeypatch.setattr(module.os, "O_NOATIME", noatime, raising=False)
    monkeypatch.setattr(module.os, "lstat", lambda path: source_metadata)

    def fail_open(path: Path, flags: int) -> int:
        open_flags.append(flags)
        raise PermissionError("injected no-atime open failure")

    monkeypatch.setattr(module.os, "open", fail_open)

    with pytest.raises(PermissionError, match="injected no-atime open failure"):
        module._read_regular_file(Path("private.env"), effective_uid=1000)

    assert len(open_flags) == 1
    assert open_flags[0] & noatime == noatime


def test_source_owner_is_revalidated_on_opened_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = metadata(uid=1000)
    opened = metadata(uid=1001)
    monkeypatch.setattr(module.os, "name", "posix")
    monkeypatch.setattr(module.os, "O_NOATIME", 0x40000, raising=False)
    monkeypatch.setattr(module.os, "lstat", lambda path: before)
    monkeypatch.setattr(module.os, "open", lambda path, flags: 42)
    monkeypatch.setattr(module.os, "fstat", lambda fd: opened)
    monkeypatch.setattr(module.os, "close", lambda fd: None)

    with pytest.raises(PermissionError, match="owner"):
        module._read_regular_file(Path("private.env"), effective_uid=1000)


def test_source_owner_is_revalidated_on_path_after_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = metadata(uid=1000)
    replaced = metadata(uid=1001)
    path_results = iter((before, replaced))
    monkeypatch.setattr(module.os, "name", "posix")
    monkeypatch.setattr(module.os, "O_NOATIME", 0x40000, raising=False)
    monkeypatch.setattr(module.os, "lstat", lambda path: next(path_results))
    monkeypatch.setattr(module.os, "open", lambda path, flags: 42)
    monkeypatch.setattr(module.os, "fstat", lambda fd: before)
    monkeypatch.setattr(module.os, "read", lambda fd, size: b"")
    monkeypatch.setattr(module.os, "close", lambda fd: None)

    with pytest.raises(PermissionError, match="owner"):
        module._read_regular_file(Path("private.env"), effective_uid=1000)


def test_posix_effective_uid_lookup_fails_closed_when_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module.os, "name", "posix")
    monkeypatch.delattr(module.os, "geteuid", raising=False)

    with pytest.raises(RuntimeError, match="effective UID"):
        module._current_effective_uid()


def test_windows_effective_uid_fallback_is_explicit_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module.os, "name", "nt")
    monkeypatch.setattr(module.os, "geteuid", lambda: 9999, raising=False)

    assert module._current_effective_uid() == 0


def test_stable_metadata_detects_atime_only_changes() -> None:
    before = metadata()
    after = SimpleNamespace(**{**vars(before), "st_atime_ns": before.st_atime_ns + 1})

    assert module._stable_metadata(before) != module._stable_metadata(after)


def test_stable_metadata_detects_ctime_only_changes() -> None:
    before = metadata()
    after = SimpleNamespace(**{**vars(before), "st_ctime_ns": before.st_ctime_ns + 1})

    assert module._stable_metadata(before) != module._stable_metadata(after)


@pytest.mark.parametrize(
    ("raw", "error_type"),
    [
        (b"KEY=value\n", None),
        (b"KEY='quoted'\n", ValueError),
    ],
)
def test_parse_preserves_all_audited_source_metadata_on_success_and_failure(
    tmp_path: Path, raw: bytes, error_type: type[Exception] | None
) -> None:
    source, _ = private_file(tmp_path, raw)
    os.utime(source, ns=(1_700_000_000_123_456_700, 1_700_000_100_765_432_100))
    before = source_metadata_state(source)

    if error_type is None:
        module.parse_simple_environment(source)
    else:
        with pytest.raises(error_type):
            module.parse_simple_environment(source)

    after = source_metadata_state(source)
    if os.name == "posix":
        assert after == before
    else:
        # Windows updates atime/ctime on open; all reproducible fields stay fixed.
        assert after[:4] + after[5:6] + after[7:] == (
            before[:4] + before[5:6] + before[7:]
        )


def test_parse_rejects_symlinks_without_following_them(tmp_path: Path) -> None:
    target, _ = private_file(tmp_path, b"KEY=value\n", "target.env")
    link = tmp_path / "link.env"
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    with pytest.raises(ValueError, match="symlink"):
        module.parse_simple_environment(link)


def test_validate_effective_configuration_uses_exact_audited_defaults() -> None:
    assert module.validate_effective_configuration({}, effective_uid=0) == MANAGED_DEFAULTS


@pytest.mark.parametrize("value", ["$(id)", "one two", "'quoted'", "value\\"])
def test_validate_effective_configuration_rejects_unsupported_unmanaged_literals(
    value: str,
) -> None:
    with pytest.raises(ValueError):
        module.validate_effective_configuration({"UNMANAGED": value}, effective_uid=0)


@pytest.mark.parametrize("key", ["BAD KEY", "1BAD", "BAD-NAME", ""])
def test_validate_effective_configuration_rejects_malformed_unmanaged_keys(key: str) -> None:
    with pytest.raises(ValueError, match="configuration key"):
        module.validate_effective_configuration({key: "value"}, effective_uid=0)


@pytest.mark.parametrize("key", [key for key in MANAGED_DEFAULTS if key != "REMOTE_PRUNE_ENABLED"])
def test_validate_effective_configuration_rejects_every_fixed_default_override(key: str) -> None:
    with pytest.raises(ValueError, match=key):
        module.validate_effective_configuration({key: "different"}, effective_uid=0)


@pytest.mark.parametrize("value", ["0", "1"])
def test_validate_effective_configuration_accepts_only_documented_prune_values(value: str) -> None:
    effective = module.validate_effective_configuration(
        {"REMOTE_PRUNE_ENABLED": value}, effective_uid=0
    )
    assert effective["REMOTE_PRUNE_ENABLED"] == value


@pytest.mark.parametrize("value", ["", "2", "00", "true", "-1"])
def test_validate_effective_configuration_rejects_other_prune_values(value: str) -> None:
    with pytest.raises(ValueError, match="REMOTE_PRUNE_ENABLED"):
        module.validate_effective_configuration(
            {"REMOTE_PRUNE_ENABLED": value}, effective_uid=0
        )


@pytest.mark.parametrize("value", ["degen_green_prod_", "A-Z.0_", "db_host-1_"])
def test_validate_effective_configuration_accepts_safe_trailing_underscore_prefixes(
    value: str,
) -> None:
    effective = module.validate_effective_configuration({"BACKUP_PREFIX": value}, effective_uid=0)
    assert effective["BACKUP_PREFIX"] == value


@pytest.mark.parametrize(
    "value", ["degen", "degen/", "degen prefix_", "degen$_", "../degen_", "degen__\n"]
)
def test_validate_effective_configuration_rejects_unsafe_prefixes(value: str) -> None:
    with pytest.raises(ValueError, match="BACKUP_PREFIX"):
        module.validate_effective_configuration({"BACKUP_PREFIX": value}, effective_uid=0)


@pytest.mark.parametrize("effective_uid", [-1, True, "0"])
def test_validate_effective_configuration_rejects_invalid_effective_uid(effective_uid: object) -> None:
    with pytest.raises((TypeError, ValueError), match="effective_uid"):
        module.validate_effective_configuration({}, effective_uid=effective_uid)


def test_render_preserves_all_unmanaged_bytes_and_canonicalizes_managed_assignments(
    tmp_path: Path,
) -> None:
    raw = (
        b"# private unmanaged header\r\n"
        b"UNRELATED_SECRET=do-not-emit-this\r\n"
        b"  APP_ENV_FILE \t= /opt/degen/web.env\r\n"
        b"\r\n"
        b"OTHER_VALUE=keep=exactly"
    )
    source, uid = private_file(tmp_path, raw)
    before = stable_source_state(source)
    destination = tmp_path / "staged.env"

    module.render_managed_environment(
        source,
        destination,
        {"REMOTE_PRUNE_ENABLED": "1", "BACKUP_PREFIX": "degen_green_prod_"},
        effective_uid=uid,
    )

    rendered = destination.read_bytes()
    for unmanaged_line in (
        b"# private unmanaged header\r\n",
        b"UNRELATED_SECRET=do-not-emit-this\r\n",
        b"\r\n",
        b"OTHER_VALUE=keep=exactly",
    ):
        assert unmanaged_line in rendered
    effective = dict(MANAGED_DEFAULTS)
    effective["REMOTE_PRUNE_ENABLED"] = "1"
    effective["BACKUP_PREFIX"] = "degen_green_prod_"
    for key, value in effective.items():
        assert rendered.count(f"{key}={value}\n".encode()) == 1
    assert b"  APP_ENV_FILE \t=" not in rendered
    assert stable_source_state(source) == before
    assert not destination.is_symlink()
    if os.name == "posix":
        assert stat.S_IMODE(destination.stat().st_mode) == 0o600


@pytest.mark.parametrize("case", ["interleaved-mixed", "unmanaged-no-final-newline"])
def test_rendered_output_bytes_are_exact_for_mixed_endings_and_no_final_newline(
    tmp_path: Path, case: str
) -> None:
    effective = dict(MANAGED_DEFAULTS)
    if case == "interleaved-mixed":
        raw = (
            b"# header\r\n"
            b"UNMANAGED_A=alpha\r\n"
            b"BACKUP_DIR=/opt/degen/backups/db\r\n"
            b"# between\n"
            b"REMOTE_PRUNE_ENABLED=0\n"
            b"UNMANAGED_B=beta"
        )
        updates = {
            "REMOTE_PRUNE_ENABLED": "1",
            "BACKUP_PREFIX": "degen_green_prod_",
        }
        effective.update(updates)
        expected = (
            b"# header\r\n"
            b"UNMANAGED_A=alpha\r\n"
            + canonical(effective).encode("ascii")
            + b"# between\nUNMANAGED_B=beta"
        )
    else:
        raw = b"UNMANAGED_A=alpha\r\n# final-comment"
        updates = {}
        expected = canonical(effective).encode("ascii") + raw
    source, uid = private_file(tmp_path, raw)
    destination = tmp_path / f"{case}.env"

    module.render_managed_environment(
        source, destination, updates, effective_uid=uid
    )

    assert destination.read_bytes() == expected


@pytest.mark.parametrize(
    "updates",
    [
        {"UNMANAGED": "value"},
        {"APP_ENV_FILE": "/tmp/override"},
        {"BACKUP_PREFIX": "unsafe"},
        {"REMOTE_PRUNE_ENABLED": "2"},
    ],
)
def test_render_builds_and_validates_complete_output_before_destination_creation(
    tmp_path: Path, updates: dict[str, str]
) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    before = stable_source_state(source)
    destination = tmp_path / "must-not-exist.env"

    with pytest.raises((TypeError, ValueError)):
        module.render_managed_environment(
            source, destination, updates, effective_uid=uid
        )

    assert not destination.exists()
    assert stable_source_state(source) == before


def test_render_rejects_invalid_source_before_destination_creation(tmp_path: Path) -> None:
    source, uid = private_file(tmp_path, b"SECRET='quoted'\n")
    before = stable_source_state(source)
    destination = tmp_path / "must-not-exist.env"

    with pytest.raises(ValueError):
        module.render_managed_environment(source, destination, {}, effective_uid=uid)

    assert not destination.exists()
    assert stable_source_state(source) == before


def test_render_uses_exclusive_create_and_never_changes_an_existing_destination(
    tmp_path: Path,
) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    destination = tmp_path / "staged.env"
    destination.write_bytes(b"preexisting")
    before_source = stable_source_state(source)
    before_destination = stable_source_state(destination)

    with pytest.raises(FileExistsError):
        module.render_managed_environment(source, destination, {}, effective_uid=uid)

    assert stable_source_state(source) == before_source
    assert stable_source_state(destination) == before_destination


def test_render_rejects_destination_path_traversal_before_creation(tmp_path: Path) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    nested = tmp_path / "nested"
    nested.mkdir()
    destination = nested / ".." / "escaped.env"

    with pytest.raises(ValueError, match="destination"):
        module.render_managed_environment(source, destination, {}, effective_uid=uid)

    assert not (tmp_path / "escaped.env").exists()


def test_render_fsyncs_file_then_parent_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    destination = tmp_path / "staged.env"
    calls: list[str] = []
    real_file_fsync = module._fsync_file
    real_parent_fsync = module._fsync_parent_directory

    def record_file_fsync(fd: int) -> None:
        calls.append("file")
        real_file_fsync(fd)

    def record_parent_fsync(path: Path) -> None:
        calls.append("parent")
        real_parent_fsync(path)

    monkeypatch.setattr(module, "_fsync_file", record_file_fsync)
    monkeypatch.setattr(module, "_fsync_parent_directory", record_parent_fsync)

    module.render_managed_environment(source, destination, {}, effective_uid=uid)

    assert calls == ["file", "parent"]


def test_render_leaves_private_recovery_evidence_after_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    before = stable_source_state(source)
    destination = tmp_path / "staged.env"

    def fail_fsync(fd: int) -> None:
        raise OSError("injected fsync failure")

    monkeypatch.setattr(module, "_fsync_file", fail_fsync)

    with pytest.raises(OSError, match="injected fsync failure"):
        module.render_managed_environment(source, destination, {}, effective_uid=uid)

    assert destination.is_file()
    assert not destination.is_symlink()
    assert destination.stat().st_uid == uid
    if os.name == "posix":
        assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert stable_source_state(source) == before


def test_render_does_not_unlink_a_replacement_at_destination_on_parent_fsync_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    before = stable_source_state(source)
    destination = tmp_path / "staged.env"
    moved_partial = tmp_path / "moved-partial.env"
    replacement_created = False

    def replace_then_fail(path: Path) -> None:
        nonlocal replacement_created
        try:
            destination.replace(moved_partial)
        except PermissionError as exc:
            raise OSError("injected parent fsync failure") from exc
        destination.write_bytes(b"replacement-owned-by-someone-else")
        replacement_created = True
        raise OSError("injected parent fsync failure")

    monkeypatch.setattr(module, "_fsync_parent_directory", replace_then_fail)

    with pytest.raises(OSError, match="injected parent fsync failure"):
        module.render_managed_environment(source, destination, {}, effective_uid=uid)

    if replacement_created:
        assert destination.read_bytes() == b"replacement-owned-by-someone-else"
        assert moved_partial.exists()
    else:
        assert destination.is_file()
        assert destination.stat().st_uid == uid
        if os.name == "posix":
            assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert stable_source_state(source) == before


def test_render_revalidates_destination_after_successful_parent_fsync_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    destination = tmp_path / "staged.env"
    moved_partial = tmp_path / "moved-partial.env"
    foreign = b"foreign-replacement-must-survive"
    replacement_created = False

    def replace_then_report_success(path: Path) -> None:
        nonlocal replacement_created
        try:
            destination.replace(moved_partial)
        except PermissionError as exc:
            # Keeping the file descriptor open blocks this race on Windows.
            raise OSError("destination replacement was blocked") from exc
        destination.write_bytes(foreign)
        replacement_created = True

    monkeypatch.setattr(module, "_fsync_parent_directory", replace_then_report_success)

    with pytest.raises((OSError, RuntimeError)):
        module.render_managed_environment(source, destination, {}, effective_uid=uid)

    if replacement_created:
        assert destination.read_bytes() == foreign
        assert moved_partial.exists()
    else:
        assert destination.is_file()
        assert destination.stat().st_uid == uid
        if os.name == "posix":
            assert stat.S_IMODE(destination.stat().st_mode) == 0o600


def test_render_failure_never_unlinks_at_the_inode_swap_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=preserved\n")
    destination = tmp_path / "partial.env"
    moved_partial = tmp_path / "moved-partial.env"
    foreign = b"foreign-replacement-must-survive"
    real_unlink = module.os.unlink
    unlink_calls = 0

    def fail_fsync(fd: int) -> None:
        raise OSError("injected fsync failure")

    def replace_inside_unlink(path: Path, *args: object, **kwargs: object) -> None:
        nonlocal unlink_calls
        unlink_calls += 1
        destination.replace(moved_partial)
        destination.write_bytes(foreign)
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(module, "_fsync_file", fail_fsync)
    monkeypatch.setattr(module.os, "unlink", replace_inside_unlink)

    with pytest.raises(OSError, match="injected fsync failure"):
        module.render_managed_environment(source, destination, {}, effective_uid=uid)

    assert unlink_calls == 0
    assert destination.is_file()
    assert destination.stat().st_uid == uid
    if os.name == "posix":
        assert stat.S_IMODE(destination.stat().st_mode) == 0o600

    # With no deferred unlink pending, a later replacement remains untouched.
    destination.replace(moved_partial)
    destination.write_bytes(foreign)
    assert destination.read_bytes() == foreign
    assert moved_partial.exists()


def test_emit_runtime_configuration_contains_only_allowlisted_non_secret_keys(
    tmp_path: Path,
) -> None:
    source, uid = private_file(
        tmp_path,
        b"UNRELATED_SECRET=never-print-this\n"
        b"BACKUP_PREFIX=degen_green_prod_\n"
        b"REMOTE_PRUNE_ENABLED=1\n",
    )
    effective = dict(MANAGED_DEFAULTS)
    effective["REMOTE_PRUNE_ENABLED"] = "1"
    effective["BACKUP_PREFIX"] = "degen_green_prod_"

    output = module.emit_runtime_configuration(source, effective_uid=uid)

    assert output == canonical(effective)
    assert "UNRELATED_SECRET" not in output
    assert "never-print-this" not in output
    assert set(line.split("=", 1)[0] for line in output.splitlines()) == set(effective)


def test_emit_rejects_wrong_owner_or_mode_through_linux_metadata_contract(
    tmp_path: Path,
) -> None:
    source, uid = private_file(tmp_path, b"UNMANAGED=value\n")

    with pytest.raises(PermissionError, match="owner"):
        module.emit_runtime_configuration(source, effective_uid=uid + 1)


def test_cli_inspect_emits_compact_sorted_json_without_unrelated_names_or_values(
    tmp_path: Path,
) -> None:
    source, _ = private_file(
        tmp_path,
        b"# header\r\nUNRELATED_SECRET=never-print-this\r\nREMOTE_PRUNE_ENABLED=1",
    )
    expected = {
        "managed": {**MANAGED_DEFAULTS, "REMOTE_PRUNE_ENABLED": "1"},
        "structure": {
            "assignment_count": 2,
            "blank_count": 0,
            "comment_count": 1,
            "final_newline": False,
            "line_count": 3,
            "line_endings": {"crlf": 2, "lf": 0, "none": 1},
        },
    }

    result = run_cli("inspect", "--file", str(source))

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == json.dumps(expected, sort_keys=True, separators=(",", ":")) + "\n"
    assert "UNRELATED_SECRET" not in result.stdout
    assert "never-print-this" not in result.stdout


def test_cli_emit_is_exact_and_never_emits_unrelated_values(tmp_path: Path) -> None:
    source, _ = private_file(
        tmp_path,
        b"UNRELATED_SECRET=never-print-this\nBACKUP_PREFIX=degen_green_prod_\n",
    )
    effective = {**MANAGED_DEFAULTS, "BACKUP_PREFIX": "degen_green_prod_"}

    result = run_cli("emit", "--file", str(source))

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == canonical(effective)
    assert "never-print-this" not in result.stdout


def test_cli_render_accepts_only_unique_managed_set_values(tmp_path: Path) -> None:
    source, _ = private_file(tmp_path, b"UNMANAGED=preserved\n")
    destination = tmp_path / "staged.env"

    result = run_cli(
        "render",
        "--source",
        str(source),
        "--destination",
        str(destination),
        "--set",
        "BACKUP_PREFIX=degen_green_prod_",
        "--set",
        "REMOTE_PRUNE_ENABLED=1",
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
    assert b"UNMANAGED=preserved\n" in destination.read_bytes()
    assert b"BACKUP_PREFIX=degen_green_prod_\n" in destination.read_bytes()


@pytest.mark.parametrize(
    "set_args",
    [
        ["--set", "UNMANAGED=value"],
        ["--set", "BACKUP_PREFIX=one_", "--set", "BACKUP_PREFIX=two_"],
        ["--set", "MALFORMED"],
    ],
)
def test_cli_render_rejects_unknown_duplicate_or_malformed_updates_without_output(
    tmp_path: Path, set_args: list[str]
) -> None:
    source, _ = private_file(tmp_path, b"UNMANAGED=preserved\n")
    destination = tmp_path / "staged.env"

    result = run_cli(
        "render",
        "--source",
        str(source),
        "--destination",
        str(destination),
        *set_args,
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert not destination.exists()


def test_cli_errors_never_echo_file_contents(tmp_path: Path) -> None:
    secret_marker = "unique-secret-that-must-not-leak"
    source, _ = private_file(tmp_path, f"SECRET='{secret_marker}'\n".encode())

    result = run_cli("inspect", "--file", str(source))

    assert result.returncode != 0
    assert result.stdout == ""
    assert secret_marker not in result.stderr
    assert "Traceback" not in result.stderr


def test_cli_duplicate_key_error_never_echoes_valid_secret_marker_key(tmp_path: Path) -> None:
    secret_marker = "UNIQUE_SECRET_MARKER_KEY"
    source, _ = private_file(
        tmp_path,
        f"{secret_marker}=first\n  {secret_marker} = second\n".encode(),
    )

    result = run_cli("inspect", "--file", str(source))

    assert result.returncode != 0
    assert result.stdout == ""
    assert secret_marker not in result.stderr
    assert "Traceback" not in result.stderr


def test_cli_sanitizes_effective_uid_lookup_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, _ = private_file(tmp_path, b"KEY=value\n")

    def fail_uid_lookup() -> int:
        raise RuntimeError("unable to obtain the POSIX effective UID")

    monkeypatch.setattr(module, "_current_effective_uid", fail_uid_lookup)

    with pytest.raises(SystemExit) as exc_info:
        module.main(["inspect", "--file", str(source)])

    captured = capsys.readouterr()
    assert exc_info.value.code != 0
    assert captured.out == ""
    assert "unable to obtain the POSIX effective UID" in captured.err
    assert "Traceback" not in captured.err
