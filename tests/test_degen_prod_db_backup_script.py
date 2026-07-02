from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "linux" / "degen-prod-db-backup.sh"
ENV_HELPER = ROOT / "deploy" / "linux" / "degen-prod-db-backup-env.py"
PLANNER = ROOT / "deploy" / "linux" / "degen-prod-db-retention.py"
SERVICE = ROOT / "deploy" / "systemd" / "degen-prod-db-backup.service"
TIMER = ROOT / "deploy" / "systemd" / "degen-prod-db-backup.timer"
ENV_TEMPLATE = ROOT / "deploy" / "systemd" / "degen-prod-db-backup.env.example"
RUNBOOK = ROOT / "docs" / "green-postgres-backup-runbook.md"
OLD_PLAN = ROOT / "docs" / "superpowers" / "plans" / "2026-06-29-green-backup-retention.md"
STAMP = "20260629T230000Z"
PREFIX = "degen_green_prod_green_"
SECRET = "postgresql://degen:do-not-log-this@db.internal/degen_green_prod"
FIXED_BACKUP_ENV_FILE = "/etc/degen/prod-db-backup.env"
FIXED_ENV_HELPER = "/usr/local/sbin/degen-prod-db-backup-env"
MANAGED_DEFAULT_KEYS = (
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
)
MANAGED_KEYS = frozenset((*MANAGED_DEFAULT_KEYS, "BACKUP_PREFIX"))


def _usable_bash() -> str | None:
    candidate = shutil.which("bash")
    if candidate is None:
        return None
    try:
        result = subprocess.run(
            [candidate, "-lc", "test -x /usr/bin/flock && test -x /usr/bin/python3"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return candidate if result.returncode == 0 else None


BASH = _usable_bash()


def _posix_path(path: Path) -> str:
    resolved = os.path.abspath(path)
    if os.name != "nt":
        return resolved
    normalized = resolved.replace("\\", "/")
    if normalized.startswith("//wsl.localhost/") or normalized.startswith("//wsl$/"):
        parts = normalized.split("/", 4)
        assert len(parts) == 5 and parts[4], resolved
        return f"/{parts[4]}"
    drive, tail = os.path.splitdrive(resolved)
    assert drive and len(drive) == 2, resolved
    stripped_tail = tail.lstrip("\\/").replace("\\", "/")
    return f"/mnt/{drive[0].lower()}/{stripped_tail}"


def _symlink(target: Path, link: Path) -> None:
    if os.name == "nt":
        subprocess.run(
            ["wsl.exe", "-e", "ln", "-s", _posix_path(target), _posix_path(link)],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    else:
        link.symlink_to(target, target_is_directory=target.is_dir())


def _hardlink(target: Path, link: Path) -> None:
    if os.name == "nt":
        subprocess.run(
            ["wsl.exe", "-e", "ln", _posix_path(target), _posix_path(link)],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    else:
        os.link(target, link)


def _is_symlink(path: Path) -> bool:
    if os.name == "nt":
        return subprocess.run(
            ["wsl.exe", "-e", "test", "-L", _posix_path(path)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        ).returncode == 0
    return path.is_symlink()


def _chown(path: Path, uid: int) -> None:
    if os.name == "nt":
        subprocess.run(
            ["wsl.exe", "-u", "root", "-e", "chown", str(uid), _posix_path(path)],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    else:
        os.chown(path, uid, -1)


def _effective_uid() -> int:
    if os.name == "nt":
        return int(
            subprocess.run(
                ["wsl.exe", "-e", "id", "-u"],
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            ).stdout.strip()
        )
    return os.geteuid()


def _native_behavior_root(tmp_path: Path) -> Path:
    if os.name != "nt":
        tmp_path.chmod(0o700)
        return tmp_path
    created = subprocess.run(
        ["wsl.exe", "-e", "mktemp", "-d", "/tmp/degen-backup-test.XXXXXXXX"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    ).stdout.strip()
    converted = subprocess.run(
        ["wsl.exe", "-e", "wslpath", "-w", created],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    ).stdout.strip()
    root = Path(converted)
    root.chmod(0o700)
    return root


def _write_executable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)
    _chmod(path, 0o755)


def _chmod(path: Path, mode: int) -> None:
    if os.name == "nt" and str(path).startswith(("\\\\wsl.localhost\\", "\\\\wsl$\\")):
        subprocess.run(
            ["wsl.exe", "-e", "chmod", f"{mode:o}", _posix_path(path)],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    else:
        path.chmod(mode)


FAKE_COMMAND = r'''#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

name = Path(sys.argv[0]).name
args = sys.argv[1:]
failure = os.environ.get("FAKE_FAIL", "")
cleanup_rm_failure = os.environ.get("FAKE_CLEANUP_RM_FAILURE", "")
trace_path = Path(os.environ["FAKE_TRACE"])


def trace(message):
    with trace_path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def fail_if(label, code=71):
    if failure == label:
        print(f"injected {label} failure", file=sys.stderr)
        raise SystemExit(code)


def require_pgdatabase():
    value = os.environ.get("PGDATABASE", "")
    expected = os.environ.get("FAKE_EXPECT_PGDATABASE")
    if not value or any("://" in item for item in args) or (expected is not None and value != expected):
        print("PGDATABASE transport contract violated", file=sys.stderr)
        raise SystemExit(72)


def block_if(label):
    if os.environ.get("FAKE_BLOCK") != label:
        return
    Path(os.environ["FAKE_BLOCK_READY"]).write_text(label, encoding="utf-8")
    while True:
        time.sleep(1)


def ascii_fold(value):
    return value.translate(str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"))


def remote_path(value):
    root_name = os.environ["FAKE_RCLONE_REMOTE_PATH"]
    root = Path(os.environ["FAKE_REMOTE_ROOT"])
    if value == root_name:
        return root
    prefix = root_name.rstrip("/") + "/"
    if not value.startswith(prefix):
        print("unexpected remote path", file=sys.stderr)
        raise SystemExit(73)
    relative = value[len(prefix):]
    if not relative or "/" in relative or relative in {".", ".."}:
        print("unsafe remote object", file=sys.stderr)
        raise SystemExit(74)
    folded = ascii_fold(relative)
    matches = [child for child in root.iterdir() if ascii_fold(child.name) == folded]
    if len(matches) > 1:
        print("ambiguous case-insensitive remote objects", file=sys.stderr)
        raise SystemExit(82)
    return matches[0] if matches else root / relative


if name == "tee":
    targets = [item for item in args if item != "-a" and not item.startswith("-")]
    if len(targets) != 1:
        raise SystemExit(87)
    marker = os.environ.get("FAKE_TEE_EXIT_AFTER", "")
    mode = "a" if "-a" in args else "w"
    with Path(targets[0]).open(mode, encoding="utf-8") as log_handle:
        for line in sys.stdin:
            log_handle.write(line)
            log_handle.flush()
            sys.stdout.write(line)
            sys.stdout.flush()
            if marker and marker in line:
                trace("tee:exit-after-marker")
                raise SystemExit(96)
elif name == "hostname":
    trace("hostname")
    print(os.environ.get("FAKE_HOST", "green"))
elif name == "date":
    if any("%Y%m%dT%H%M%SZ" in item for item in args):
        print(os.environ.get("FAKE_NOW", "20260629T230000Z"))
    else:
        value = os.environ.get("FAKE_NOW", "20260629T230000Z")
        print(f"{value[0:4]}-{value[4:6]}-{value[6:8]}T{value[9:11]}:{value[11:13]}:{value[13:15]}Z")
elif name == "df":
    trace("df")
    print("Avail")
    print(os.environ.get("FAKE_DF_AVAILABLE", "1000000000"))
elif name == "psql":
    require_pgdatabase()
    query = " ".join(args)
    if "pg_database_size" in query:
        trace("psql:size")
        print(os.environ.get("FAKE_DB_SIZE", "4096"))
    else:
        trace("psql:database")
        print(os.environ.get("FAKE_DB_NAME", "degen_green_prod"))
elif name == "pg_dump":
    require_pgdatabase()
    trace("pg_dump")
    output = None
    for index, item in enumerate(args):
        if item.startswith("--file="):
            output = item.split("=", 1)[1]
        elif item == "--file" and index + 1 < len(args):
            output = args[index + 1]
    if output is None:
        raise SystemExit(75)
    Path(output).write_bytes(b"DEGEN-CUSTOM-DUMP\x00verified-payload\n")
    block_if("pg_dump")
    fail_if("pg_dump")
elif name == "pg_restore":
    trace("pg_restore")
    dump = Path(args[-1])
    resolved_dump = dump.resolve(strict=True)
    trace(f"pg_restore-file:{resolved_dump.name}")
    fail_if("pg_restore")
    if os.environ.get("FAKE_PG_RESTORE_FAIL_BASENAME") == resolved_dump.name:
        raise SystemExit(76)
    if not dump.read_bytes().startswith(b"DEGEN-CUSTOM-DUMP"):
        raise SystemExit(76)
    print("; Archive created for tests")
elif name == "sha256sum":
    trace("sha256sum")
    fail_if("sha256sum")
    targets = [item for item in args if item != "--" and not item.startswith("-")]
    target = Path(targets[-1])
    trace(f"sha256sum-file:{target.resolve(strict=True).name}")
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    race_final = os.environ.get("FAKE_CREATE_LOCAL_FINAL")
    if race_final:
        Path(race_final).write_bytes(b"local-final-race-sentinel")
    print(f"{digest}  {target}")
elif name == "mktemp":
    trace("mktemp")
    template = args[-1]
    target = Path(template.replace("XXXXXXXX", "TESTTOKEN"))
    try:
        target.touch(exist_ok=False)
    except FileExistsError:
        raise SystemExit(80)
    print(target)
elif name == "rm":
    targets = [item for item in args if item != "--" and not item.startswith("-")]
    force = "-f" in args or "-rf" in args or "-fr" in args
    for item in targets:
        target = Path(item)
        trace(f"rm:{target.name}")
        cleanup_kind = ""
        if ".dump.sha256.partial." in target.name:
            cleanup_kind = "sidecar"
        elif ".dump.partial." in target.name:
            cleanup_kind = "dump"
        if cleanup_kind and cleanup_rm_failure in {"all", cleanup_kind}:
            print(f"injected local {cleanup_kind} cleanup failure", file=sys.stderr)
            raise SystemExit(88)
        try:
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        except FileNotFoundError:
            if not force:
                raise
elif name == "flock":
    trace("flock")
    race_path = os.environ.get("FAKE_FLOCK_REPLACE_LOCK")
    if race_path:
        target = Path(race_path)
        renamed = Path(os.environ["FAKE_FLOCK_RENAMED_LOCK"])
        target.replace(renamed)
        target.touch(mode=0o600)
        target.chmod(0o600)
    os.execv("/usr/bin/flock", ["flock", *args])
elif name == "rclone":
    immutable = "--immutable" in args
    ignore_existing = "--ignore-existing" in args
    error_on_no_transfer = "--error-on-no-transfer" in args
    config_path = None
    filtered = []
    index = 0
    while index < len(args):
        if args[index] == "--config" and index + 1 < len(args):
            config_path = args[index + 1]
            index += 2
            continue
        if args[index] in {"--immutable", "--ignore-existing", "--error-on-no-transfer"}:
            index += 1
            continue
        filtered.append(args[index])
        index += 1
    args = filtered
    if not args:
        raise SystemExit(77)
    operation = args[0]
    if os.environ.get("FAKE_RCLONE_REFRESH_CONFIG") == "1":
        if config_path is None:
            raise SystemExit(86)
        prior_calls = sum(
            line.startswith("rclone:")
            for line in trace_path.read_text(encoding="utf-8").splitlines()
        )
        config = Path(config_path)
        config_stat = config.stat()
        config_bytes = config.read_bytes()
        archive = config.with_name(config.name + f".prior-{prior_calls + 1}")
        os.link(config, archive)
        refreshed = config.with_name(config.name + ".refreshed")
        if os.environ.get("FAKE_RCLONE_REFRESH_PRESERVE_CONTENT") == "1":
            refreshed.write_bytes(config_bytes)
            os.utime(
                refreshed,
                ns=(config_stat.st_atime_ns, config_stat.st_mtime_ns),
            )
        else:
            refreshed.write_text(
                f"token = refreshed-rclone-secret-{prior_calls + 1}\n",
                encoding="utf-8",
            )
        refreshed.chmod(0o600)
        refreshed.replace(config)
    if operation == "lsf":
        trace("rclone:lsf")
        root = remote_path(args[1])
        for child in sorted(root.iterdir()):
            if child.is_file():
                print(child.name)
    elif operation == "copyto":
        source = Path(args[1])
        target = remote_path(args[2])
        kind = "sidecar" if ".sha256" in source.name else "dump"
        trace(f"rclone:copy:{kind}")
        trace(
            f"rclone:publication-flags:copy:{kind}:"
            f"ignore-existing={int(ignore_existing)}:"
            f"error-on-no-transfer={int(error_on_no_transfer)}"
        )
        if os.environ.get("FAKE_REQUIRE_STRICT_PUBLICATION_FLAGS") == "1" and not (
            ignore_existing and error_on_no_transfer
        ):
            raise SystemExit(84)
        if os.environ.get("FAKE_RACE_TEMP_ON") == kind:
            remote_root = Path(os.environ["FAKE_REMOTE_ROOT"])
            remote_prefix = os.environ["FAKE_RCLONE_REMOTE_PATH"].rstrip("/") + "/"
            requested_name = args[2][len(remote_prefix):]
            race_name = requested_name if os.environ.get("FAKE_RACE_TEMP_CASE") == "exact" else requested_name.swapcase()
            (remote_root / race_name).write_bytes(b"foreign-raced-temp")
            trace(f"rclone:race:{kind}:{race_name}")
            target = remote_path(args[2])
        if ignore_existing and target.exists():
            raise SystemExit(9 if error_on_no_transfer else 0)
        target.write_bytes(source.read_bytes())
        if os.environ.get("FAKE_CREATE_REMOTE_FINAL_ON") == kind:
            race_name = os.environ["FAKE_REMOTE_RACE_NAME"]
            (Path(os.environ["FAKE_REMOTE_ROOT"]) / race_name).write_bytes(b"remote-final-race-sentinel")
        block_if(f"copy_{kind}")
        fail_if(f"copy_{kind}")
    elif operation == "lsjson":
        values = [item for item in args[1:] if not item.startswith("-")]
        target = remote_path(values[0])
        stage = "temp" if target.name.lower().startswith(".degen-upload-") else "final"
        trace(f"rclone:{stage}-size")
        size = target.stat().st_size
        if failure == f"{stage}_size":
            size += 1
        print(json.dumps({"Path": target.name, "Name": target.name, "Size": size, "IsDir": False}))
    elif operation == "cat":
        target = remote_path(args[1])
        stage = "temp" if target.name.lower().startswith(".degen-upload-") else "final"
        trace(f"rclone:{stage}-content")
        if failure == f"{stage}_sidecar" or failure == f"{stage}_content":
            sys.stdout.buffer.write(b"incorrect sidecar\n")
        else:
            sys.stdout.buffer.write(target.read_bytes())
    elif operation == "moveto":
        source = remote_path(args[1])
        target = remote_path(args[2])
        kind = "sidecar" if ".sha256" in source.name else "dump"
        trace(f"rclone:move:{kind}")
        trace(
            f"rclone:publication-flags:move:{kind}:"
            f"ignore-existing={int(ignore_existing)}:"
            f"error-on-no-transfer={int(error_on_no_transfer)}"
        )
        fail_if(f"move_{kind}")
        if os.environ.get("FAKE_REQUIRE_STRICT_PUBLICATION_FLAGS") == "1" and not (
            ignore_existing and error_on_no_transfer
        ):
            raise SystemExit(84)
        if os.environ.get("FAKE_RACE_FINAL_ON") == kind:
            target.write_bytes(b"foreign-raced-final")
            trace(f"rclone:race-final:{kind}:{target.name}")
        if ignore_existing and target.exists():
            raise SystemExit(9 if error_on_no_transfer else 0)
        if os.environ.get("FAKE_MOVE_LEAVES_SOURCE") == kind:
            target.write_bytes(source.read_bytes())
        else:
            source.replace(target)
    elif operation == "deletefile":
        target = remote_path(args[1])
        trace(f"rclone:delete:{target.name}")
        delete_mode = os.environ.get("FAKE_DELETEFILE_MODE", "")
        if failure == "deletefile" or delete_mode == "fail-before":
            print("injected deletefile failure", file=sys.stderr)
            raise SystemExit(71)
        try:
            target.unlink()
        except FileNotFoundError:
            pass
        if delete_mode == "delete-then-fail":
            print("injected deletefile post-delete failure", file=sys.stderr)
            raise SystemExit(71)
    else:
        print(f"unexpected rclone operation: {operation}", file=sys.stderr)
        raise SystemExit(78)
    if os.environ.get("FAKE_RCLONE_REMOVE_CONFIG_AFTER") == operation:
        try:
            Path(config_path).unlink()
        except FileNotFoundError:
            pass
else:
    print(f"unexpected fake command: {name}", file=sys.stderr)
    raise SystemExit(79)
'''


def _planner_wrapper(real_planner: str) -> str:
    return textwrap.dedent(
        f'''\
        #!/usr/bin/env python3
        import hashlib
        import os
        from pathlib import Path
        import subprocess
        import sys

        args = sys.argv[1:]
        mode = args[args.index("--mode") + 1]
        output_format = args[args.index("--format") + 1]
        inventory = sys.stdin.buffer.read()
        inventory_digest = hashlib.sha256(inventory).hexdigest()
        with Path(os.environ["FAKE_TRACE"]).open("a", encoding="utf-8") as handle:
            handle.write(f"planner:{{mode}}\\n")
            handle.write(f"planner:{{mode}}:{{output_format}}:{{inventory_digest}}\\n")
            if os.environ.get("FAKE_TRACE_PLANNER_NOW") == "1":
                handle.write(f"planner-now:{{args[args.index('--now') + 1]}}\\n")
        if os.environ.get("FAKE_FAIL") in {{"planner", f"planner_{{mode}}"}}:
            raise SystemExit(81)
        completed = subprocess.run(
            ["/usr/bin/python3", {real_planner!r}, *args],
            input=inventory,
            check=False,
        )
        if completed.returncode == 0 and output_format == "delete-names":
            race_path = os.environ.get("FAKE_PLANNER_SYMLINK_PATH")
            race_target = os.environ.get("FAKE_PLANNER_SYMLINK_TARGET")
            if race_path and race_target:
                target = Path(race_path)
                target.unlink()
                target.symlink_to(race_target)
                with Path(os.environ["FAKE_TRACE"]).open("a", encoding="utf-8") as handle:
                    handle.write(f"planner:local:symlink-race:{{target.name}}\\n")
        raise SystemExit(completed.returncode)
        '''
    )


TEST_ENV_HELPER = r'''#!/usr/bin/env python3
import os
from pathlib import Path
import sys

if len(sys.argv) != 4 or sys.argv[1:3] != ["emit", "--file"]:
    raise SystemExit(64)
source = Path(sys.argv[3])
with Path(os.environ["FAKE_TRACE"]).open("a", encoding="utf-8") as handle:
    handle.write(f"env-helper:emit:{source}\n")
sys.stdout.buffer.write(source.read_bytes())
'''


@dataclass
class BackupHarness:
    root: Path
    bash: str
    backup_dir: Path
    log_dir: Path
    remote_dir: Path
    fake_bin: Path
    app_env: Path
    managed_env: Path
    env_helper: Path
    rclone_config: Path
    planner: Path
    trace: Path
    lock_file: Path
    runner: Path
    lock_holder: Path
    lock_fd_runner: Path
    signal_runner: Path

    @classmethod
    def create(cls, tmp_path: Path, bash: str) -> "BackupHarness":
        root = _native_behavior_root(tmp_path)
        backup_dir = root / "backups"
        log_dir = root / "logs"
        remote_dir = root / "remote"
        fake_bin = root / "fake-bin"
        config_dir = root / "config"
        lock_dir = root / "run"
        for path in (backup_dir, log_dir, remote_dir, fake_bin, config_dir, lock_dir):
            path.mkdir(parents=True, mode=0o700)
            _chmod(path, 0o700)

        for command in ("date", "df", "flock", "hostname", "mktemp", "pg_dump", "pg_restore", "psql", "rclone", "rm", "sha256sum"):
            _write_executable(fake_bin / command, FAKE_COMMAND)

        app_env = root / "web.env"
        app_env.write_text(f"DATABASE_URL='{SECRET}'\n", encoding="utf-8")
        _chmod(app_env, 0o600)
        rclone_config = config_dir / "rclone.conf"
        rclone_config.write_text("token = do-not-log-rclone-secret\n", encoding="utf-8")
        _chmod(rclone_config, 0o600)
        managed_env = root / "prod-db-backup.env"
        env_helper = root / "degen-prod-db-backup-env-test"
        _write_executable(env_helper, TEST_ENV_HELPER)
        planner = root / "retention-planner"
        _write_executable(planner, _planner_wrapper(_posix_path(PLANNER)))
        runner = root / "run-backup.sh"
        _write_executable(
            runner,
            '''#!/usr/bin/env bash
set -eu
export PATH="$FAKE_BIN:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
exec "$BACKUP_SCRIPT" "$@"
''',
        )
        lock_holder = root / "hold-lock.sh"
        _write_executable(
            lock_holder,
            '''#!/usr/bin/env bash
set -eu
umask 077
exec 9<>"$1"
/usr/bin/flock -n 9
printf 'READY\\n'
IFS= read -r _
''',
        )
        lock_fd_runner = root / "run-backup-with-lock-fd.sh"
        _write_executable(
            lock_fd_runner,
            '''#!/usr/bin/env bash
set -eu
export PATH="$FAKE_BIN:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
case "${FAKE_LOCK_FD_CASE:-valid}" in
    valid)
        exec 8<>"$FAKE_LOCK_FD_PATH"
        ;;
    closed)
        ;;
    wrong-file)
        exec 8<>"$FAKE_WRONG_LOCK_FD_PATH"
        ;;
    replaced-path)
        exec 8<>"$FAKE_LOCK_FD_PATH"
        mv -- "$FAKE_LOCK_FD_PATH" "$FAKE_LOCK_FD_RENAMED_PATH"
        : > "$FAKE_LOCK_FD_PATH"
        chmod 0600 "$FAKE_LOCK_FD_PATH"
        ;;
    *)
        exit 65
        ;;
esac
if [[ -n "${FAKE_LOCK_FD_NOW:-}" ]]; then
    exec "$BACKUP_SCRIPT" "$FAKE_LOCK_FD_MODE" --lock-fd 8 --now "$FAKE_LOCK_FD_NOW"
fi
exec "$BACKUP_SCRIPT" "$FAKE_LOCK_FD_MODE" --lock-fd 8
''',
        )
        signal_runner = root / "signal-backup.sh"
        _write_executable(
            signal_runner,
            '''#!/usr/bin/env bash
set -eu
export PATH="$FAKE_BIN:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
/usr/bin/setsid "$BACKUP_SCRIPT" >"$FAKE_SIGNAL_STDOUT" 2>"$FAKE_SIGNAL_STDERR" &
backup_pid=$!
ready=0
for _ in $(seq 1 200); do
    if [[ -f "$FAKE_BLOCK_READY" ]]; then
        ready=1
        break
    fi
    sleep 0.05
done
if [[ "$ready" != "1" ]]; then
    kill -KILL -- "-$backup_pid" 2>/dev/null || true
    wait "$backup_pid" 2>/dev/null || true
    exit 98
fi
kill -TERM -- "-$backup_pid"
set +e
wait "$backup_pid"
status=$?
set -e
exit "$status"
''',
        )
        trace = root / "trace.log"
        trace.write_text("", encoding="utf-8")
        _chmod(trace, 0o600)
        instance = cls(
            root=root,
            bash=bash,
            backup_dir=backup_dir,
            log_dir=log_dir,
            remote_dir=remote_dir,
            fake_bin=fake_bin,
            app_env=app_env,
            managed_env=managed_env,
            env_helper=env_helper,
            rclone_config=rclone_config,
            planner=planner,
            trace=trace,
            lock_file=lock_dir / "backup.lock",
            runner=runner,
            lock_holder=lock_holder,
            lock_fd_runner=lock_fd_runner,
            signal_runner=signal_runner,
        )
        instance.write_managed_environment(instance.managed_values())
        return instance

    @property
    def log_file(self) -> Path:
        return self.log_dir / "prod-db-backup.log"

    def managed_values(self) -> dict[str, str]:
        return {
            "APP_ENV_FILE": _posix_path(self.app_env),
            "BACKUP_DIR": _posix_path(self.backup_dir),
            "LOG_DIR": _posix_path(self.log_dir),
            "RCLONE_CONFIG": _posix_path(self.rclone_config),
            "RCLONE_REMOTE_PATH": "test:backups/degen-db",
            "KEEP_LOCAL_COUNT": "2",
            "KEEP_REMOTE_DAILY": "7",
            "KEEP_REMOTE_WEEKLY": "4",
            "KEEP_REMOTE_MONTHLY": "3",
            "REMOTE_PRUNE_ENABLED": "0",
            "MIN_FREE_AFTER_BYTES": "100",
            "RETENTION_PLANNER": _posix_path(self.planner),
            "LOCK_FILE": _posix_path(self.lock_file),
        }

    def write_managed_environment(self, values: dict[str, str]) -> None:
        content = "".join(f"{key}={value}\n" for key, value in values.items())
        self.managed_env.write_text(content, encoding="utf-8", newline="\n")
        _chmod(self.managed_env, 0o600)

    def environment(
        self,
        overrides: dict[str, str | None] | None = None,
        *,
        inherited_managed: dict[str, str] | None = None,
        managed_raw: str | None = None,
    ) -> dict[str, str]:
        managed_values = self.managed_values()
        values: dict[str, str] = {
            "FAKE_BIN": _posix_path(self.fake_bin),
            "FAKE_TRACE": _posix_path(self.trace),
            "FAKE_REMOTE_ROOT": _posix_path(self.remote_dir),
            "FAKE_RCLONE_REMOTE_PATH": managed_values["RCLONE_REMOTE_PATH"],
            "FAKE_DB_SIZE": "4096",
            "FAKE_DF_AVAILABLE": "1000000000",
            "FAKE_DB_NAME": "degen_green_prod",
            "FAKE_HOST": "green",
            "FAKE_NOW": STAMP,
            "BACKUP_SCRIPT": _posix_path(SCRIPT),
            "DEGEN_BACKUP_TEST_MODE": "1",
            "DEGEN_BACKUP_TEST_ENV_FILE": _posix_path(self.managed_env),
            "DEGEN_BACKUP_TEST_ENV_HELPER": _posix_path(self.env_helper),
        }
        if overrides:
            for key, value in overrides.items():
                target = managed_values if key in MANAGED_KEYS else values
                if value is None:
                    target.pop(key, None)
                else:
                    target[key] = value
        values.setdefault("FAKE_RCLONE_REMOTE_PATH", managed_values.get("RCLONE_REMOTE_PATH", ""))
        if managed_raw is None:
            self.write_managed_environment(managed_values)
        else:
            self.managed_env.write_text(managed_raw, encoding="utf-8", newline="\n")
            _chmod(self.managed_env, 0o600)

        env = os.environ.copy()
        env.pop("DATABASE_URL", None)
        env.pop("BACKUP_ENV_FILE", None)
        env.pop("ENV_HELPER", None)
        env.pop("LOG_FILE", None)
        for key in MANAGED_KEYS:
            env.pop(key, None)
        env.update(values)
        if inherited_managed:
            env.update(inherited_managed)
        if os.name == "nt":
            env["WSLENV"] = ":".join(sorted({*values, *(inherited_managed or {})}))
        else:
            env["PATH"] = f"{values['FAKE_BIN']}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        return env

    def run_with_lock_fd(
        self,
        mode: str,
        *,
        fd_case: str = "valid",
        frozen_now: str | None = None,
        overrides: dict[str, str | None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if not self.lock_file.exists() and not _is_symlink(self.lock_file):
            self.lock_file.touch(mode=0o600)
            _chmod(self.lock_file, 0o600)
        wrong_file = self.root / "wrong.lock"
        wrong_file.touch(mode=0o600, exist_ok=True)
        _chmod(wrong_file, 0o600)
        renamed_file = self.root / "renamed.lock"
        env = self.environment(
            {
                **(overrides or {}),
                "FAKE_LOCK_FD_MODE": mode,
                "FAKE_LOCK_FD_CASE": fd_case,
                "FAKE_LOCK_FD_PATH": _posix_path(self.lock_file),
                "FAKE_WRONG_LOCK_FD_PATH": _posix_path(wrong_file),
                "FAKE_LOCK_FD_RENAMED_PATH": _posix_path(renamed_file),
                "FAKE_LOCK_FD_NOW": frozen_now,
            }
        )
        return subprocess.run(
            [self.bash, _posix_path(self.lock_fd_runner)],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def run(
        self,
        mode: str | None = None,
        *,
        overrides: dict[str, str | None] | None = None,
        inherited_managed: dict[str, str] | None = None,
        managed_raw: str | None = None,
        extra_args: tuple[str, ...] = (),
    ) -> subprocess.CompletedProcess[str]:
        env = self.environment(
            overrides,
            inherited_managed=inherited_managed,
            managed_raw=managed_raw,
        )
        arguments = ([] if mode is None else [mode]) + list(extra_args)
        command = [self.bash, _posix_path(self.runner), *arguments]
        return subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    def terminate_while(self, blocked_operation: str) -> subprocess.CompletedProcess[str]:
        ready = self.root / "blocked.ready"
        signal_stdout = self.root / "signal.stdout"
        signal_stderr = self.root / "signal.stderr"
        env = self.environment(
            {
                "FAKE_BLOCK": blocked_operation,
                "FAKE_BLOCK_READY": _posix_path(ready),
                "FAKE_SIGNAL_STDOUT": _posix_path(signal_stdout),
                "FAKE_SIGNAL_STDERR": _posix_path(signal_stderr),
            }
        )
        return subprocess.run(
            [self.bash, _posix_path(self.signal_runner)],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )

    def trace_lines(self) -> list[str]:
        return self.trace.read_text(encoding="utf-8").splitlines()


@pytest.fixture
def harness(tmp_path: Path) -> BackupHarness:
    if BASH is None:
        pytest.skip("No usable POSIX Bash/WSL environment")
    instance = BackupHarness.create(tmp_path, BASH)
    try:
        yield instance
    finally:
        if instance.root != tmp_path:
            shutil.rmtree(instance.root, ignore_errors=True)


def _seed_pair(directory: Path, stamp: str, *, prefix: str = PREFIX) -> tuple[str, str]:
    dump_name = f"{prefix}{stamp}.dump"
    sidecar_name = f"{dump_name}.sha256"
    payload = f"DEGEN-CUSTOM-DUMP\x00prior-{stamp}\n".encode()
    dump_path = directory / dump_name
    sidecar_path = directory / sidecar_name
    dump_path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    sidecar_path.write_bytes(f"{digest}  {dump_name}\n".encode("ascii"))
    _chmod(dump_path, 0o600)
    _chmod(sidecar_path, 0o600)
    return dump_name, sidecar_name


def _seed_old_pairs(directory: Path) -> list[str]:
    return [
        name
        for stamp in ("20260626T230000Z", "20260627T230000Z", "20260628T230000Z")
        for name in _seed_pair(directory, stamp)
    ]


def _pair_names(stamp: str = STAMP) -> tuple[str, str]:
    dump = f"{PREFIX}{stamp}.dump"
    return dump, f"{dump}.sha256"


def _remote_delete_names(trace: list[str]) -> list[str]:
    return [line.removeprefix("rclone:delete:") for line in trace if line.startswith("rclone:delete:")]


def _local_retention_delete_names(trace: list[str]) -> list[str]:
    pattern = re.compile(rf"^rm:({re.escape(PREFIX)}\d{{8}}T\d{{6}}Z\.dump(?:\.sha256)?)$")
    return [match.group(1) for line in trace if (match := pattern.fullmatch(line))]


def _write_format_planner(path: Path, *, keep: list[str], delete: list[str]) -> None:
    _write_executable(
        path,
        textwrap.dedent(
            f'''\
            #!/usr/bin/env python3
            import sys

            output_format = sys.argv[sys.argv.index("--format") + 1]
            outputs = {{"keep-names": {keep!r}, "delete-names": {delete!r}}}
            for name in outputs[output_format]:
                print(name)
            '''
        ),
    )


def _write_raw_format_planner(path: Path, *, keep_expression: str, delete_expression: str) -> None:
    _write_executable(
        path,
        textwrap.dedent(
            f'''\
            #!/usr/bin/env python3
            import sys

            output_format = sys.argv[sys.argv.index("--format") + 1]
            if output_format == "keep-names":
                payload = {keep_expression}
            else:
                payload = {delete_expression}
            sys.stdout.buffer.write(payload)
            '''
        ),
    )


def _cleanup_warning(category: str, basename: str) -> str:
    return f"[2026-06-29T23:00:00Z] WARNING: backup cleanup failed ({category}): {basename}"


def _run_fake_rclone(
    harness: BackupHarness,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    env = harness.environment()
    runner = harness.root / "run-fake-rclone.sh"
    _write_executable(
        runner,
        '''#!/usr/bin/env bash
set -eu
export PATH="$FAKE_BIN:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
exec rclone "$@"
''',
    )
    return subprocess.run(
        [harness.bash, _posix_path(runner), *arguments],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _receipt_records(harness: BackupHarness) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for line in harness.log_file.read_text(encoding="utf-8").splitlines():
        marker = "RCLONE_CONFIG_RECEIPT "
        if marker not in line:
            continue
        payload = line.split(marker, 1)[1]
        records.append(dict(field.split("=", 1) for field in payload.split()))
    return records


def _assert_no_backup_side_effects(
    harness: BackupHarness, *, helper_may_run: bool
) -> None:
    trace = harness.trace_lines()
    allowed_prefixes = ("env-helper:",) if helper_may_run else ()
    assert all(line.startswith(allowed_prefixes) for line in trace)
    assert not harness.log_file.exists()
    assert not harness.lock_file.exists()


def _assert_lock_rejected_before_external_work(
    harness: BackupHarness, result: subprocess.CompletedProcess[str]
) -> None:
    assert "Invalid backup lock" in result.stdout + result.stderr
    assert f"env-helper:emit:{_posix_path(harness.managed_env)}" in harness.trace_lines()
    assert "psql:database" not in harness.trace_lines()
    assert not any(line.startswith("rclone:") for line in harness.trace_lines())


def _assert_rclone_config_rejected_before_rclone(
    harness: BackupHarness, result: subprocess.CompletedProcess[str]
) -> None:
    assert "Invalid rclone configuration" in result.stdout + result.stderr
    assert f"env-helper:emit:{_posix_path(harness.managed_env)}" in harness.trace_lines()
    assert not any(line.startswith("rclone:") for line in harness.trace_lines())


def _parse_unit(source: str) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    for raw_line in source.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            assert section not in sections, f"duplicate unit section: {section}"
            current = sections.setdefault(section, {})
            continue
        assert current is not None and "=" in line, f"invalid unit line: {raw_line}"
        key, value = line.split("=", 1)
        assert key not in current, f"duplicate unit directive: {key}"
        current[key] = value
    return sections


def _systemd_analyze_prefix() -> list[str] | None:
    if os.name == "nt":
        wsl = shutil.which("wsl.exe")
        if wsl is None:
            return None
        probe = subprocess.run(
            [wsl, "-e", "sh", "-lc", "command -v systemd-analyze >/dev/null 2>&1"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return [wsl, "-e", "systemd-analyze"] if probe.returncode == 0 else None
    executable = shutil.which("systemd-analyze")
    return [executable] if executable is not None else None


def test_source_declares_safe_shell_contract_and_live_defaults() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert source.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in source
    assert re.search(r"^umask 077$", source, re.MULTILINE)
    assert "set -x" not in source
    assert source.count("--ignore-existing --error-on-no-transfer") == 4
    assert "--immutable" not in source
    assert "os.O_NOFOLLOW" in source
    assert 'getattr(os, name, 0)' not in source
    assert "trap 'exit 129' HUP" in source
    assert "trap 'exit 130' INT" in source
    assert "trap 'exit 143' TERM" in source
    assert "owned_local_dump_partial=$(mktemp" in source
    assert "owned_local_sidecar_partial=$(mktemp" in source
    assert "owned_upload_token_file=$(mktemp" in source
    assert not re.search(r"if ! dump_partial=\$\(mktemp", source)
    assert not re.search(r"if ! sidecar_partial=\$\(mktemp", source)
    assert f'FIXED_BACKUP_ENV_FILE="{FIXED_BACKUP_ENV_FILE}"' in source
    assert f'FIXED_ENV_HELPER="{FIXED_ENV_HELPER}"' in source
    assert 'LOG_FILE="$LOG_DIR/prod-db-backup.log"' in source
    for key in MANAGED_KEYS:
        assert not re.search(rf"^{key}=\$\{{{key}:-", source, re.MULTILINE)
    assert not re.search(r"^LOG_FILE=\$\{LOG_FILE:-", source, re.MULTILINE)
    assert not re.search(r"^\s*(?:source|\.)\s", source, re.MULTILINE)
    assert not re.search(r"\beval\s", source)


def test_systemd_service_is_exact_oneshot_with_postgres_compatible_hardening() -> None:
    unit = SERVICE.read_text(encoding="utf-8")

    assert _parse_unit(unit) == {
        "Unit": {
            "Description": "Degen PostgreSQL verified backup",
            "After": "network-online.target postgresql.service",
            "Wants": "network-online.target",
            "RefuseManualStart": "yes",
        },
        "Service": {
            "Type": "oneshot",
            "User": "root",
            "Group": "root",
            "Environment": "BACKUP_ENV_FILE=/etc/degen/prod-db-backup.env",
            "ExecStart": "/usr/local/sbin/degen-prod-db-backup",
            "RuntimeDirectory": "degen-prod-db-backup",
            "RuntimeDirectoryMode": "0700",
            "RuntimeDirectoryPreserve": "yes",
            "TimeoutStartSec": "infinity",
            "TimeoutStopSec": "90",
            "KillMode": "control-group",
            "Nice": "10",
            "IOSchedulingClass": "best-effort",
            "IOSchedulingPriority": "7",
            "UMask": "0077",
            "NoNewPrivileges": "true",
            "PrivateTmp": "true",
            "PrivateDevices": "true",
            "ProtectHome": "true",
            "ProtectSystem": "full",
            "ReadWritePaths": "/etc/degen",
            "ProtectKernelTunables": "true",
            "ProtectKernelModules": "true",
            "ProtectControlGroups": "true",
            "RestrictSUIDSGID": "true",
            "LockPersonality": "true",
            "RestrictAddressFamilies": "AF_UNIX AF_INET AF_INET6",
        },
    }
    assert "[Install]" not in unit
    assert not re.search(r"^Restart(?:Sec)?=", unit, re.MULTILINE)
    assert "StandardOutput=" not in unit and "StandardError=" not in unit
    for unrelated_service in ("degen-web.service", "degen-worker.service", "degen-ops-discord-bot.service"):
        assert unrelated_service not in unit
    assert not re.search(r"^Exec\w*=.*\bsystemctl\b", unit, re.MULTILINE)


def test_timer_preserves_exact_green_schedule_and_persistence() -> None:
    timer = TIMER.read_text(encoding="utf-8")

    assert _parse_unit(timer) == {
        "Unit": {"Description": "Nightly Degen PostgreSQL verified backup"},
        "Timer": {
            "Unit": "degen-prod-db-backup.service",
            "OnCalendar": "*-*-* 03:15:00 America/Los_Angeles",
            "RandomizedDelaySec": "20m",
            "AccuracySec": "1m",
            "Persistent": "true",
        },
        "Install": {"WantedBy": "timers.target"},
    }


def test_env_template_has_exact_secret_free_defaults_and_preservation_warning() -> None:
    template = ENV_TEMPLATE.read_text(encoding="utf-8")
    assignments = {
        key: value
        for line in template.splitlines()
        if (stripped := line.strip()) and not stripped.startswith("#")
        for key, value in [stripped.split("=", 1)]
    }

    assert assignments == {
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
    lowered = template.lower()
    for forbidden in ("database_url", "token", "password", "secret", "openclaw-9902ae", "brev"):
        assert forbidden not in lowered
    assert not re.search(r"^KEEP_[A-Z0-9_]*_DAYS=", template, re.MULTILINE)
    assert "Never overwrite /etc/degen/prod-db-backup.env from this template." in template
    assert "root:root mode 0600" in template
    assert "edit and preserve" in lowered
    assert "BACKUP_PREFIX is host-derived" in template
    assert "not a tracked default" in template


@pytest.mark.parametrize("mode", ["preflight", "remote-retention-dry-run"])
def test_direct_modes_load_the_same_single_managed_configuration(
    harness: BackupHarness, mode: str
) -> None:
    result = harness.run(mode)

    assert result.returncode == 0, result.stdout + result.stderr
    assert harness.trace_lines().count(
        f"env-helper:emit:{_posix_path(harness.managed_env)}"
    ) == 1


@pytest.mark.parametrize("variable", ["BACKUP_ENV_FILE", "ENV_HELPER"])
def test_fixed_runtime_config_paths_accept_absent_or_identical_values(
    harness: BackupHarness, variable: str
) -> None:
    fixed = FIXED_BACKUP_ENV_FILE if variable == "BACKUP_ENV_FILE" else FIXED_ENV_HELPER

    absent = harness.run("preflight")
    harness.trace.write_text("", encoding="utf-8")
    identical = harness.run("preflight", overrides={variable: fixed})

    assert absent.returncode == 0, absent.stdout + absent.stderr
    assert identical.returncode == 0, identical.stdout + identical.stderr


@pytest.mark.parametrize("variable", ["BACKUP_ENV_FILE", "ENV_HELPER"])
def test_alternate_production_runtime_config_paths_fail_before_side_effects(
    harness: BackupHarness, variable: str
) -> None:
    harness.backup_dir.rmdir()
    harness.log_dir.rmdir()
    alternate = f"/tmp/attacker-{variable.lower()}-must-not-leak"

    result = harness.run("preflight", overrides={variable: alternate})

    assert result.returncode != 0
    assert "Invalid backup runtime configuration" in result.stdout + result.stderr
    assert alternate not in result.stdout + result.stderr
    assert not harness.backup_dir.exists()
    assert not harness.log_dir.exists()
    _assert_no_backup_side_effects(harness, helper_may_run=False)


@pytest.mark.parametrize(
    ("inherited", "expected_success"),
    [
        ({"KEEP_LOCAL_COUNT": "2"}, True),
        ({"KEEP_LOCAL_COUNT": "99"}, False),
        ({"APP_ENV_FILE": "/tmp/different-web.env"}, False),
        ({"BACKUP_PREFIX": PREFIX}, False),
    ],
)
def test_inherited_managed_values_must_match_helper_output(
    harness: BackupHarness,
    inherited: dict[str, str],
    expected_success: bool,
) -> None:
    result = harness.run("preflight", inherited_managed=inherited)

    assert (result.returncode == 0) is expected_success, result.stdout + result.stderr
    if not expected_success:
        combined = result.stdout + result.stderr
        assert "Invalid managed backup configuration" in combined
        assert not any(value in combined for value in inherited.values())
        assert "psql:database" not in harness.trace_lines()
        assert not any(line.startswith("rclone:") for line in harness.trace_lines())


def test_inherited_backup_prefix_is_accepted_only_when_helper_emits_same_value(
    harness: BackupHarness,
) -> None:
    values = harness.managed_values()
    values["BACKUP_PREFIX"] = PREFIX
    raw = "".join(f"{key}={value}\n" for key, value in values.items())

    result = harness.run(
        "preflight",
        inherited_managed={"BACKUP_PREFIX": PREFIX},
        managed_raw=raw,
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "mutation",
    [
        "UNKNOWN=value\n",
        "KEEP_LOCAL_COUNT=2\n",
        "\n",
        "BACKUP_PREFIX=one_\nBACKUP_PREFIX=two_\n",
    ],
)
def test_unsupported_blank_or_duplicate_helper_output_fails_before_side_effects(
    harness: BackupHarness, mutation: str
) -> None:
    raw = "".join(
        f"{key}={value}\n" for key, value in harness.managed_values().items()
    ) + mutation
    harness.backup_dir.rmdir()
    harness.log_dir.rmdir()

    result = harness.run("preflight", managed_raw=raw)

    assert result.returncode != 0
    assert "Invalid managed backup configuration" in result.stdout + result.stderr
    assert not harness.backup_dir.exists()
    assert not harness.log_dir.exists()
    _assert_no_backup_side_effects(harness, helper_may_run=True)


def test_helper_output_is_never_evaluated_or_sourced(
    harness: BackupHarness,
) -> None:
    marker = harness.root / "must-not-run"
    raw = "".join(
        f"{key}={value}\n" for key, value in harness.managed_values().items()
    ) + f"BACKUP_DIR=$(touch {_posix_path(marker)})\n"

    result = harness.run("preflight", managed_raw=raw)

    assert result.returncode != 0
    assert "Invalid managed backup configuration" in result.stdout + result.stderr
    assert not marker.exists()
    assert "must-not-run" not in result.stdout + result.stderr
    assert "psql:database" not in harness.trace_lines()
    assert not any(line.startswith("rclone:") for line in harness.trace_lines())


@pytest.mark.parametrize("mutation", ["missing-default", "missing-final-newline"])
def test_incomplete_helper_output_is_rejected_before_side_effects(
    harness: BackupHarness, mutation: str
) -> None:
    values = harness.managed_values()
    if mutation == "missing-default":
        values.pop("LOCK_FILE")
    raw = "".join(f"{key}={value}\n" for key, value in values.items())
    if mutation == "missing-final-newline":
        raw = raw.rstrip("\n")

    result = harness.run("preflight", managed_raw=raw)

    assert result.returncode != 0
    assert "Invalid managed backup configuration" in result.stdout + result.stderr
    _assert_no_backup_side_effects(harness, helper_may_run=True)


def test_arbitrary_inherited_log_file_is_rejected_before_logging(
    harness: BackupHarness,
) -> None:
    alternate = _posix_path(harness.root / "attacker.log")

    result = harness.run("preflight", overrides={"LOG_FILE": alternate})

    assert result.returncode != 0
    assert "Invalid managed backup configuration" in result.stdout + result.stderr
    assert not (harness.root / "attacker.log").exists()
    assert not harness.log_file.exists()
    assert "psql:database" not in harness.trace_lines()


def test_root_execution_rejects_explicit_harness_injection(
    harness: BackupHarness,
) -> None:
    if os.name != "nt":
        pytest.skip("root test-mode rejection is exercised through WSL on Windows")
    env = harness.environment()

    result = subprocess.run(
        [
            "wsl.exe",
            "-u",
            "root",
            "-e",
            "bash",
            _posix_path(harness.runner),
            "preflight",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode != 0
    assert "Test backup configuration is not permitted" in result.stdout + result.stderr
    _assert_no_backup_side_effects(harness, helper_may_run=False)


def test_task9_runbook_bootstrap_and_source_success_path_are_ordered() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    old_plan = OLD_PLAN.read_text(encoding="utf-8")
    expected_archive_paths = (
        "deploy/linux/degen-prod-db-backup-assets.sha256",
        "deploy/linux/degen-prod-db-backup-env.py",
        "deploy/linux/degen-prod-db-backup-ops.py",
        "deploy/linux/degen-prod-db-backup.sh",
        "deploy/linux/degen-prod-db-retention.py",
        "deploy/systemd/degen-prod-db-backup.env.example",
        "deploy/systemd/degen-prod-db-backup.service",
        "deploy/systemd/degen-prod-db-backup.timer",
    )
    expected_archive_members = (
        "deploy/",
        "deploy/linux/",
        *expected_archive_paths[:5],
        "deploy/systemd/",
        *expected_archive_paths[5:],
    )

    def shell_array(name: str) -> tuple[str, ...]:
        match = re.search(
            rf"(?ms)^{re.escape(name)}=\(\n(?P<body>.*?)^\)$",
            runbook,
        )
        assert match is not None, f"missing exact {name} array"
        return tuple(
            line.strip().strip("'\"")
            for line in match.group("body").splitlines()
            if line.strip()
        )

    assert "SUPERSEDED FOR PRODUCTION EXECUTION" in old_plan
    push_gate = runbook.index("## Gate 1: push the exact reviewed commit")
    ref_check = runbook.index('git check-ref-format "$REMOTE_REF"')
    push_url_check = runbook.index("git remote get-url --push --all origin")
    push_url_equality = runbook.index(
        'test "${ORIGIN_PUSH_URLS[0]}" = "$CANONICAL_REMOTE_URL"'
    )
    push = runbook.index('git push origin "$REVIEWED_SHA:$REMOTE_REF"')
    remote_sha = runbook.index(
        'REMOTE_BRANCH_SHA="$(git ls-remote --exit-code --refs origin "$REMOTE_REF"'
    )
    remote_equality = runbook.index('test "$REMOTE_BRANCH_SHA" = "$REVIEWED_SHA"')
    archive_creation = runbook.index("git -c tar.umask=0002 archive --format=tar")
    local_embedded_commit = runbook.index('git get-tar-commit-id < "$ARCHIVE_LOCAL"')
    local_member_types = runbook.index('tar --list --verbose --file "$ARCHIVE_LOCAL"')
    local_extraction = runbook.index('tar --extract --file "$ARCHIVE_LOCAL"')
    local_manifest_parity = runbook.index("sha256sum --check --strict", local_extraction)
    production_gate = runbook.index("## Gate 2: approve production installation")
    assignments = (
        'UTC_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"',
        'OPERATION_DIR="/opt/degen/backups/config/$UTC_STAMP"',
        'SOURCE_OPS="$OPERATION_DIR/source/deploy/linux/degen-prod-db-backup-ops.py"',
        'MANIFEST_SHA256="${APPROVED_MANIFEST_SHA256:?set the approved reviewed-manifest SHA-256}"',
    )
    assignment_positions = [runbook.index(value) for value in assignments]
    operation_creation = runbook.index('mkdir -m 0700 -- "$OPERATION_DIR"')
    transfer_creation = runbook.index('mkdir -m 0700 -- "$TRANSFER_DIR"')
    archive_transfer = runbook.index("brev copy --host")
    archive_digest = runbook.index(
        'printf \'%s  %s\\n\' "$ARCHIVE_SHA256"', archive_transfer
    )
    embedded_commit = runbook.index("git get-tar-commit-id", archive_digest)
    member_types = runbook.index("tar --list --verbose", embedded_commit)
    extraction = runbook.index('tar --extract --file "$OPERATION_DIR/source.tar"', member_types)
    manifest_digest = runbook.index('printf \'%s  %s\\n\' "$MANIFEST_SHA256"')
    manifest_parity = runbook.index("sha256sum --check --strict", extraction)
    verify_source = runbook.index('"$SOURCE_OPS" verify-source --operation-dir "$OPERATION_DIR"')
    prepare = runbook.index('"$SOURCE_OPS" prepare-staging --operation-dir "$OPERATION_DIR"')
    snapshot = runbook.index('"$SOURCE_OPS" snapshot --operation-dir "$OPERATION_DIR"')
    install = runbook.index('"$SOURCE_OPS" install --operation-dir "$OPERATION_DIR"')

    assert push_gate < ref_check < push_url_check < push_url_equality < push
    assert push < remote_sha < remote_equality < archive_creation
    assert archive_creation < local_embedded_commit < local_member_types < local_extraction
    assert local_extraction < local_manifest_parity < production_gate
    assert 'REMOTE_REF="${APPROVED_REMOTE_REF:?set the exact approved refs/heads/... ref}"' in runbook
    assert "test \"$REMOTE_REF\" = refs/heads/codex/backup-retention-hardening" in runbook
    assert "https://github.com/Degen-Collectibles/degen-deal-parser.git" in runbook
    assert runbook.count("git remote get-url --all origin") == 2
    assert runbook.count("git remote get-url --push --all origin") == 2
    assert runbook.count('test "${#ORIGIN_FETCH_URLS[@]}" -eq 1') == 2
    assert runbook.count('test "${#ORIGIN_PUSH_URLS[@]}" -eq 1') == 2
    assert not re.search(r"git remote get-url(?! --(?:push )?--all) origin", runbook)
    assert runbook.count("git -c tar.umask=0002 archive --format=tar") == 1
    assert shell_array("ARCHIVE_PATHS") == expected_archive_paths
    assert shell_array("EXPECTED_ARCHIVE_MEMBERS") == expected_archive_members
    assert assignment_positions == sorted(assignment_positions)
    assert production_gate < assignment_positions[0]
    assert max(assignment_positions) < transfer_creation < operation_creation < archive_transfer
    assert 'TRANSFER_DIR="/tmp/degen-backup-transfer-$TRANSFER_TOKEN"' in runbook
    assert 'REMOTE_ARCHIVE="$TRANSFER_DIR/source.tar"' in runbook
    assert 'EVIDENCE_DIR="$(mktemp -d /tmp/degen-backup-evidence.XXXXXXXX)"' in runbook
    assert 'PREPARE_SCRIPT="$(mktemp /tmp/degen-backup-prepare.XXXXXXXX)"' in runbook
    assert 'BOOTSTRAP_SCRIPT="$(mktemp /tmp/degen-backup-bootstrap.XXXXXXXX)"' in runbook
    assert archive_transfer < archive_digest < embedded_commit < member_types < extraction
    assert extraction < manifest_digest < manifest_parity < verify_source < prepare < snapshot < install
    assert 'test ! -e "$SOURCE_DIR"' in runbook
    assert "umask 077" in runbook
    assert "--no-same-owner" in runbook
    assert "--no-same-permissions" in runbook
    assert 'find "$SOURCE_DIR" -xdev' in runbook
    assert '--expected-manifest-sha256 "$MANIFEST_SHA256"' in runbook
    assert "deploy/linux/degen-prod-db-backup-assets.sha256" in runbook
    for asset in (
        "deploy/linux/degen-prod-db-backup.sh",
        "deploy/linux/degen-prod-db-retention.py",
        "deploy/linux/degen-prod-db-backup-env.py",
        "deploy/linux/degen-prod-db-backup-ops.py",
        "deploy/systemd/degen-prod-db-backup.service",
        "deploy/systemd/degen-prod-db-backup.timer",
        "deploy/systemd/degen-prod-db-backup.env.example",
    ):
        assert asset in runbook


def test_task9_runbook_separates_recovery_and_installed_helper_gates() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    lowered = runbook.lower()
    verify_source = runbook.index('"$SOURCE_OPS" verify-source --operation-dir "$OPERATION_DIR"')
    install = runbook.index('"$SOURCE_OPS" install --operation-dir "$OPERATION_DIR"')
    recovery_heading = runbook.index("## Conditional recovery only")
    recovery = runbook.index('"$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"')
    installed_hash_gate = runbook.index("EXPECTED_INSTALLED_OPS_SHA256")
    probe = runbook.index("/usr/local/sbin/degen-prod-db-backup-ops probe-remote")
    dry_run = runbook.index("/usr/local/sbin/degen-prod-db-backup-ops record-dry-run")
    prune_gate = runbook.index("## Gate 3: approve remote pruning")
    enable = runbook.index("/usr/local/sbin/degen-prod-db-backup-ops enable-prune")
    observe = runbook.index("/usr/local/sbin/degen-prod-db-backup-ops observe")
    dry_run_state = runbook.index(
        "/usr/local/sbin/degen-prod-db-backup-ops show-state", dry_run
    )
    enable_state = runbook.index(
        "/usr/local/sbin/degen-prod-db-backup-ops show-state", enable
    )
    observe_state = runbook.index(
        "/usr/local/sbin/degen-prod-db-backup-ops show-state", observe
    )
    rollback_heading = runbook.index("## Separately approved manual rollback")
    rollback = runbook.index('"$SOURCE_OPS" rollback --operation-dir "$OPERATION_DIR"')
    installed_hash_checks = [
        match.start()
        for match in re.finditer(
            r"(?m)^printf '%s  %s\\n' \"\$EXPECTED_INSTALLED_OPS_SHA256\" "
            r"/usr/local/sbin/degen-prod-db-backup-ops \| sha256sum --check --strict -$",
            runbook,
        )
    ]
    normal_section = runbook[runbook.index("### Standard-tool bootstrap"):recovery_heading]
    recovery_section = runbook[recovery_heading:prune_gate]
    rollback_section = runbook[rollback_heading:]

    assert verify_source < install < installed_hash_gate < probe < dry_run < prune_gate < enable < observe
    assert dry_run < dry_run_state < prune_gate
    assert enable < enable_state < observe < observe_state
    assert install < recovery_heading < recovery
    assert len(installed_hash_checks) == 4
    assert (
        install
        < installed_hash_checks[0]
        < probe
        < installed_hash_checks[1]
        < dry_run
        < prune_gate
        < installed_hash_checks[2]
        < enable
        < installed_hash_checks[3]
        < observe
    )
    assert '"$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"' not in normal_section
    assert '"$SOURCE_OPS" show-state --operation-dir "$OPERATION_DIR"' in recovery_section
    assert '"$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"' in recovery_section
    assert runbook.count('"$SOURCE_OPS" recover --operation-dir "$OPERATION_DIR"') == 1
    assert rollback_heading < rollback
    assert runbook.count('"$SOURCE_OPS" rollback --operation-dir "$OPERATION_DIR"') == 1
    assert '"$SOURCE_OPS" rollback --operation-dir "$OPERATION_DIR"' in rollback_section
    interrupted_phases = (
        "installing|recovering|probing|dry_run_recording|policy_enabling|observing|"
        "recovery_required|recovering_policy|manual_rollback|recovering_probe|recovering_guard"
    )
    assert interrupted_phases in runbook
    assert "Gate 2 for install/probe/dry-run phases" in runbook
    assert "Gate 3 for" in runbook and "policy/observation phases" in runbook
    assert "separately approved manual rollback" in runbook
    assert "An earlier Gate 2 approval never authorizes recovery" in runbook
    assert "persistent catch-up or any" in lowered
    assert "ordinary scheduled run before gate 3" in lowered
    assert "any catch-up or later scheduled run after timer restoration" in lowered
    assert "approved local newest-2 policy" in lowered
    assert "recovery and rollback cannot" in lowered
    assert "restore those deleted dumps" in lowered
    assert "zero candidates still require approval" in lowered
    assert "timer stop/start" in lowered
    assert "rclone configuration may refresh" in lowered
    assert "remote probe creates and deletes" in lowered
    assert "remote deletion is potentially irreversible" in lowered
    assert "pg_restore --list" in runbook
    assert "does not prove an end-to-end logical restore" in lowered
    assert "rclone.conf.audit" in runbook
    assert "does not automatically restore" in lowered
    assert "cannot restore deleted local backups" in lowered
    assert "deleted onedrive objects" in lowered
    assert "CONFIG_BACKUP_DIR" not in runbook
    assert "# BEGIN " not in runbook
    assert "sudo install -o root" not in runbook
    assert "python3 - <<" not in runbook
    assert "sudo -E" not in runbook
    assert "set -x" not in runbook
    assert not re.search(r"(?m)^[ \t]*(?:sudo[ \t]+)?rclone\b", runbook)
    assert not re.search(
        r"(?mi)^[ \t]*(?:sudo[ \t]+)?(?:/usr/bin/)?systemctl[ \t]+"
        r"(?:start|stop|restart|daemon-reload)\b",
        runbook,
    )
    assert not re.search(
        r"(?mi)^[ \t]*(?:sudo[ \t]+)?(?:source|\.)[ \t]+"
        r"/(?:etc/degen/(?:prod-db-backup\.env|rclone\.conf)|opt/degen/web\.env)\b",
        runbook,
    )
    assert not re.search(
        r"(?mi)^[ \t]*(?:sudo[ \t]+)?cat[ \t]+"
        r"/(?:etc/degen/(?:prod-db-backup\.env|rclone\.conf)|opt/degen/web\.env)\b",
        runbook,
    )
    protected_targets = (
        "/usr/local/sbin/degen-prod-db-backup",
        "/usr/local/sbin/degen-prod-db-retention",
        "/usr/local/sbin/degen-prod-db-backup-env",
        "/usr/local/sbin/degen-prod-db-backup-ops",
        "/etc/systemd/system/degen-prod-db-backup.service",
        "/etc/systemd/system/degen-prod-db-backup.timer",
        "/etc/degen/prod-db-backup.env",
    )
    for target in protected_targets:
        assert not re.search(
            rf"(?mi)^[ \t]*(?:sudo[ \t]+)?(?:install|cp|mv|rm|tee|sed)[^\n]*{re.escape(target)}",
            runbook,
        ), target


def test_task9_later_root_wrappers_fail_closed_before_any_helper(
    tmp_path: Path,
) -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    def section(start: str, end: str | None) -> str:
        start_position = runbook.index(start)
        end_position = runbook.index(end, start_position) if end else len(runbook)
        return runbook[start_position:end_position]

    recovery_blocks = re.findall(
        r"(?ms)^```bash\n(.*?)^```$",
        section("## Conditional recovery only", "## Stable checkpoint resume"),
    )
    prune_blocks = re.findall(
        r"(?ms)^```bash\n(.*?)^```$",
        section("## Gate 3: approve remote pruning", "## Evidence and accepted limitation"),
    )
    rollback_blocks = re.findall(
        r"(?ms)^```bash\n(.*?)^```$",
        section("## Separately approved manual rollback", None),
    )
    wrappers = (*recovery_blocks, *prune_blocks, *rollback_blocks)

    assert len(recovery_blocks) == 1
    assert len(prune_blocks) == 2
    assert len(rollback_blocks) == 1

    operation_dir = tmp_path / "operation"
    source_dir = operation_dir / "source" / "deploy" / "linux"
    source_dir.mkdir(parents=True)
    source_ops = source_dir / "degen-prod-db-backup-ops.py"
    source_manifest = source_dir / "degen-prod-db-backup-assets.sha256"
    helper_log = tmp_path / "helper.log"
    helper = tmp_path / "helper"
    _write_executable(
        helper,
        f"#!/usr/bin/env bash\nprintf '%s\\n' reached >> {_posix_path(helper_log)!r}\nexit 0\n",
    )
    source_ops.write_bytes(helper.read_bytes())
    source_manifest.write_text(
        f"{'1' * 64}  deploy/linux/degen-prod-db-backup-ops.py\n",
        encoding="ascii",
        newline="\n",
    )
    values = {
        "OPERATION_DIR": _posix_path(operation_dir),
        "SOURCE_OPS": _posix_path(source_ops),
        "SOURCE_MANIFEST": _posix_path(source_manifest),
        "MANIFEST_SHA256": "0" * 64,
    }

    for index, wrapper in enumerate(wrappers, start=1):
        assert wrapper.startswith("#!/usr/bin/env bash\nset -euo pipefail\numask 077\n")
        script = tmp_path / f"fail-closed-wrapper-{index}.sh"
        script.write_text(
            wrapper.replace(
                "/usr/local/sbin/degen-prod-db-backup-ops",
                _posix_path(helper),
            ),
            encoding="utf-8",
            newline="\n",
        )
        assignments = [f"{key}={value}" for key, value in values.items()]
        if os.name == "nt":
            wsl = shutil.which("wsl.exe")
            if wsl is None:
                pytest.skip("WSL is unavailable")
            command = [wsl, "-u", "root", "-e", "env", *assignments, "bash", _posix_path(script)]
        else:
            if os.geteuid() != 0:
                pytest.skip("root is required for fail-closed wrapper tests")
            command = ["env", *assignments, BASH or "bash", str(script)]
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )

        assert result.returncode != 0
        assert not helper_log.exists(), result.stdout + result.stderr


def test_task9_runbook_bash_blocks_pass_syntax_check(tmp_path: Path) -> None:
    if BASH is None:
        pytest.skip("No usable POSIX Bash/WSL environment")
    runbook = RUNBOOK.read_text(encoding="utf-8")
    blocks = re.findall(r"(?ms)^```bash\n(.*?)^```$", runbook)

    assert len(blocks) >= 8
    for index, block in enumerate(blocks, start=1):
        script = tmp_path / f"runbook-block-{index}.sh"
        script.write_text(block, encoding="utf-8", newline="\n")
        result = subprocess.run(
            [BASH, "-n", _posix_path(script)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 0, f"bash block {index}: {result.stderr}"


def test_systemd_units_and_calendar_validate_when_systemd_analyze_is_available(tmp_path: Path) -> None:
    command = _systemd_analyze_prefix()
    if command is None:
        pytest.skip("systemd-analyze is unavailable")

    service_copy = tmp_path / SERVICE.name
    timer_copy = tmp_path / TIMER.name
    service_copy.write_text(
        SERVICE.read_text(encoding="utf-8").replace(
            "ExecStart=/usr/local/sbin/degen-prod-db-backup",
            "ExecStart=/bin/true",
        ),
        encoding="utf-8",
    )
    timer_copy.write_text(TIMER.read_text(encoding="utf-8"), encoding="utf-8")
    service_arg = _posix_path(service_copy) if os.name == "nt" else str(service_copy)
    timer_arg = _posix_path(timer_copy) if os.name == "nt" else str(timer_copy)
    verify = subprocess.run(
        [*command, "verify", service_arg, timer_arg],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert verify.returncode == 0, verify.stdout + verify.stderr

    expression = "*-*-* 03:15:00 America/Los_Angeles"
    calendar = subprocess.run(
        [*command, "calendar", "--iterations=2", expression],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert calendar.returncode == 0, calendar.stdout + calendar.stderr
    assert "America/Los_Angeles" in calendar.stdout


def test_script_passes_bash_syntax_check() -> None:
    if BASH is None:
        pytest.skip("No usable POSIX Bash/WSL environment")
    result = subprocess.run(
        [BASH, "-n", _posix_path(SCRIPT)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_success_verifies_before_pruning_and_preserves_non_candidates(harness: BackupHarness) -> None:
    old_names = _seed_old_pairs(harness.backup_dir)
    manual = harness.backup_dir / "manual-preserve.dump"
    manual.write_text("manual", encoding="utf-8")
    incomplete = harness.backup_dir / f"{PREFIX}20260625T230000Z.dump"
    incomplete.write_text("incomplete", encoding="utf-8")
    partial = harness.backup_dir / ".unrelated.partial"
    partial.write_text("partial", encoding="utf-8")
    protected_dir = harness.backup_dir / f"{PREFIX}20260620T230000Z.dump"
    protected_dir.mkdir()

    result = harness.run()

    assert result.returncode == 0, result.stdout + result.stderr
    current_dump, current_sidecar = _pair_names()
    retained = {
        path.name
        for path in harness.backup_dir.iterdir()
        if path.is_file() and re.fullmatch(rf"{re.escape(PREFIX)}\d{{8}}T\d{{6}}Z\.dump(?:\.sha256)?", path.name)
    }
    expected_retained = set(_pair_names("20260628T230000Z")) | {current_dump, current_sidecar}
    assert retained == expected_retained | {incomplete.name}
    assert all((harness.backup_dir / name).exists() is (name in expected_retained) for name in old_names)
    assert manual.exists()
    assert incomplete.exists()
    assert partial.exists()
    assert protected_dir.is_dir()

    dump_bytes = (harness.backup_dir / current_dump).read_bytes()
    expected_sidecar = f"{hashlib.sha256(dump_bytes).hexdigest()}  {current_dump}\n"
    assert (harness.backup_dir / current_sidecar).read_text(encoding="ascii") == expected_sidecar
    assert (harness.remote_dir / current_dump).read_bytes() == dump_bytes
    assert (harness.remote_dir / current_sidecar).read_text(encoding="ascii") == expected_sidecar
    assert not any(path.name.startswith(".degen-upload-") for path in harness.remote_dir.iterdir())

    trace = harness.trace_lines()
    prior_dump = _pair_names("20260628T230000Z")[0]
    first_local_delete = trace.index(f"rm:{_pair_names('20260626T230000Z')[0]}")
    assert trace.index("pg_restore") < trace.index("rclone:copy:dump")
    assert (
        trace.index("rclone:final-content")
        < trace.index(f"sha256sum-file:{prior_dump}")
        < trace.index(f"pg_restore-file:{prior_dump}")
        < first_local_delete
    )
    local_plans = [line.split(":", 3) for line in trace if line.startswith("planner:local:")]
    assert {parts[2] for parts in local_plans} == {"keep-names", "delete-names"}
    assert len({parts[3] for parts in local_plans}) == 1
    assert first_local_delete < trace.index("planner:remote")
    assert "Backup completed successfully" in result.stdout
    log_lines = harness.log_file.read_text(encoding="utf-8").splitlines()
    assert log_lines
    assert all(re.match(r"^\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\] ", line) for line in log_lines)


@pytest.mark.parametrize(
    "sidecar_case",
    ["wrong-digest", "wrong-basename", "missing-lf", "crlf", "extra-record", "uppercase", "control"],
)
def test_invalid_retained_sidecar_blocks_every_local_delete(
    harness: BackupHarness,
    sidecar_case: str,
) -> None:
    old_names = _seed_old_pairs(harness.backup_dir)
    retained_dump, retained_sidecar = _pair_names("20260628T230000Z")
    digest = hashlib.sha256((harness.backup_dir / retained_dump).read_bytes()).hexdigest()
    exact = f"{digest}  {retained_dump}\n".encode("ascii")
    malformed = {
        "wrong-digest": f"{'0' * 64}  {retained_dump}\n".encode("ascii"),
        "wrong-basename": f"{digest}  other.dump\n".encode("ascii"),
        "missing-lf": exact[:-1],
        "crlf": exact[:-1] + b"\r\n",
        "extra-record": exact + exact,
        "uppercase": exact.upper(),
        "control": exact[:-1] + b"\x00\n",
    }[sidecar_case]
    (harness.backup_dir / retained_sidecar).write_bytes(malformed)

    result = harness.run()

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() for name in old_names)
    assert _local_retention_delete_names(harness.trace_lines()) == []
    current_dump, current_sidecar = _pair_names()
    assert (harness.remote_dir / current_dump).exists()
    assert (harness.remote_dir / current_sidecar).exists()
    assert "Retained local backup validation failed" in result.stdout + result.stderr


@pytest.mark.parametrize("symlink_kind", ["dump", "sidecar"])
def test_retained_backup_symlink_blocks_every_local_delete(
    harness: BackupHarness,
    symlink_kind: str,
) -> None:
    old_names = _seed_old_pairs(harness.backup_dir)
    retained_dump, retained_sidecar = _pair_names("20260628T230000Z")
    attacked = harness.backup_dir / (retained_dump if symlink_kind == "dump" else retained_sidecar)
    external = harness.root / f"external-{symlink_kind}"
    external.write_bytes(attacked.read_bytes())
    attacked.unlink()
    _symlink(external, attacked)

    result = harness.run()

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() or _is_symlink(harness.backup_dir / name) for name in old_names)
    assert _local_retention_delete_names(harness.trace_lines()) == []
    assert "Unsafe local backup inventory entry" in result.stdout + result.stderr


@pytest.mark.parametrize("symlink_kind", ["dump", "sidecar"])
def test_post_inventory_retained_symlink_swap_reaches_fd_validator_and_blocks_delete(
    harness: BackupHarness,
    symlink_kind: str,
) -> None:
    old_names = _seed_old_pairs(harness.backup_dir)
    retained_dump, retained_sidecar = _pair_names("20260628T230000Z")
    attacked = harness.backup_dir / (retained_dump if symlink_kind == "dump" else retained_sidecar)
    external = harness.root / f"late-external-{symlink_kind}"
    external.write_bytes(attacked.read_bytes())
    _chmod(external, 0o600)

    result = harness.run(
        overrides={
            "FAKE_PLANNER_SYMLINK_PATH": _posix_path(attacked),
            "FAKE_PLANNER_SYMLINK_TARGET": _posix_path(external),
        }
    )

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() or _is_symlink(harness.backup_dir / name) for name in old_names)
    trace = harness.trace_lines()
    assert f"planner:local:symlink-race:{attacked.name}" in trace
    assert _local_retention_delete_names(trace) == []
    assert "Retained local backup validation failed" in result.stdout + result.stderr


@pytest.mark.parametrize("file_kind", ["dump", "sidecar"])
@pytest.mark.parametrize("metadata_attack", ["wrong-owner", "wrong-mode", "hardlink"])
def test_unsafe_retained_file_metadata_blocks_every_local_delete(
    harness: BackupHarness,
    file_kind: str,
    metadata_attack: str,
) -> None:
    old_names = _seed_old_pairs(harness.backup_dir)
    retained_dump, retained_sidecar = _pair_names("20260628T230000Z")
    attacked = harness.backup_dir / (retained_dump if file_kind == "dump" else retained_sidecar)
    if metadata_attack == "wrong-owner":
        wrong_uid = 0 if _effective_uid() != 0 else 1
        _chown(attacked, wrong_uid)
    elif metadata_attack == "wrong-mode":
        _chmod(attacked, 0o640)
    else:
        _hardlink(attacked, harness.root / f"retained-{file_kind}.hardlink")

    result = harness.run()

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() for name in old_names)
    assert _local_retention_delete_names(harness.trace_lines()) == []
    assert "Retained local backup validation failed" in result.stdout + result.stderr


def test_retained_pg_restore_failure_blocks_every_local_delete_after_remote_verification(
    harness: BackupHarness,
) -> None:
    old_names = _seed_old_pairs(harness.backup_dir)
    retained_dump = _pair_names("20260628T230000Z")[0]

    result = harness.run(overrides={"FAKE_PG_RESTORE_FAIL_BASENAME": retained_dump})

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() for name in old_names)
    trace = harness.trace_lines()
    assert _local_retention_delete_names(trace) == []
    assert trace.index("rclone:final-content") < trace.index(f"pg_restore-file:{retained_dump}")
    assert "Retained local backup validation failed" in result.stdout + result.stderr


def test_retained_pair_is_validated_even_when_delete_plan_is_empty(harness: BackupHarness) -> None:
    retained_dump, retained_sidecar = _seed_pair(harness.backup_dir, "20260628T230000Z")
    (harness.backup_dir / retained_sidecar).write_text(
        f"{'0' * 64}  {retained_dump}\n",
        encoding="ascii",
    )

    result = harness.run()

    assert result.returncode != 0
    assert (harness.backup_dir / retained_dump).exists()
    assert (harness.backup_dir / retained_sidecar).exists()
    assert _local_retention_delete_names(harness.trace_lines()) == []
    assert "Retained local backup validation failed" in result.stdout + result.stderr


def test_retained_sidecar_exact_size_is_gated_before_any_content_read() -> None:
    script = SCRIPT.read_text(encoding="utf-8")
    validator = script.split("validate_retained_local_pair() {", 1)[1].split(
        "run_local_retention() {",
        1,
    )[0]

    size_formula = 'expected_sidecar_size = 64 + 2 + len(os.fsencode(dump_name)) + 1'
    size_gate = 'if sidecar_metadata.st_size != expected_sidecar_size:'
    content_read = "sidecar = read_descriptor(sidecar_fd)"
    final_stability = 'require_stable(sidecar_fd, sidecar_metadata, sidecar_name)'
    restore_check = 'if restore.returncode != 0:'
    python_validator = validator.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
    ast.parse(python_validator, filename="<retained-local-validator>", feature_version=(3, 10))
    assert 'required_flags = ("O_NOFOLLOW", "O_DIRECTORY")' in python_validator
    assert 'getattr(os, "O_NOFOLLOW", 0)' not in python_validator
    assert 'getattr(os, "O_DIRECTORY", 0)' not in python_validator
    assert size_formula in validator
    assert validator.index(size_formula) < validator.index(size_gate) < validator.index(content_read)
    assert validator.rindex(final_stability) > validator.index(restore_check)


def test_every_kept_prior_pair_is_validated_before_any_delete(harness: BackupHarness) -> None:
    old_names = _seed_old_pairs(harness.backup_dir)
    retained_dump, retained_sidecar = _pair_names("20260627T230000Z")
    (harness.backup_dir / retained_sidecar).write_text(
        f"{'0' * 64}  {retained_dump}\n",
        encoding="ascii",
    )

    result = harness.run(overrides={"KEEP_LOCAL_COUNT": "3"})

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() for name in old_names)
    assert _local_retention_delete_names(harness.trace_lines()) == []
    assert "Retained local backup validation failed" in result.stdout + result.stderr


def test_first_ever_backup_succeeds_with_only_the_current_pair(harness: BackupHarness) -> None:
    result = harness.run()

    assert result.returncode == 0, result.stdout + result.stderr
    current_dump, current_sidecar = _pair_names()
    assert (harness.backup_dir / current_dump).is_file()
    assert (harness.backup_dir / current_sidecar).is_file()
    assert _local_retention_delete_names(harness.trace_lines()) == []


@pytest.mark.parametrize(
    "invalid_plan",
    ["current-missing", "future", "overlap", "duplicate", "incomplete"],
)
def test_unsafe_keep_and_delete_plans_are_rejected_before_local_delete(
    harness: BackupHarness,
    invalid_plan: str,
) -> None:
    prior = list(_seed_pair(harness.backup_dir, "20260628T230000Z"))
    future = list(_seed_pair(harness.backup_dir, "20260630T230000Z"))
    current = list(_pair_names())
    keep, delete = {
        "current-missing": (prior, []),
        "future": (current + future, []),
        "overlap": (current + prior, prior),
        "duplicate": (current + current, []),
        "incomplete": ([current[0]], []),
    }[invalid_plan]
    _write_format_planner(harness.planner, keep=keep, delete=delete)

    result = harness.run()

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() for name in prior + future + current)
    assert _local_retention_delete_names(harness.trace_lines()) == []
    assert "Unsafe retention" in result.stdout + result.stderr


@pytest.mark.parametrize(
    "planner_attack",
    [
        "oversized-line",
        "too-many-lines",
        "nul-normalization",
        "carriage-return",
        "empty-record",
        "missing-final-lf",
    ],
)
def test_planner_output_is_binary_safe_and_bounded_before_bash_capture(
    harness: BackupHarness,
    planner_attack: str,
) -> None:
    prior = list(_seed_pair(harness.backup_dir, "20260628T230000Z"))
    current_dump, current_sidecar = _pair_names()
    keep_expression, expected_error = {
        "oversized-line": ("b'A' * 1000000", "exceeded safe line length"),
        "too-many-lines": ("b'x\\n' * 1000", "exceeded safe record count"),
        "nul-normalization": (
            f"{current_dump.encode('ascii')!r} + b'\\x00\\n' + {current_sidecar.encode('ascii')!r} + b'\\n'",
            "contained a forbidden byte",
        ),
        "carriage-return": (
            f"{current_dump.encode('ascii')!r} + b'\\r\\n'",
            "contained a forbidden byte",
        ),
        "empty-record": ("b'\\n'", "contained an empty record"),
        "missing-final-lf": (
            f"{current_dump.encode('ascii')!r}",
            "was not LF-terminated",
        ),
    }[planner_attack]
    _write_raw_format_planner(
        harness.planner,
        keep_expression=keep_expression,
        delete_expression="b''",
    )

    result = harness.run()

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() for name in prior)
    current = list(_pair_names())
    assert all((harness.backup_dir / name).exists() for name in current)
    assert _local_retention_delete_names(harness.trace_lines()) == []
    assert expected_error in result.stdout + result.stderr


@pytest.mark.parametrize("mode", [None, "preflight", "remote-retention-dry-run"])
def test_every_rclone_mode_brackets_all_commands_with_safe_config_receipts(
    harness: BackupHarness,
    mode: str | None,
) -> None:
    original_bytes = harness.rclone_config.read_bytes()

    result = harness.run(mode, overrides={"FAKE_RCLONE_REFRESH_CONFIG": "1"})

    assert result.returncode == 0, result.stdout + result.stderr
    records = _receipt_records(harness)
    assert [record["phase"] for record in records] == ["before", "final"]
    required = {
        "phase",
        "status",
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
        "change",
    }
    assert all(required <= record.keys() for record in records)
    assert all(record["status"] == "ok" for record in records)
    assert records[0]["sha256"] == hashlib.sha256(original_bytes).hexdigest()
    assert records[1]["sha256"] == hashlib.sha256(harness.rclone_config.read_bytes()).hexdigest()
    assert records[0]["change"] == "baseline"
    assert records[1]["change"] == "possible-oauth-refresh"
    assert records[0]["inode"] != records[1]["inode"]
    assert records[0]["mode"] == records[1]["mode"] == "0600"
    assert records[0]["links"] == records[1]["links"] == "1"
    assert all(re.fullmatch(r"\d+", record["mtime_ns"]) for record in records)
    combined = result.stdout + result.stderr + harness.log_file.read_text(encoding="utf-8")
    assert "do-not-log-rclone-secret" not in combined
    assert "refreshed-rclone-secret" not in combined


def test_safe_atomic_config_replacement_is_recorded_even_when_hash_and_mtime_match(
    harness: BackupHarness,
) -> None:
    result = harness.run(
        "preflight",
        overrides={
            "FAKE_RCLONE_REFRESH_CONFIG": "1",
            "FAKE_RCLONE_REFRESH_PRESERVE_CONTENT": "1",
        },
    )

    assert result.returncode == 0, result.stdout + result.stderr
    before, final = _receipt_records(harness)
    assert before["sha256"] == final["sha256"]
    assert before["mtime_ns"] == final["mtime_ns"]
    assert before["inode"] != final["inode"]
    assert final["change"] == "possible-oauth-refresh"


def test_receipt_rejects_in_place_rewrite_that_restores_size_and_mtime(
    harness: BackupHarness,
) -> None:
    driver = harness.root / "exercise-receipt-race.py"
    _write_executable(
        driver,
        r'''#!/usr/bin/env python3
import os
from pathlib import Path
import sys


source = Path(sys.argv[1]).read_text(encoding="utf-8")
body = source.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
config = Path(sys.argv[2])
original_read = os.read
mutated = False


def racing_read(fd, size):
    global mutated
    chunk = original_read(fd, size)
    if chunk and not mutated:
        metadata = config.stat()
        with config.open("r+b", buffering=0) as handle:
            handle.seek(0)
            first = handle.read(1)
            handle.seek(0)
            handle.write(b"X" if first != b"X" else b"Y")
        os.utime(config, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        mutated = True
    return chunk


os.read = racing_read
sys.argv = ["receipt-race", str(config)]
status = 0
try:
    exec(compile(body, "embedded-rclone-receipt", "exec"), {})
except SystemExit as exc:
    status = int(exc.code or 0)
finally:
    print(f"receipt-race-mutated={int(mutated)}", file=sys.stderr)
raise SystemExit(status)
''',
    )
    runner = harness.root / "run-receipt-race.sh"
    _write_executable(
        runner,
        '''#!/usr/bin/env bash
set -eu
exec /usr/bin/python3 "$@"
''',
    )

    result = subprocess.run(
        [
            harness.bash,
            _posix_path(runner),
            _posix_path(driver),
            _posix_path(SCRIPT),
            _posix_path(harness.rclone_config),
        ],
        cwd=ROOT,
        env=harness.environment(),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert "receipt-race-mutated=1" in result.stderr
    assert result.returncode != 0


def test_final_config_receipt_follows_cleanup_rclone_calls_and_preserves_primary_error(
    harness: BackupHarness,
) -> None:
    result = harness.run(
        overrides={
            "FAKE_FAIL": "move_dump",
            "FAKE_DELETEFILE_MODE": "fail-before",
            "FAKE_RCLONE_REFRESH_CONFIG": "1",
        }
    )

    assert result.returncode == 1
    assert "ERROR: Remote dump publish move failed" in result.stdout
    assert len(_remote_delete_names(harness.trace_lines())) == 2
    records = _receipt_records(harness)
    assert [record["phase"] for record in records] == ["before", "final"]
    assert records[1]["status"] == "ok"
    assert records[1]["sha256"] == hashlib.sha256(harness.rclone_config.read_bytes()).hexdigest()
    assert records[1]["change"] == "possible-oauth-refresh"


def test_missing_mandatory_final_receipt_turns_an_otherwise_successful_mode_into_failure(
    harness: BackupHarness,
) -> None:
    result = harness.run(
        "preflight",
        overrides={"FAKE_RCLONE_REMOVE_CONFIG_AFTER": "lsf"},
    )

    assert result.returncode != 0
    records = _receipt_records(harness)
    assert records[0]["phase"] == "before"
    assert records[-1] == {
        "phase": "final",
        "status": "error",
        "reason": "metadata-unavailable",
    }


def test_final_receipt_failure_does_not_replace_an_existing_backup_failure(
    harness: BackupHarness,
) -> None:
    result = harness.run(
        overrides={
            "FAKE_FAIL": "copy_dump",
            "FAKE_RCLONE_REMOVE_CONFIG_AFTER": "lsf",
        }
    )

    assert result.returncode == 1
    assert "ERROR: Remote dump temporary upload failed" in result.stdout
    assert _receipt_records(harness)[-1] == {
        "phase": "final",
        "status": "error",
        "reason": "metadata-unavailable",
    }


@pytest.mark.parametrize("blocked_operation", ["pg_dump", "copy_dump"])
def test_process_group_term_cleans_only_current_run_temps(
    harness: BackupHarness,
    blocked_operation: str,
) -> None:
    old_local = _seed_old_pairs(harness.backup_dir)
    old_remote = _seed_old_pairs(harness.remote_dir)
    unrelated_partial = harness.backup_dir / ".manual-unrelated.partial"
    unrelated_partial.write_bytes(b"preserve-local-partial")
    unrelated_remote_temp = harness.remote_dir / ".degen-upload-unrelated"
    unrelated_remote_temp.write_bytes(b"preserve-remote-temp")

    result = harness.terminate_while(blocked_operation)

    assert result.returncode == 143, result.stdout + result.stderr
    assert all((harness.backup_dir / name).exists() for name in old_local)
    assert all((harness.remote_dir / name).exists() for name in old_remote)
    assert unrelated_partial.read_bytes() == b"preserve-local-partial"
    assert unrelated_remote_temp.read_bytes() == b"preserve-remote-temp"
    current_dump, current_sidecar = _pair_names()
    assert not list(harness.backup_dir.glob(f".{current_dump}.partial*"))
    assert not list(harness.backup_dir.glob(f".{current_sidecar}.partial*"))
    current_remote_temps = [
        path
        for path in harness.remote_dir.iterdir()
        if path.name.lower().startswith(".degen-upload-") and current_dump.lower() in path.name.lower()
    ]
    if blocked_operation == "pg_dump":
        assert current_remote_temps == []
    else:
        assert [path.name for path in current_remote_temps] == [f".degen-upload-TESTTOKEN-{current_dump}"]
        assert current_remote_temps[0].read_bytes() == b"DEGEN-CUSTOM-DUMP\x00verified-payload\n"
        assert f"rclone:delete:{current_remote_temps[0].name}" not in harness.trace_lines()
    assert not (harness.remote_dir / current_dump).exists()
    assert not (harness.remote_dir / current_sidecar).exists()
    if blocked_operation == "pg_dump":
        assert not (harness.backup_dir / current_dump).exists()
        assert not (harness.backup_dir / current_sidecar).exists()
    else:
        assert (harness.backup_dir / current_dump).exists()
        assert (harness.backup_dir / current_sidecar).exists()
    trace = harness.trace_lines()
    assert not any(line.startswith("planner:") for line in trace)
    assert not [name for name in _remote_delete_names(trace) if not name.startswith(".degen-upload-")]


def test_compound_local_cleanup_failures_warn_on_saved_stderr_and_preserve_primary_status(
    harness: BackupHarness,
) -> None:
    _write_executable(harness.fake_bin / "tee", FAKE_COMMAND)
    current_dump, current_sidecar = _pair_names()
    dump_partial = f".{current_dump}.partial.TESTTOKEN"
    sidecar_partial = f".{current_sidecar}.partial.TESTTOKEN"
    final_dump = harness.backup_dir / current_dump

    result = harness.run(
        overrides={
            "FAKE_CLEANUP_RM_FAILURE": "all",
            "FAKE_CREATE_LOCAL_FINAL": _posix_path(final_dump),
            "FAKE_TEE_EXIT_AFTER": "ERROR: Dump final path already exists",
        }
    )

    assert result.returncode == 1
    assert "ERROR: Dump final path already exists" in result.stdout
    assert "tee:exit-after-marker" in harness.trace_lines()
    assert _cleanup_warning("local dump partial", dump_partial) in result.stderr
    assert _cleanup_warning("local checksum partial", sidecar_partial) in result.stderr
    assert (harness.backup_dir / dump_partial).exists()
    assert (harness.backup_dir / sidecar_partial).exists()
    trace = harness.trace_lines()
    assert f"rm:{dump_partial}" in trace
    assert f"rm:{sidecar_partial}" in trace
    combined = result.stdout + result.stderr + harness.log_file.read_text(encoding="utf-8")
    assert SECRET not in combined
    assert "do-not-log-rclone-secret" not in combined


def test_compound_remote_cleanup_failures_warn_for_every_owned_temp_and_preserve_publication_error(
    harness: BackupHarness,
) -> None:
    current_dump, current_sidecar = _pair_names()
    dump_temp = f".degen-upload-TESTTOKEN-{current_dump}"
    sidecar_temp = f".degen-upload-TESTTOKEN-{current_sidecar}"

    result = harness.run(
        overrides={
            "FAKE_FAIL": "move_dump",
            "FAKE_DELETEFILE_MODE": "fail-before",
        }
    )

    assert result.returncode == 1
    assert "ERROR: Remote dump publish move failed" in result.stdout
    assert _remote_delete_names(harness.trace_lines()) == [dump_temp, sidecar_temp]
    assert _cleanup_warning("remote dump temp", dump_temp) in result.stderr
    assert _cleanup_warning("remote checksum temp", sidecar_temp) in result.stderr
    assert (harness.remote_dir / dump_temp).exists()
    assert (harness.remote_dir / sidecar_temp).exists()
    combined = result.stdout + result.stderr + harness.log_file.read_text(encoding="utf-8")
    assert SECRET not in combined
    assert "do-not-log-rclone-secret" not in combined


def test_exact_remote_final_collision_is_never_overwritten(harness: BackupHarness) -> None:
    current_dump, current_sidecar = _pair_names()
    dump_sentinel = b"existing-final-dump"
    sidecar_sentinel = b"existing-final-sidecar"
    (harness.remote_dir / current_dump).write_bytes(dump_sentinel)
    (harness.remote_dir / current_sidecar).write_bytes(sidecar_sentinel)

    result = harness.run()

    assert result.returncode != 0
    assert (harness.remote_dir / current_dump).read_bytes() == dump_sentinel
    assert (harness.remote_dir / current_sidecar).read_bytes() == sidecar_sentinel
    trace = harness.trace_lines()
    assert "rclone:copy:dump" not in trace
    assert not any(line.startswith("planner:") for line in trace)
    assert _remote_delete_names(trace) == []


def test_remote_final_collision_created_during_upload_is_not_overwritten(harness: BackupHarness) -> None:
    current_dump, current_sidecar = _pair_names()

    result = harness.run(
        overrides={
            "FAKE_CREATE_REMOTE_FINAL_ON": "sidecar",
            "FAKE_REMOTE_RACE_NAME": current_dump,
        }
    )

    assert result.returncode != 0
    assert (harness.remote_dir / current_dump).read_bytes() == b"remote-final-race-sentinel"
    assert not (harness.remote_dir / current_sidecar).exists()
    assert not any(path.name.lower().startswith(".degen-upload-") for path in harness.remote_dir.iterdir())
    assert not any(line.startswith("planner:") for line in harness.trace_lines())


@pytest.mark.parametrize("race_case", ["exact", "case-variant"])
def test_failed_dump_temp_copy_never_claims_or_deletes_raced_object(
    harness: BackupHarness,
    race_case: str,
) -> None:
    current_dump, _ = _pair_names()
    requested_temp = f".degen-upload-TESTTOKEN-{current_dump}"
    raced_name = requested_temp if race_case == "exact" else requested_temp.swapcase()

    result = harness.run(
        overrides={
            "FAKE_RACE_TEMP_ON": "dump",
            "FAKE_RACE_TEMP_CASE": "exact" if race_case == "exact" else "case-variant",
            "FAKE_DELETEFILE_MODE": "fail-before",
        }
    )

    assert result.returncode != 0
    assert (harness.remote_dir / raced_name).read_bytes() == b"foreign-raced-temp"
    trace = harness.trace_lines()
    assert f"rclone:delete:{raced_name}" not in trace
    assert "not cleanup-owned" in result.stdout
    assert "WARNING: backup cleanup failed" not in result.stderr
    assert not any(line.startswith("planner:") for line in trace)


def test_failed_sidecar_copy_warns_only_for_owned_dump_cleanup_and_preserves_unowned_race(
    harness: BackupHarness,
) -> None:
    current_dump, current_sidecar = _pair_names()
    dump_temp = f".degen-upload-TESTTOKEN-{current_dump}"
    sidecar_temp = f".degen-upload-TESTTOKEN-{current_sidecar}"

    result = harness.run(
        overrides={
            "FAKE_RACE_TEMP_ON": "sidecar",
            "FAKE_RACE_TEMP_CASE": "case-variant",
            "FAKE_DELETEFILE_MODE": "fail-before",
        }
    )

    raced_sidecar = sidecar_temp.swapcase()
    assert result.returncode == 1
    assert "ERROR: Remote checksum temporary upload failed" in result.stdout
    assert (harness.remote_dir / dump_temp).exists()
    assert (harness.remote_dir / raced_sidecar).read_bytes() == b"foreign-raced-temp"
    trace = harness.trace_lines()
    assert f"rclone:delete:{dump_temp}" in trace
    assert f"rclone:delete:{raced_sidecar}" not in trace
    assert "not cleanup-owned" in result.stdout
    assert _cleanup_warning("remote dump temp", dump_temp) in result.stderr
    assert _cleanup_warning("remote checksum temp", sidecar_temp) not in result.stderr
    assert raced_sidecar not in result.stderr
    assert not any(line.startswith("planner:") for line in trace)


@pytest.mark.parametrize("collision_kind", ["temp", "final"])
def test_case_variant_remote_collision_is_never_claimed_or_overwritten(
    harness: BackupHarness,
    collision_kind: str,
) -> None:
    current_dump, _ = _pair_names()
    if collision_kind == "temp":
        requested = f".degen-upload-TESTTOKEN-{current_dump}"
    else:
        requested = current_dump
    collision = requested.swapcase()
    sentinel = harness.remote_dir / collision
    sentinel.write_bytes(b"case-variant-sentinel")

    result = harness.run()

    assert result.returncode != 0
    assert sentinel.read_bytes() == b"case-variant-sentinel"
    trace = harness.trace_lines()
    assert "rclone:copy:dump" not in trace
    assert f"rclone:delete:{collision}" not in trace
    assert not any(line.startswith("planner:") for line in trace)


def test_fake_rclone_immutable_overwrites_an_existing_different_destination(
    harness: BackupHarness,
) -> None:
    source = harness.root / "immutable-source.dump"
    target = harness.remote_dir / "immutable-target.dump"
    source.write_bytes(b"replacement")
    target.write_bytes(b"existing-different")

    result = _run_fake_rclone(
        harness,
        "--config",
        _posix_path(harness.rclone_config),
        "--immutable",
        "copyto",
        _posix_path(source),
        "test:backups/degen-db/immutable-target.dump",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert target.read_bytes() == b"replacement"


@pytest.mark.parametrize("existing", [b"source-bytes", b"existing-different"])
def test_fake_rclone_strict_copy_skips_existing_destination_with_exit_9(
    harness: BackupHarness,
    existing: bytes,
) -> None:
    source = harness.root / "strict-source.dump"
    target = harness.remote_dir / "strict-target.dump"
    source.write_bytes(b"source-bytes")
    target.write_bytes(existing)

    result = _run_fake_rclone(
        harness,
        "--config",
        _posix_path(harness.rclone_config),
        "--ignore-existing",
        "--error-on-no-transfer",
        "copyto",
        _posix_path(source),
        "test:backups/degen-db/strict-target.dump",
    )

    assert result.returncode == 9, result.stdout + result.stderr
    assert target.read_bytes() == existing


def test_fake_rclone_strict_move_skip_leaves_source_and_destination_unchanged(
    harness: BackupHarness,
) -> None:
    source = harness.remote_dir / "strict-move-source.dump"
    target = harness.remote_dir / "strict-move-target.dump"
    source.write_bytes(b"source-bytes")
    target.write_bytes(b"foreign-destination")

    result = _run_fake_rclone(
        harness,
        "--config",
        _posix_path(harness.rclone_config),
        "--ignore-existing",
        "--error-on-no-transfer",
        "moveto",
        "test:backups/degen-db/strict-move-source.dump",
        "test:backups/degen-db/strict-move-target.dump",
    )

    assert result.returncode == 9, result.stdout + result.stderr
    assert source.read_bytes() == b"source-bytes"
    assert target.read_bytes() == b"foreign-destination"


def test_all_remote_publication_operations_use_strict_no_existing_flags(
    harness: BackupHarness,
) -> None:
    result = harness.run(overrides={"FAKE_REQUIRE_STRICT_PUBLICATION_FLAGS": "1"})

    assert result.returncode == 0, result.stdout + result.stderr
    current_dump, current_sidecar = _pair_names()
    assert (harness.remote_dir / current_dump).exists()
    assert (harness.remote_dir / current_sidecar).exists()
    assert [
        line
        for line in harness.trace_lines()
        if line.startswith("rclone:publication-flags:")
    ] == [
        "rclone:publication-flags:copy:dump:ignore-existing=1:error-on-no-transfer=1",
        "rclone:publication-flags:copy:sidecar:ignore-existing=1:error-on-no-transfer=1",
        "rclone:publication-flags:move:dump:ignore-existing=1:error-on-no-transfer=1",
        "rclone:publication-flags:move:sidecar:ignore-existing=1:error-on-no-transfer=1",
    ]


@pytest.mark.parametrize("kind", ["dump", "sidecar"])
def test_command_time_final_race_preserves_foreign_destination_bytes(
    harness: BackupHarness,
    kind: str,
) -> None:
    result = harness.run(overrides={"FAKE_RACE_FINAL_ON": kind})

    assert result.returncode != 0
    current_dump, current_sidecar = _pair_names()
    raced_name = current_dump if kind == "dump" else current_sidecar
    assert (harness.remote_dir / raced_name).read_bytes() == b"foreign-raced-final"
    assert f"rclone:race-final:{kind}:{raced_name}" in harness.trace_lines()
    assert not any(line.startswith("planner:") for line in harness.trace_lines())


@pytest.mark.parametrize("kind", ["dump", "sidecar"])
def test_remote_move_must_remove_each_temp_source(harness: BackupHarness, kind: str) -> None:
    result = harness.run(overrides={"FAKE_MOVE_LEAVES_SOURCE": kind})

    assert result.returncode != 0
    assert not any(path.name.lower().startswith(".degen-upload-") for path in harness.remote_dir.iterdir())
    assert not any(line.startswith("planner:") for line in harness.trace_lines())


def test_remote_prune_is_opt_in_by_default(harness: BackupHarness) -> None:
    old_remote = _seed_old_pairs(harness.remote_dir)

    result = harness.run(
        overrides={
            "KEEP_REMOTE_DAILY": "0",
            "KEEP_REMOTE_WEEKLY": "0",
            "KEEP_REMOTE_MONTHLY": "0",
        }
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert all((harness.remote_dir / name).exists() for name in old_remote)
    assert not [name for name in _remote_delete_names(harness.trace_lines()) if not name.startswith(".degen-upload-")]
    assert "Remote retention dry run" in result.stdout


def test_remote_retention_dry_run_never_dumps_or_deletes_even_when_enabled(harness: BackupHarness) -> None:
    old_remote = _seed_old_pairs(harness.remote_dir)

    result = harness.run(
        "remote-retention-dry-run",
        overrides={
            "REMOTE_PRUNE_ENABLED": "1",
            "KEEP_REMOTE_DAILY": "0",
            "KEEP_REMOTE_WEEKLY": "0",
            "KEEP_REMOTE_MONTHLY": "0",
            "FAKE_DB_SIZE": "999999999999",
            "FAKE_DF_AVAILABLE": "0",
        },
    )

    assert result.returncode == 0, result.stdout + result.stderr
    trace = harness.trace_lines()
    assert "pg_dump" not in trace
    assert "psql:size" not in trace
    assert "df" not in trace
    assert "planner:remote" in trace
    assert _remote_delete_names(trace) == []
    assert all((harness.remote_dir / name).exists() for name in old_remote)
    for candidate in old_remote[:4]:
        assert f"Remote retention candidate: {candidate}" in result.stdout
        assert f"Remote retention dry run: would delete {candidate}" in result.stdout
    for protected in old_remote[4:]:
        assert f"Remote retention candidate: {protected}" not in result.stdout


def test_enabled_remote_prune_deletes_only_planner_candidates(harness: BackupHarness) -> None:
    old_remote = _seed_old_pairs(harness.remote_dir)
    manual = harness.remote_dir / "manual-preserve.dump"
    manual.write_text("manual", encoding="utf-8")
    incomplete = harness.remote_dir / f"{PREFIX}20260625T230000Z.dump"
    incomplete.write_text("incomplete", encoding="utf-8")
    unrelated_temp = harness.remote_dir / ".degen-upload-manual-preserve"
    unrelated_temp.write_text("temporary", encoding="utf-8")

    result = harness.run(
        overrides={
            "REMOTE_PRUNE_ENABLED": "1",
            "KEEP_REMOTE_DAILY": "0",
            "KEEP_REMOTE_WEEKLY": "0",
            "KEEP_REMOTE_MONTHLY": "0",
        }
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert all(not (harness.remote_dir / name).exists() for name in old_remote)
    current = _pair_names()
    assert all((harness.remote_dir / name).exists() for name in current)
    assert manual.exists()
    assert incomplete.exists()
    assert unrelated_temp.exists()
    assert set(_remote_delete_names(harness.trace_lines())) == set(old_remote)


def test_preexisting_remote_temp_name_is_not_overwritten_or_claimed(harness: BackupHarness) -> None:
    dump_name, _ = _pair_names()
    stale_temp = harness.remote_dir / f".degen-upload-TESTTOKEN-{dump_name}"
    stale_temp.write_bytes(b"stale-remote-temp")

    result = harness.run()

    assert result.returncode != 0
    assert stale_temp.read_bytes() == b"stale-remote-temp"
    trace = harness.trace_lines()
    assert "mktemp" in trace
    assert "rclone:copy:dump" not in trace
    assert f"rclone:delete:{stale_temp.name}" not in trace


@pytest.mark.parametrize(
    "failure",
    [
        "pg_dump",
        "pg_restore",
        "sha256sum",
        "copy_dump",
        "copy_sidecar",
        "temp_size",
        "temp_sidecar",
        "move_dump",
        "move_sidecar",
        "final_size",
        "final_content",
        "planner",
    ],
)
def test_preverification_and_planner_failures_never_prune_old_pairs(
    harness: BackupHarness,
    failure: str,
) -> None:
    old_local = _seed_old_pairs(harness.backup_dir)
    old_remote = _seed_old_pairs(harness.remote_dir)
    unrelated_partial = harness.backup_dir / ".preexisting-unrelated.partial"
    unrelated_partial.write_bytes(b"preserve-unrelated-partial")

    result = harness.run(
        overrides={
            "FAKE_FAIL": failure,
            "REMOTE_PRUNE_ENABLED": "1",
            "KEEP_REMOTE_DAILY": "0",
            "KEEP_REMOTE_WEEKLY": "0",
            "KEEP_REMOTE_MONTHLY": "0",
        }
    )

    assert result.returncode != 0, failure
    assert all((harness.backup_dir / name).exists() for name in old_local)
    assert all((harness.remote_dir / name).exists() for name in old_remote)
    assert unrelated_partial.read_bytes() == b"preserve-unrelated-partial"
    current_dump, current_sidecar = _pair_names()
    assert not list(harness.backup_dir.glob(f".{current_dump}.partial*"))
    assert not list(harness.backup_dir.glob(f".{current_sidecar}.partial*"))
    dump_temp = f".degen-upload-TESTTOKEN-{current_dump}"
    sidecar_temp = f".degen-upload-TESTTOKEN-{current_sidecar}"
    remaining_temps = {
        path.name for path in harness.remote_dir.iterdir() if path.name.startswith(".degen-upload-")
    }
    expected_remaining_temps = {
        "copy_dump": {dump_temp},
        "copy_sidecar": {sidecar_temp},
    }.get(failure, set())
    assert remaining_temps == expected_remaining_temps
    if failure in {"copy_dump", "copy_sidecar"}:
        assert "not cleanup-owned" in result.stdout
    trace = harness.trace_lines()
    expected_operation = {
        "pg_dump": "pg_dump",
        "pg_restore": "pg_restore",
        "sha256sum": "sha256sum",
        "copy_dump": "rclone:copy:dump",
        "copy_sidecar": "rclone:copy:sidecar",
        "temp_size": "rclone:temp-size",
        "temp_sidecar": "rclone:temp-content",
        "move_dump": "rclone:move:dump",
        "move_sidecar": "rclone:move:sidecar",
        "final_size": "rclone:final-size",
        "final_content": "rclone:final-content",
        "planner": "planner:local",
    }[failure]
    assert expected_operation in trace
    remote_deletes = _remote_delete_names(trace)
    assert all(name.startswith(".degen-upload-") for name in remote_deletes)
    if failure == "copy_dump":
        assert dump_temp not in remote_deletes
    elif failure == "copy_sidecar":
        assert dump_temp in remote_deletes
        assert sidecar_temp not in remote_deletes
    current_remote_names = {
        name
        for name in (current_dump, current_sidecar)
        if (harness.remote_dir / name).exists()
    }
    expected_current_remote = {
        "move_sidecar": {current_dump},
        "final_size": {current_dump, current_sidecar},
        "final_content": {current_dump, current_sidecar},
        "planner": {current_dump, current_sidecar},
    }.get(failure, set())
    assert current_remote_names == expected_current_remote
    assert "WARNING: backup cleanup failed" not in result.stderr


def test_remote_planner_failure_preserves_all_remote_final_pairs(harness: BackupHarness) -> None:
    old_remote = _seed_old_pairs(harness.remote_dir)

    result = harness.run(
        overrides={
            "FAKE_FAIL": "planner_remote",
            "REMOTE_PRUNE_ENABLED": "1",
            "KEEP_REMOTE_DAILY": "0",
            "KEEP_REMOTE_WEEKLY": "0",
            "KEEP_REMOTE_MONTHLY": "0",
        }
    )

    assert result.returncode != 0
    current_dump, current_sidecar = _pair_names()
    assert all((harness.remote_dir / name).exists() for name in old_remote)
    assert (harness.remote_dir / current_dump).exists()
    assert (harness.remote_dir / current_sidecar).exists()
    trace = harness.trace_lines()
    assert "planner:remote" in trace
    assert _remote_delete_names(trace) == []


@pytest.mark.parametrize(
    ("delete_mode", "attempted_candidate_remains"),
    [("fail-before", True), ("delete-then-fail", False)],
)
def test_remote_delete_failure_identifies_candidate_and_stops_before_later_candidates(
    harness: BackupHarness,
    delete_mode: str,
    attempted_candidate_remains: bool,
) -> None:
    old_remote = _seed_old_pairs(harness.remote_dir)

    result = harness.run(
        overrides={
            "FAKE_DELETEFILE_MODE": delete_mode,
            "REMOTE_PRUNE_ENABLED": "1",
            "KEEP_REMOTE_DAILY": "0",
            "KEEP_REMOTE_WEEKLY": "0",
            "KEEP_REMOTE_MONTHLY": "0",
        }
    )

    assert result.returncode == 1
    current_dump, current_sidecar = _pair_names()
    assert (harness.remote_dir / old_remote[0]).exists() is attempted_candidate_remains
    assert all((harness.remote_dir / name).exists() for name in old_remote[1:])
    assert (harness.remote_dir / current_dump).exists()
    assert (harness.remote_dir / current_sidecar).exists()
    deleted_attempts = _remote_delete_names(harness.trace_lines())
    assert deleted_attempts == [old_remote[0]]
    assert f"ERROR: Remote retention delete failed: {old_remote[0]}" in result.stdout


def test_insufficient_capacity_fails_before_dump_or_publish(harness: BackupHarness) -> None:
    result = harness.run(overrides={"FAKE_DB_SIZE": "900", "FAKE_DF_AVAILABLE": "999", "MIN_FREE_AFTER_BYTES": "100"})

    assert result.returncode != 0
    trace = harness.trace_lines()
    assert "psql:size" in trace
    assert "pg_dump" not in trace
    assert "rclone:copy:dump" not in trace
    assert not any(line.startswith("planner:") for line in trace)
    assert "Insufficient backup capacity" in result.stdout


def test_preexisting_legacy_partials_are_unrelated_and_preserved(harness: BackupHarness) -> None:
    dump_name, sidecar_name = _pair_names()
    dump_partial = harness.backup_dir / f".{dump_name}.partial"
    sidecar_partial = harness.backup_dir / f".{sidecar_name}.partial"
    dump_partial.write_bytes(b"stale-dump-partial")
    sidecar_partial.write_bytes(b"stale-sidecar-partial")

    result = harness.run()

    assert result.returncode == 0, result.stdout + result.stderr
    assert dump_partial.read_bytes() == b"stale-dump-partial"
    assert sidecar_partial.read_bytes() == b"stale-sidecar-partial"
    assert "pg_dump" in harness.trace_lines()


def test_backup_directory_must_not_be_a_symlink(harness: BackupHarness) -> None:
    target = harness.root / "real-backup-target"
    target.mkdir(mode=0o700)
    _chmod(target, 0o700)
    harness.backup_dir.rmdir()
    _symlink(target, harness.backup_dir)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert _is_symlink(harness.backup_dir)
    assert list(target.iterdir()) == []
    assert "pg_dump" not in harness.trace_lines()


def test_backup_directory_must_not_be_group_or_world_writable(harness: BackupHarness) -> None:
    _chmod(harness.backup_dir, 0o770)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert "pg_dump" not in harness.trace_lines()


def test_backup_directory_must_be_owned_by_effective_uid(harness: BackupHarness) -> None:
    effective_uid = _effective_uid()
    other_uid = 0 if effective_uid != 0 else 1
    _chown(harness.backup_dir, other_uid)
    try:
        result = harness.run("preflight")
    finally:
        _chown(harness.backup_dir, effective_uid)

    assert result.returncode != 0
    assert "pg_dump" not in harness.trace_lines()


def test_broken_partial_symlink_is_untouched_and_never_followed(harness: BackupHarness) -> None:
    current_dump, current_sidecar = _pair_names()
    outside = harness.root / "outside-partial-target"
    attacker_partial = harness.backup_dir / f".{current_dump}.partial"
    _symlink(outside, attacker_partial)

    result = harness.run()

    assert result.returncode == 0, result.stdout + result.stderr
    assert _is_symlink(attacker_partial)
    assert not outside.exists()
    assert (harness.backup_dir / current_dump).is_file()
    assert (harness.backup_dir / current_sidecar).is_file()


def test_broken_final_symlink_collision_is_rejected_without_following(harness: BackupHarness) -> None:
    current_dump, _ = _pair_names()
    outside = harness.root / "outside-final-target"
    final_dump = harness.backup_dir / current_dump
    _symlink(outside, final_dump)

    result = harness.run()

    assert result.returncode != 0
    assert _is_symlink(final_dump)
    assert not outside.exists()
    assert "pg_dump" not in harness.trace_lines()
    assert not any(line.startswith("planner:") for line in harness.trace_lines())


@pytest.mark.parametrize("final_kind", ["dump", "sidecar"])
def test_preexisting_regular_local_final_is_untouched(
    harness: BackupHarness,
    final_kind: str,
) -> None:
    current_dump, current_sidecar = _pair_names()
    final_name = current_dump if final_kind == "dump" else current_sidecar
    final_path = harness.backup_dir / final_name
    final_path.write_bytes(b"preexisting-local-final")

    result = harness.run()

    assert result.returncode != 0
    assert final_path.read_bytes() == b"preexisting-local-final"
    assert "pg_dump" not in harness.trace_lines()
    assert not any(line.startswith("planner:") for line in harness.trace_lines())


@pytest.mark.parametrize("final_kind", ["dump", "sidecar"])
def test_local_publish_race_never_overwrites_existing_final(
    harness: BackupHarness,
    final_kind: str,
) -> None:
    current_dump, current_sidecar = _pair_names()
    final_name = current_dump if final_kind == "dump" else current_sidecar
    final_path = harness.backup_dir / final_name

    result = harness.run(overrides={"FAKE_CREATE_LOCAL_FINAL": _posix_path(final_path)})

    assert result.returncode != 0
    assert final_path.read_bytes() == b"local-final-race-sentinel"
    assert not list(harness.backup_dir.glob(f".{current_dump}.partial*"))
    assert not list(harness.backup_dir.glob(f".{current_sidecar}.partial*"))
    assert not any(line.startswith("planner:") for line in harness.trace_lines())


def test_rclone_config_symlink_is_rejected_without_reading_target(
    harness: BackupHarness,
) -> None:
    sentinel = harness.root / "rclone-config-sentinel"
    sentinel.write_bytes(b"token = preserve-target-secret\n")
    _chmod(sentinel, 0o600)
    harness.rclone_config.unlink()
    _symlink(sentinel, harness.rclone_config)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert _is_symlink(harness.rclone_config)
    assert sentinel.read_bytes() == b"token = preserve-target-secret\n"
    assert "preserve-target-secret" not in result.stdout + result.stderr
    _assert_rclone_config_rejected_before_rclone(harness, result)


@pytest.mark.parametrize("mode", [0o640, 0o400, 0o660])
def test_rclone_config_requires_exact_mode_0600(
    harness: BackupHarness, mode: int
) -> None:
    _chmod(harness.rclone_config, mode)

    result = harness.run("preflight")

    assert result.returncode != 0
    _assert_rclone_config_rejected_before_rclone(harness, result)


def test_rclone_config_requires_single_link(
    harness: BackupHarness,
) -> None:
    alias = harness.root / "rclone-config-alias"
    _hardlink(harness.rclone_config, alias)

    result = harness.run("preflight")

    assert result.returncode != 0
    _assert_rclone_config_rejected_before_rclone(harness, result)


def test_rclone_config_requires_effective_uid_ownership(
    harness: BackupHarness,
) -> None:
    effective_uid = _effective_uid()
    other_uid = 0 if effective_uid != 0 else 1
    _chown(harness.rclone_config, other_uid)
    try:
        result = harness.run("preflight")
    finally:
        _chown(harness.rclone_config, effective_uid)

    assert result.returncode != 0
    _assert_rclone_config_rejected_before_rclone(harness, result)


def test_rclone_config_parent_must_not_be_group_or_world_writable(
    harness: BackupHarness,
) -> None:
    _chmod(harness.rclone_config.parent, 0o770)

    result = harness.run("preflight")

    assert result.returncode != 0
    _assert_rclone_config_rejected_before_rclone(harness, result)


def test_rclone_config_parent_must_be_real_non_symlink_directory(
    harness: BackupHarness,
) -> None:
    parent = harness.rclone_config.parent
    target = harness.root / "rclone-parent-target"
    target.mkdir(mode=0o700)
    _chmod(target, 0o700)
    target_config = target / harness.rclone_config.name
    target_config.write_bytes(harness.rclone_config.read_bytes())
    _chmod(target_config, 0o600)
    harness.rclone_config.unlink()
    parent.rmdir()
    _symlink(target, parent)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert _is_symlink(parent)
    _assert_rclone_config_rejected_before_rclone(harness, result)


def test_rclone_config_parent_requires_effective_uid_ownership(
    harness: BackupHarness,
) -> None:
    effective_uid = _effective_uid()
    other_uid = 0 if effective_uid != 0 else 1
    _chown(harness.rclone_config.parent, other_uid)
    try:
        result = harness.run("preflight")
    finally:
        _chown(harness.rclone_config.parent, effective_uid)

    assert result.returncode != 0
    _assert_rclone_config_rejected_before_rclone(harness, result)


@pytest.mark.parametrize("mode", [0o750, 0o711, 0o770])
def test_lock_parent_requires_exact_mode_0700(
    harness: BackupHarness, mode: int
) -> None:
    _chmod(harness.lock_file.parent, mode)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert not harness.lock_file.exists()
    _assert_lock_rejected_before_external_work(harness, result)


def test_lock_parent_must_be_real_non_symlink_directory(
    harness: BackupHarness,
) -> None:
    lock_parent = harness.lock_file.parent
    target = harness.root / "lock-parent-target"
    target.mkdir(mode=0o700)
    _chmod(target, 0o700)
    lock_parent.rmdir()
    _symlink(target, lock_parent)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert _is_symlink(lock_parent)
    assert list(target.iterdir()) == []
    _assert_lock_rejected_before_external_work(harness, result)


def test_lock_parent_must_be_owned_by_effective_uid(
    harness: BackupHarness,
) -> None:
    effective_uid = _effective_uid()
    other_uid = 0 if effective_uid != 0 else 1
    _chown(harness.lock_file.parent, other_uid)
    try:
        result = harness.run("preflight")
    finally:
        _chown(harness.lock_file.parent, effective_uid)

    assert result.returncode != 0
    assert not harness.lock_file.exists()
    _assert_lock_rejected_before_external_work(harness, result)


def test_lock_symlink_is_rejected_without_changing_target_sentinel(
    harness: BackupHarness,
) -> None:
    sentinel = harness.root / "lock-target-sentinel"
    sentinel.write_bytes(b"preserve-lock-target-bytes")
    _chmod(sentinel, 0o600)
    _symlink(sentinel, harness.lock_file)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert _is_symlink(harness.lock_file)
    assert sentinel.read_bytes() == b"preserve-lock-target-bytes"
    _assert_lock_rejected_before_external_work(harness, result)


@pytest.mark.parametrize("mode", [0o640, 0o606])
def test_existing_lock_rejects_group_or_world_permission_bits(
    harness: BackupHarness, mode: int
) -> None:
    harness.lock_file.write_bytes(b"preserve-private-lock")
    _chmod(harness.lock_file, mode)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert harness.lock_file.read_bytes() == b"preserve-private-lock"
    _assert_lock_rejected_before_external_work(harness, result)


def test_existing_lock_requires_single_link_and_preserves_bytes(
    harness: BackupHarness,
) -> None:
    harness.lock_file.write_bytes(b"preserve-linked-lock")
    _chmod(harness.lock_file, 0o600)
    alias = harness.root / "backup-lock-alias"
    _hardlink(harness.lock_file, alias)

    result = harness.run("preflight")

    assert result.returncode != 0
    assert harness.lock_file.read_bytes() == b"preserve-linked-lock"
    assert alias.read_bytes() == b"preserve-linked-lock"
    _assert_lock_rejected_before_external_work(harness, result)


def test_existing_lock_requires_effective_uid_ownership(
    harness: BackupHarness,
) -> None:
    harness.lock_file.write_bytes(b"preserve-owned-lock")
    _chmod(harness.lock_file, 0o600)
    effective_uid = _effective_uid()
    other_uid = 0 if effective_uid != 0 else 1
    _chown(harness.lock_file, other_uid)
    try:
        result = harness.run("preflight")
    finally:
        _chown(harness.lock_file, effective_uid)

    assert result.returncode != 0
    assert harness.lock_file.read_bytes() == b"preserve-owned-lock"
    _assert_lock_rejected_before_external_work(harness, result)


def test_lock_path_replacement_during_flock_fails_identity_revalidation(
    harness: BackupHarness,
) -> None:
    harness.lock_file.write_bytes(b"original-lock")
    _chmod(harness.lock_file, 0o600)
    renamed = harness.root / "raced-original-lock"

    result = harness.run(
        "preflight",
        overrides={
            "FAKE_FLOCK_REPLACE_LOCK": _posix_path(harness.lock_file),
            "FAKE_FLOCK_RENAMED_LOCK": _posix_path(renamed),
        },
    )

    assert result.returncode != 0
    assert renamed.read_bytes() == b"original-lock"
    _assert_lock_rejected_before_external_work(harness, result)


def test_private_existing_lock_preserves_bytes_and_nonblocking_overlap(
    harness: BackupHarness,
) -> None:
    harness.lock_file.write_bytes(b"private-lock-sentinel")
    _chmod(harness.lock_file, 0o600)
    lock_path = _posix_path(harness.lock_file)
    holder = subprocess.Popen(
        [harness.bash, _posix_path(harness.lock_holder), lock_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "READY"
        result = harness.run("preflight")
    finally:
        if holder.stdin is not None:
            holder.stdin.write("\n")
            holder.stdin.flush()
        holder.wait(timeout=10)

    assert result.returncode != 0
    assert harness.lock_file.read_bytes() == b"private-lock-sentinel"
    assert "already running" in (result.stdout + result.stderr).lower()
    assert "psql:database" not in harness.trace_lines()


@pytest.mark.parametrize("mode", ["preflight", "remote-retention-dry-run"])
def test_inherited_lock_fd_is_validated_flocked_and_kept_for_allowed_modes(
    harness: BackupHarness, mode: str
) -> None:
    result = harness.run_with_lock_fd(mode)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "flock" in harness.trace_lines()


def test_remote_dry_run_uses_operator_frozen_retention_timestamp(
    harness: BackupHarness,
) -> None:
    frozen_now = "20260701T123456Z"

    result = harness.run_with_lock_fd(
        "remote-retention-dry-run",
        frozen_now=frozen_now,
        overrides={"FAKE_TRACE_PLANNER_NOW": "1"},
    )

    assert result.returncode == 0, result.stdout + result.stderr
    planner_times = [
        line.removeprefix("planner-now:")
        for line in harness.trace_lines()
        if line.startswith("planner-now:")
    ]
    assert planner_times
    assert set(planner_times) == {frozen_now}


@pytest.mark.parametrize(
    ("mode", "frozen_now"),
    [
        ("preflight", "20260701T123456Z"),
        ("remote-retention-dry-run", "20260701T12345Z"),
        ("remote-retention-dry-run", "20260230T123456Z"),
    ],
)
def test_frozen_retention_timestamp_rejects_wrong_mode_or_invalid_value(
    harness: BackupHarness,
    mode: str,
    frozen_now: str,
) -> None:
    result = harness.run_with_lock_fd(mode, frozen_now=frozen_now)

    assert result.returncode != 0
    assert "psql:database" not in harness.trace_lines()
    assert not any(line.startswith("rclone:") for line in harness.trace_lines())


@pytest.mark.parametrize("fd_case", ["closed", "wrong-file", "replaced-path"])
def test_inherited_lock_fd_rejects_closed_wrong_or_path_mismatched_descriptors(
    harness: BackupHarness, fd_case: str
) -> None:
    result = harness.run_with_lock_fd("preflight", fd_case=fd_case)

    assert result.returncode != 0
    assert "psql:database" not in harness.trace_lines()
    assert not any(line.startswith("rclone:") for line in harness.trace_lines())


def test_inherited_lock_fd_rejects_mismatched_metadata(
    harness: BackupHarness,
) -> None:
    harness.lock_file.write_bytes(b"metadata-lock")
    _chmod(harness.lock_file, 0o640)

    result = harness.run_with_lock_fd("preflight")

    assert result.returncode != 0
    assert harness.lock_file.read_bytes() == b"metadata-lock"
    _assert_lock_rejected_before_external_work(harness, result)


def test_inherited_lock_fd_is_rejected_for_run_mode(
    harness: BackupHarness,
) -> None:
    result = harness.run_with_lock_fd("run")

    assert result.returncode != 0
    assert "psql:database" not in harness.trace_lines()
    assert not any(line.startswith("rclone:") for line in harness.trace_lines())


@pytest.mark.parametrize(
    "extra_args",
    [
        ("--lock-fd",),
        ("--lock-fd", "not-a-number"),
        ("--lock-fd", "-1"),
        ("--lock-fd", "8", "extra"),
    ],
)
def test_lock_fd_option_rejects_missing_invalid_or_extra_arguments(
    harness: BackupHarness, extra_args: tuple[str, ...]
) -> None:
    result = harness.run("preflight", extra_args=extra_args)

    assert result.returncode != 0
    assert "psql:database" not in harness.trace_lines()
    assert not any(line.startswith("rclone:") for line in harness.trace_lines())


def test_lock_overlap_fails_before_preflight_or_dump(harness: BackupHarness) -> None:
    lock_path = _posix_path(harness.lock_file)
    holder = subprocess.Popen(
        [
            harness.bash,
            _posix_path(harness.lock_holder),
            lock_path,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "READY"
        result = harness.run()
    finally:
        if holder.stdin is not None:
            holder.stdin.write("\n")
            holder.stdin.flush()
        holder.wait(timeout=10)

    assert result.returncode != 0
    trace = harness.trace_lines()
    assert "psql:database" not in trace
    assert "pg_dump" not in trace
    assert not any(line.startswith("rclone:") for line in trace)
    assert not any(line.startswith("planner:") for line in trace)
    assert "already running" in result.stdout.lower()


def test_preflight_checks_capacity_and_access_without_dump_or_retention(harness: BackupHarness) -> None:
    old_local = _seed_old_pairs(harness.backup_dir)
    old_remote = _seed_old_pairs(harness.remote_dir)

    result = harness.run("preflight")

    assert result.returncode == 0, result.stdout + result.stderr
    trace = harness.trace_lines()
    assert "psql:database" in trace
    assert "psql:size" in trace
    assert "df" in trace
    assert "rclone:lsf" in trace
    assert "pg_dump" not in trace
    assert not any(line.startswith("planner:") for line in trace)
    assert _remote_delete_names(trace) == []
    assert all((harness.backup_dir / name).exists() for name in old_local)
    assert all((harness.remote_dir / name).exists() for name in old_remote)


def test_logging_pipeline_failure_is_not_masked(harness: BackupHarness) -> None:
    if os.name != "nt" and not Path("/dev/full").exists():
        pytest.skip("/dev/full is unavailable")

    result = harness.run("preflight", overrides={"LOG_FILE": "/dev/full"})

    assert result.returncode != 0
    assert "pg_dump" not in harness.trace_lines()


@pytest.mark.parametrize("quote", ["'", '"'])
def test_database_url_is_read_without_sourcing_and_never_logged(harness: BackupHarness, quote: str) -> None:
    marker = harness.root / "must-not-exist"
    harness.app_env.write_text(
        f"DATABASE_URL={quote}{SECRET}{quote}\n"
        f"UNRELATED=$(touch {_posix_path(marker)})\n",
        encoding="utf-8",
    )

    result = harness.run("preflight")

    assert result.returncode == 0, result.stdout + result.stderr
    assert not marker.exists()
    combined = result.stdout + result.stderr + harness.log_file.read_text(encoding="utf-8")
    assert SECRET not in combined
    assert "do-not-log-rclone-secret" not in combined


@pytest.mark.parametrize(
    ("database_url", "expected_pgdatabase"),
    [
        ("postgresql://degen:uri-secret@db/degen", "postgresql://degen:uri-secret@db/degen"),
        ("postgres://degen:uri-secret@db/degen", "postgres://degen:uri-secret@db/degen"),
        ("postgresql+psycopg://degen:uri-secret@db/degen", "postgresql://degen:uri-secret@db/degen"),
    ],
)
def test_postgres_database_uri_forms_are_normalized_only_when_required(
    harness: BackupHarness,
    database_url: str,
    expected_pgdatabase: str,
) -> None:
    harness.app_env.write_text(f"DATABASE_URL='{database_url}'\n", encoding="utf-8")

    result = harness.run("preflight", overrides={"FAKE_EXPECT_PGDATABASE": expected_pgdatabase})

    assert result.returncode == 0, result.stdout + result.stderr
    combined = result.stdout + result.stderr + harness.log_file.read_text(encoding="utf-8")
    assert database_url not in combined
    assert expected_pgdatabase not in combined


@pytest.mark.parametrize("database_url", ["mysql://degen:bad-secret@db/degen", "postgresql+asyncpg://degen:bad-secret@db/degen"])
def test_non_postgres_or_other_driver_uri_is_rejected_without_logging(
    harness: BackupHarness,
    database_url: str,
) -> None:
    harness.app_env.write_text(f"DATABASE_URL='{database_url}'\n", encoding="utf-8")

    result = harness.run("preflight")

    assert result.returncode != 0
    assert "psql:database" not in harness.trace_lines()
    combined = result.stdout + result.stderr + harness.log_file.read_text(encoding="utf-8")
    assert database_url not in combined
    assert "bad-secret" not in combined


@pytest.mark.parametrize("app_file_state", ["missing", "malicious"])
def test_preset_database_url_takes_precedence_without_logging_or_sourcing(
    harness: BackupHarness,
    app_file_state: str,
) -> None:
    preset = "postgresql://preset:preset-secret@db/preset"
    marker = harness.root / "preset-must-not-source"
    overrides: dict[str, str | None] = {
        "DATABASE_URL": preset,
        "FAKE_EXPECT_PGDATABASE": preset,
    }
    if app_file_state == "missing":
        overrides["APP_ENV_FILE"] = _posix_path(harness.root / "missing-web.env")
    else:
        harness.app_env.write_text(
            "DATABASE_URL='mysql://wrong:app-file-secret@db/wrong'\n"
            f"MALICIOUS=$(touch {_posix_path(marker)})\n",
            encoding="utf-8",
        )

    result = harness.run("preflight", overrides=overrides)

    assert result.returncode == 0, result.stdout + result.stderr
    assert not marker.exists()
    combined = result.stdout + result.stderr + harness.log_file.read_text(encoding="utf-8")
    assert preset not in combined
    assert "preset-secret" not in combined
    assert "app-file-secret" not in combined


def test_unsafe_planner_output_is_rejected_before_any_candidate_delete(harness: BackupHarness) -> None:
    old_local = _seed_old_pairs(harness.backup_dir)
    sentinel = harness.root / "outside-sentinel"
    sentinel.write_text("preserve", encoding="utf-8")
    first_dump, first_sidecar = _pair_names("20260626T230000Z")
    malicious = textwrap.dedent(
        f'''\
        #!/usr/bin/env python3
        print({first_dump!r})
        print({first_sidecar!r})
        print("../outside-sentinel")
        '''
    )
    _write_executable(harness.planner, malicious)

    result = harness.run(overrides={"REMOTE_PRUNE_ENABLED": "1"})

    assert result.returncode != 0
    assert sentinel.exists()
    assert all((harness.backup_dir / name).exists() for name in old_local)
    trace = harness.trace_lines()
    assert f"rm:{first_dump}" not in trace
    assert f"rm:{first_sidecar}" not in trace
    assert not [name for name in _remote_delete_names(trace) if not name.startswith(".degen-upload-")]
    assert "Unsafe retention candidate" in result.stdout


def test_inventory_present_candidate_under_another_safe_prefix_is_rejected(harness: BackupHarness) -> None:
    other_dump, other_sidecar = _seed_pair(harness.backup_dir, "20260626T230000Z", prefix="other_green_")
    malicious = "#!/usr/bin/env python3\n" + f"print({other_dump!r})\nprint({other_sidecar!r})\n"
    _write_executable(harness.planner, malicious)

    result = harness.run()

    assert result.returncode != 0
    assert (harness.backup_dir / other_dump).exists()
    assert (harness.backup_dir / other_sidecar).exists()
    trace = harness.trace_lines()
    assert f"rm:{other_dump}" not in trace
    assert f"rm:{other_sidecar}" not in trace
    assert "Unsafe retention candidate" in result.stdout


def test_planner_cannot_delete_the_current_backup_pair(harness: BackupHarness) -> None:
    old_local = _seed_old_pairs(harness.backup_dir)
    old_remote = _seed_old_pairs(harness.remote_dir)
    current_dump, current_sidecar = _pair_names()
    malicious = textwrap.dedent(
        f'''\
        #!/usr/bin/env python3
        print({current_dump!r})
        print({current_sidecar!r})
        '''
    )
    _write_executable(harness.planner, malicious)

    result = harness.run(overrides={"REMOTE_PRUNE_ENABLED": "1"})

    assert result.returncode != 0
    assert all((harness.backup_dir / name).exists() for name in old_local)
    assert all((harness.remote_dir / name).exists() for name in old_remote)
    assert (harness.backup_dir / current_dump).exists()
    assert (harness.backup_dir / current_sidecar).exists()
    assert (harness.remote_dir / current_dump).exists()
    assert (harness.remote_dir / current_sidecar).exists()
    assert "Unsafe retention candidate" in result.stdout


@pytest.mark.parametrize("attack", ["not-in-inventory", "incomplete-pair", "duplicate-pair"])
def test_planner_candidates_must_be_inventory_members_unique_complete_pairs(
    harness: BackupHarness,
    attack: str,
) -> None:
    seeded_dump, seeded_sidecar = _seed_pair(harness.backup_dir, "20260626T230000Z")
    if attack == "not-in-inventory":
        emitted_dump, emitted_sidecar = _pair_names("20260620T230000Z")
        emitted = [emitted_dump, emitted_sidecar]
    elif attack == "incomplete-pair":
        emitted = [seeded_dump]
    else:
        emitted = [seeded_dump, seeded_sidecar, seeded_dump, seeded_sidecar]
    malicious = "#!/usr/bin/env python3\n" + "\n".join(f"print({name!r})" for name in emitted) + "\n"
    _write_executable(harness.planner, malicious)

    result = harness.run()

    assert result.returncode != 0
    assert (harness.backup_dir / seeded_dump).exists()
    assert (harness.backup_dir / seeded_sidecar).exists()
    current_dump, current_sidecar = _pair_names()
    assert (harness.backup_dir / current_dump).exists()
    assert (harness.backup_dir / current_sidecar).exists()
    assert (harness.remote_dir / current_dump).exists()
    assert (harness.remote_dir / current_sidecar).exists()
    trace = harness.trace_lines()
    assert f"rm:{seeded_dump}" not in trace
    assert f"rm:{seeded_sidecar}" not in trace
    assert "Unsafe retention candidate" in result.stdout


@pytest.mark.parametrize("configured_prefix", ["manual_", "Degen_green_prod_green_"])
def test_configured_backup_prefix_must_exactly_match_live_database_and_host_identity(
    harness: BackupHarness,
    configured_prefix: str,
) -> None:
    result = harness.run(overrides={"BACKUP_PREFIX": configured_prefix})

    assert result.returncode != 0
    assert "Configured backup prefix does not match live database and host identity" in (
        result.stdout + result.stderr
    )
    trace = harness.trace_lines()
    assert "psql:database" in trace
    assert "hostname" in trace
    assert "pg_dump" not in trace
    assert not any(line.startswith("rclone:") for line in trace)


def test_matching_configured_backup_prefix_still_queries_live_identity(
    harness: BackupHarness,
) -> None:
    result = harness.run(overrides={"BACKUP_PREFIX": PREFIX})

    assert result.returncode == 0, result.stdout + result.stderr
    trace = harness.trace_lines()
    assert trace.index("psql:database") < trace.index("hostname") < trace.index("rclone:lsf")
    current_dump, current_sidecar = _pair_names()
    assert (harness.backup_dir / current_dump).exists()
    assert (harness.backup_dir / current_sidecar).exists()


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("KEEP_LOCAL_COUNT", "-1"),
        ("KEEP_REMOTE_DAILY", "one"),
        ("KEEP_REMOTE_WEEKLY", "-4"),
        ("KEEP_REMOTE_MONTHLY", "3.5"),
        ("MIN_FREE_AFTER_BYTES", "-1"),
        ("REMOTE_PRUNE_ENABLED", "yes"),
        ("REMOTE_PRUNE_ENABLED", "2"),
    ],
)
def test_invalid_numeric_configuration_is_rejected(
    harness: BackupHarness,
    variable: str,
    value: str,
) -> None:
    result = harness.run("preflight", overrides={variable: value})

    assert result.returncode != 0
    assert "Invalid configuration" in result.stdout + result.stderr
    assert "pg_dump" not in harness.trace_lines()


@pytest.mark.parametrize("mode", ["destroy-everything", ""])
def test_unknown_mode_is_rejected(harness: BackupHarness, mode: str) -> None:
    result = harness.run(mode)

    assert result.returncode != 0
    assert "Unsupported mode" in result.stdout + result.stderr
    assert "pg_dump" not in harness.trace_lines()


@pytest.mark.parametrize(
    "overrides",
    [
        {"BACKUP_PREFIX": "../escape"},
        {"FAKE_HOST": "green/other"},
        {"FAKE_DB_NAME": "bad database name"},
    ],
)
def test_unsafe_backup_labels_are_rejected(harness: BackupHarness, overrides: dict[str, str]) -> None:
    result = harness.run("preflight", overrides=overrides)

    assert result.returncode != 0
    assert "Unsafe backup label" in result.stdout + result.stderr
    assert "pg_dump" not in harness.trace_lines()
