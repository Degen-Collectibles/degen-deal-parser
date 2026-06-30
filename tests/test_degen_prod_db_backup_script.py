from __future__ import annotations

import hashlib
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
PLANNER = ROOT / "deploy" / "linux" / "degen-prod-db-retention.py"
STAMP = "20260629T230000Z"
PREFIX = "degen_green_prod_green_"
SECRET = "postgresql://degen:do-not-log-this@db.internal/degen_green_prod"


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
    return f"/mnt/{drive[0].lower()}/{tail.lstrip('\\/').replace('\\', '/')}"


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
    root_name = os.environ["RCLONE_REMOTE_PATH"]
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


if name == "hostname":
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
    fail_if("pg_restore")
    dump = Path(args[-1])
    if not dump.read_bytes().startswith(b"DEGEN-CUSTOM-DUMP"):
        raise SystemExit(76)
    print("; Archive created for tests")
elif name == "sha256sum":
    trace("sha256sum")
    fail_if("sha256sum")
    targets = [item for item in args if item != "--" and not item.startswith("-")]
    target = Path(targets[-1])
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
        try:
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        except FileNotFoundError:
            if not force:
                raise
elif name == "rclone":
    immutable = "--immutable" in args
    filtered = []
    index = 0
    while index < len(args):
        if args[index] == "--config" and index + 1 < len(args):
            index += 2
            continue
        if args[index] == "--immutable":
            index += 1
            continue
        filtered.append(args[index])
        index += 1
    args = filtered
    if not args:
        raise SystemExit(77)
    operation = args[0]
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
        if immutable and target.exists():
            raise SystemExit(83)
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
        fail_if(f"move_{kind}")
        if os.environ.get("FAKE_REQUIRE_IMMUTABLE") == "1" and not immutable:
            raise SystemExit(84)
        if immutable and target.exists():
            raise SystemExit(85)
        if os.environ.get("FAKE_MOVE_LEAVES_SOURCE") == kind:
            target.write_bytes(source.read_bytes())
        else:
            source.replace(target)
    elif operation == "deletefile":
        target = remote_path(args[1])
        trace(f"rclone:delete:{target.name}")
        fail_if("deletefile")
        try:
            target.unlink()
        except FileNotFoundError:
            pass
    else:
        print(f"unexpected rclone operation: {operation}", file=sys.stderr)
        raise SystemExit(78)
else:
    print(f"unexpected fake command: {name}", file=sys.stderr)
    raise SystemExit(79)
'''


def _planner_wrapper(real_planner: str) -> str:
    return textwrap.dedent(
        f'''\
        #!/usr/bin/env python3
        import os
        from pathlib import Path
        import sys

        args = sys.argv[1:]
        mode = args[args.index("--mode") + 1]
        with Path(os.environ["FAKE_TRACE"]).open("a", encoding="utf-8") as handle:
            handle.write(f"planner:{{mode}}\\n")
        if os.environ.get("FAKE_FAIL") in {{"planner", f"planner_{{mode}}"}}:
            raise SystemExit(81)
        os.execv("/usr/bin/python3", ["python3", {real_planner!r}, *args])
        '''
    )


@dataclass
class BackupHarness:
    root: Path
    bash: str
    backup_dir: Path
    log_dir: Path
    remote_dir: Path
    fake_bin: Path
    app_env: Path
    rclone_config: Path
    planner: Path
    trace: Path
    lock_file: Path
    runner: Path
    lock_holder: Path
    signal_runner: Path

    @classmethod
    def create(cls, tmp_path: Path, bash: str) -> "BackupHarness":
        root = _native_behavior_root(tmp_path)
        backup_dir = root / "backups"
        log_dir = root / "logs"
        remote_dir = root / "remote"
        fake_bin = root / "fake-bin"
        for path in (backup_dir, log_dir, remote_dir, fake_bin):
            path.mkdir(parents=True, mode=0o700)
            _chmod(path, 0o700)

        for command in ("date", "df", "hostname", "mktemp", "pg_dump", "pg_restore", "psql", "rclone", "rm", "sha256sum"):
            _write_executable(fake_bin / command, FAKE_COMMAND)

        app_env = root / "web.env"
        app_env.write_text(f"DATABASE_URL='{SECRET}'\n", encoding="utf-8")
        _chmod(app_env, 0o600)
        rclone_config = root / "rclone.conf"
        rclone_config.write_text("token = do-not-log-rclone-secret\n", encoding="utf-8")
        _chmod(rclone_config, 0o600)
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
exec 9>"$1"
/usr/bin/flock -n 9
printf 'READY\\n'
IFS= read -r _
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
        return cls(
            root=root,
            bash=bash,
            backup_dir=backup_dir,
            log_dir=log_dir,
            remote_dir=remote_dir,
            fake_bin=fake_bin,
            app_env=app_env,
            rclone_config=rclone_config,
            planner=planner,
            trace=trace,
            lock_file=root / "backup.lock",
            runner=runner,
            lock_holder=lock_holder,
            signal_runner=signal_runner,
        )

    @property
    def log_file(self) -> Path:
        return self.log_dir / "prod-db-backup.log"

    def environment(self, overrides: dict[str, str | None] | None = None) -> dict[str, str]:
        values: dict[str, str] = {
            "APP_ENV_FILE": _posix_path(self.app_env),
            "BACKUP_DIR": _posix_path(self.backup_dir),
            "LOG_DIR": _posix_path(self.log_dir),
            "LOG_FILE": _posix_path(self.log_file),
            "RCLONE_CONFIG": _posix_path(self.rclone_config),
            "RCLONE_REMOTE_PATH": "test:backups/degen-db",
            "RETENTION_PLANNER": _posix_path(self.planner),
            "LOCK_FILE": _posix_path(self.lock_file),
            "MIN_FREE_AFTER_BYTES": "100",
            "FAKE_BIN": _posix_path(self.fake_bin),
            "FAKE_TRACE": _posix_path(self.trace),
            "FAKE_REMOTE_ROOT": _posix_path(self.remote_dir),
            "FAKE_DB_SIZE": "4096",
            "FAKE_DF_AVAILABLE": "1000000000",
            "FAKE_DB_NAME": "degen_green_prod",
            "FAKE_HOST": "green",
            "FAKE_NOW": STAMP,
            "BACKUP_SCRIPT": _posix_path(SCRIPT),
        }
        if overrides:
            for key, value in overrides.items():
                if value is None:
                    values.pop(key, None)
                else:
                    values[key] = value

        env = os.environ.copy()
        env.pop("DATABASE_URL", None)
        env.update(values)
        if os.name == "nt":
            env["WSLENV"] = ":".join(sorted(values))
        else:
            env["PATH"] = f"{values['FAKE_BIN']}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        return env

    def run(
        self,
        mode: str | None = None,
        *,
        overrides: dict[str, str | None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = self.environment(overrides)
        arguments = [] if mode is None else [mode]
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
    (directory / dump_name).write_bytes(f"old-dump-{stamp}\n".encode())
    (directory / sidecar_name).write_text(f"{'0' * 64}  {dump_name}\n", encoding="ascii")
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


def test_source_declares_safe_shell_contract_and_live_defaults() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert source.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in source
    assert re.search(r"^umask 077$", source, re.MULTILINE)
    assert "set -x" not in source
    assert "--ignore-existing" not in source
    assert "trap 'exit 129' HUP" in source
    assert "trap 'exit 130' INT" in source
    assert "trap 'exit 143' TERM" in source
    assert "owned_local_dump_partial=$(mktemp" in source
    assert "owned_local_sidecar_partial=$(mktemp" in source
    assert "owned_upload_token_file=$(mktemp" in source
    assert not re.search(r"if ! dump_partial=\$\(mktemp", source)
    assert not re.search(r"if ! sidecar_partial=\$\(mktemp", source)
    for expected in (
        "APP_ENV_FILE=${APP_ENV_FILE:-/opt/degen/web.env}",
        "BACKUP_DIR=${BACKUP_DIR:-/opt/degen/backups/db}",
        "LOG_DIR=${LOG_DIR:-/var/log/degen}",
        "RCLONE_CONFIG=${RCLONE_CONFIG:-/etc/degen/rclone.conf}",
        "RCLONE_REMOTE_PATH=${RCLONE_REMOTE_PATH:-onedrive:backups/degen-db}",
        "KEEP_LOCAL_COUNT=${KEEP_LOCAL_COUNT:-2}",
        "KEEP_REMOTE_DAILY=${KEEP_REMOTE_DAILY:-7}",
        "KEEP_REMOTE_WEEKLY=${KEEP_REMOTE_WEEKLY:-4}",
        "KEEP_REMOTE_MONTHLY=${KEEP_REMOTE_MONTHLY:-3}",
        "REMOTE_PRUNE_ENABLED=${REMOTE_PRUNE_ENABLED:-0}",
        "MIN_FREE_AFTER_BYTES=${MIN_FREE_AFTER_BYTES:-10737418240}",
        "RETENTION_PLANNER=${RETENTION_PLANNER:-/usr/local/sbin/degen-prod-db-retention}",
        "LOCK_FILE=${LOCK_FILE:-/run/lock/degen-prod-db-backup.lock}",
        'LOG_FILE=${LOG_FILE:-$LOG_DIR/prod-db-backup.log}',
    ):
        assert expected in source


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
    first_local_delete = trace.index(f"rm:{_pair_names('20260626T230000Z')[0]}")
    assert trace.index("pg_restore") < trace.index("rclone:copy:dump")
    assert trace.index("rclone:final-content") < trace.index("planner:local") < first_local_delete
    assert first_local_delete < trace.index("planner:remote")
    assert "Backup completed successfully" in result.stdout
    log_lines = harness.log_file.read_text(encoding="utf-8").splitlines()
    assert log_lines
    assert all(re.match(r"^\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\] ", line) for line in log_lines)


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
    assert not [
        path
        for path in harness.remote_dir.iterdir()
        if path.name.lower().startswith(".degen-upload-") and current_dump.lower() in path.name.lower()
    ]
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


def test_remote_moves_use_immutable_no_overwrite_semantics(harness: BackupHarness) -> None:
    result = harness.run(overrides={"FAKE_REQUIRE_IMMUTABLE": "1"})

    assert result.returncode == 0, result.stdout + result.stderr
    current_dump, current_sidecar = _pair_names()
    assert (harness.remote_dir / current_dump).exists()
    assert (harness.remote_dir / current_sidecar).exists()


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
    assert not any(path.name.startswith(".degen-upload-") for path in harness.remote_dir.iterdir())
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


def test_remote_delete_failure_stops_and_preserves_undeleted_objects(harness: BackupHarness) -> None:
    old_remote = _seed_old_pairs(harness.remote_dir)

    result = harness.run(
        overrides={
            "FAKE_FAIL": "deletefile",
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
    deleted_attempts = _remote_delete_names(harness.trace_lines())
    assert deleted_attempts == [old_remote[0]]


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


def test_backup_prefix_override_is_the_complete_owned_prefix(harness: BackupHarness) -> None:
    result = harness.run(overrides={"BACKUP_PREFIX": "manual_"})

    assert result.returncode == 0, result.stdout + result.stderr
    dump_name = f"manual_{STAMP}.dump"
    sidecar_name = f"{dump_name}.sha256"
    assert (harness.backup_dir / dump_name).exists()
    assert (harness.backup_dir / sidecar_name).exists()
    assert (harness.remote_dir / dump_name).exists()
    assert (harness.remote_dir / sidecar_name).exists()
    assert not any(path.name.startswith("manual__green_") for path in harness.backup_dir.iterdir())


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
    assert "Invalid configuration" in result.stdout
    assert "pg_dump" not in harness.trace_lines()


@pytest.mark.parametrize("mode", ["destroy-everything", ""])
def test_unknown_mode_is_rejected(harness: BackupHarness, mode: str) -> None:
    result = harness.run(mode)

    assert result.returncode != 0
    assert "Unsupported mode" in result.stdout
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
    assert "Unsafe backup label" in result.stdout
    assert "pg_dump" not in harness.trace_lines()
