import atexit
import json
import os
import sys
from pathlib import Path
from typing import TextIO

from .config import BASE_DIR, get_settings
from .models import utcnow


class TeeStream:
    def __init__(self, primary: TextIO, mirror: TextIO):
        self._primary = primary
        self._mirror = mirror
        self.encoding = getattr(primary, "encoding", "utf-8")

    def write(self, data: str) -> int:
        self._primary.write(data)
        self._mirror.write(data)
        return len(data)

    def flush(self) -> None:
        try:
            self._primary.flush()
        except (OSError, ValueError):
            pass
        if getattr(self._mirror, "closed", False):
            return
        try:
            self._mirror.flush()
        except (OSError, ValueError):
            pass

    def isatty(self) -> bool:
        return bool(getattr(self._primary, "isatty", lambda: False)())

    def fileno(self) -> int:
        return self._primary.fileno()


def _close_runtime_log_handle(handle: TextIO) -> None:
    try:
        handle.flush()
    except (OSError, ValueError):
        pass
    try:
        handle.close()
    except (OSError, ValueError):
        pass


def setup_runtime_file_logging(default_name: str = "app.log") -> Path | None:
    settings = get_settings()
    if not settings.log_to_file:
        return None

    resolved_dir = Path(settings.log_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = BASE_DIR / resolved_dir
    resolved_dir.mkdir(parents=True, exist_ok=True)

    log_path = resolved_dir / default_name
    sentinel = f"_degen_log_path_{default_name.replace('.', '_')}"
    if getattr(sys, sentinel, None) == str(log_path):
        return log_path

    handle = open(log_path, "a", encoding="utf-8", buffering=1)
    setattr(sys, sentinel, str(log_path))

    if not getattr(sys.stdout, "_degen_tee_wrapped", False):
        sys.stdout = TeeStream(sys.stdout, handle)
        setattr(sys.stdout, "_degen_tee_wrapped", True)

    if not getattr(sys.stderr, "_degen_tee_wrapped", False):
        sys.stderr = TeeStream(sys.stderr, handle)
        setattr(sys.stderr, "_degen_tee_wrapped", True)

    atexit.register(_close_runtime_log_handle, handle)
    print(f"[logging] writing runtime output to {os.path.normpath(str(log_path))}")
    return log_path


def resolve_runtime_log_path(default_name: str) -> Path:
    settings = get_settings()
    resolved_dir = Path(settings.log_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = BASE_DIR / resolved_dir
    return resolved_dir / default_name


def structured_log_line(
    *,
    runtime: str,
    action: str,
    success: bool | None = None,
    error: str | None = None,
    **details,
) -> str:
    payload = {
        "timestamp": utcnow().isoformat(),
        "runtime": runtime,
        "action": action,
        "success": success,
        "error": error,
    }
    payload.update(details)
    return json.dumps(payload, default=str, sort_keys=True)
