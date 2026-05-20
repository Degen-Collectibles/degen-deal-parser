import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.config import Settings


def read_text(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def assert_contains(text: str, needle: str, message: str) -> None:
    if needle not in text:
        raise AssertionError(message)


def main() -> None:
    web_script = read_text("scripts/run_local_web.ps1")
    worker_script = read_text("scripts/run_local_worker.ps1")

    failures: list[str] = []

    for text, needle, message in [
        (
            web_script,
            'Remove-Item Env:DATABASE_URL',
            "run_local_web.ps1 should clear any inherited DATABASE_URL before booting.",
        ),
        (
            worker_script,
            'Remove-Item Env:DATABASE_URL',
            "run_local_worker.ps1 should clear any inherited DATABASE_URL before booting.",
        ),
        (
            web_script,
            'RUNTIME_NAME = "local_web"',
            "run_local_web.ps1 should set a distinct local_web runtime name.",
        ),
        (
            worker_script,
            'RUNTIME_NAME = "local_worker"',
            "run_local_worker.ps1 should set a distinct local_worker runtime name.",
        ),
        (
            web_script,
            'DATABASE_URL = "sqlite:///data/degen_live.db"',
            "run_local_web.ps1 should pin the local SQLite database.",
        ),
        (
            worker_script,
            'DATABASE_URL = "sqlite:///data/degen_live.db"',
            "run_local_worker.ps1 should pin the local SQLite database.",
        ),
    ]:
        try:
            assert_contains(text, needle, message)
        except AssertionError as exc:
            failures.append(str(exc))

    settings = Settings(runtime_name="local_web")
    expected_worker_runtime_name = "local_worker"
    if settings.effective_worker_runtime_name != expected_worker_runtime_name:
        failures.append(
            f"Settings(runtime_name='local_web').effective_worker_runtime_name should be {expected_worker_runtime_name!r}, "
            f"got {settings.effective_worker_runtime_name!r}."
        )

    if failures:
        raise AssertionError("\n".join(failures))

    print("Local split deployment contract looks good.")


if __name__ == "__main__":
    main()
