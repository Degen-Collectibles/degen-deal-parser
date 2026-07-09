from __future__ import annotations

import asyncio
import signal
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.inventory import pricing


def _item() -> SimpleNamespace:
    return SimpleNamespace(
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
        cert_number="12345678",
    )


class _CompletedProcess:
    def __init__(
        self,
        *,
        pid: int = 12345,
        returncode: int = 0,
        stdout: bytes = b"saved comps",
        stderr: bytes = b"",
    ) -> None:
        self.pid = pid
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.communicate_calls = 0
        self.wait_calls = 0

    async def communicate(self):
        self.communicate_calls += 1
        return self.stdout, self.stderr

    async def wait(self):
        self.wait_calls += 1
        return self.returncode

    def kill(self):
        self.returncode = -9


class _BlockingProcess(_CompletedProcess):
    def __init__(self, *, pid: int = 12345) -> None:
        super().__init__(pid=pid, returncode=None)  # type: ignore[arg-type]
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.communicate_cancelled = False

    async def communicate(self):
        self.communicate_calls += 1
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.communicate_cancelled = True
            raise
        self.returncode = 0
        return self.stdout, self.stderr


async def _force_timeout(awaitable, timeout):
    task = asyncio.ensure_future(awaitable)
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    raise asyncio.TimeoutError


@pytest.fixture(autouse=True)
def _isolated_card_ladder_profile(tmp_path, monkeypatch):
    monkeypatch.setenv("CARDLADDER_PROFILE_DIR", str(tmp_path / "chrome-profile"))
    monkeypatch.setattr(pricing, "_CARD_LADDER_SYNC_LOCK", asyncio.Lock(), raising=False)
    monkeypatch.setattr(
        pricing,
        "_CARD_LADDER_MIN_TIMEOUT_SECONDS",
        0.01,
        raising=False,
    )


def test_card_ladder_sync_launches_the_repo_cli(monkeypatch):
    spawned: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def fake_spawn(*args, **kwargs):
        spawned.append((args, kwargs))
        return _CompletedProcess()

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_fetch_card_ladder_cli_cache",
        lambda *args, **kwargs: {"source": "card_ladder"},
    )

    result = asyncio.run(pricing.sync_card_ladder_cli_for_item(_item()))

    assert result == {"source": "card_ladder"}
    assert len(spawned) == 1
    command = spawned[0][0]
    assert Path(str(command[1])).parts[-2:] == ("scripts", "cardladder_cli.py")


def test_card_ladder_timeout_awaits_tree_cleanup_before_raising(monkeypatch):
    cleanup_finished = False
    process_holder: list[_BlockingProcess] = []

    async def fake_spawn(*args, **kwargs):
        process = _BlockingProcess()
        process_holder.append(process)
        return process

    async def fake_cleanup(process):
        nonlocal cleanup_finished
        assert process is process_holder[0]
        await asyncio.sleep(0)
        process.returncode = -9
        cleanup_finished = True

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(pricing.asyncio, "wait_for", _force_timeout)
    monkeypatch.setattr(
        pricing,
        "_terminate_card_ladder_process_tree",
        fake_cleanup,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="timed out"):
        asyncio.run(
            pricing.sync_card_ladder_cli_for_item(_item(), timeout_seconds=0)
        )

    assert process_holder[0].communicate_cancelled is True
    assert cleanup_finished is True


def test_card_ladder_timeout_releases_locks_for_retry(monkeypatch):
    spawned: list[_CompletedProcess] = []
    real_wait_for = asyncio.wait_for
    wait_calls = 0

    async def fake_spawn(*args, **kwargs):
        process: _CompletedProcess
        if not spawned:
            process = _BlockingProcess()
        else:
            process = _CompletedProcess()
        spawned.append(process)
        return process

    async def fake_cleanup(process):
        process.returncode = -9

    async def timeout_once(awaitable, timeout):
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls == 1:
            return await _force_timeout(awaitable, timeout)
        return await real_wait_for(awaitable, timeout)

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(pricing.asyncio, "wait_for", timeout_once)
    monkeypatch.setattr(
        pricing,
        "_terminate_card_ladder_process_tree",
        fake_cleanup,
        raising=False,
    )
    monkeypatch.setattr(
        pricing,
        "_fetch_card_ladder_cli_cache",
        lambda *args, **kwargs: {"source": "card_ladder"},
    )

    async def run_scenario():
        with pytest.raises(RuntimeError, match="timed out"):
            await pricing.sync_card_ladder_cli_for_item(_item(), timeout_seconds=0)
        return await pricing.sync_card_ladder_cli_for_item(_item())

    result = asyncio.run(run_scenario())

    assert result == {"source": "card_ladder"}
    assert len(spawned) == 2


def test_card_ladder_cancellation_waits_for_tree_cleanup(monkeypatch):
    process_holder: list[_BlockingProcess] = []
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def fake_spawn(*args, **kwargs):
        process = _BlockingProcess()
        process_holder.append(process)
        return process

    async def fake_cleanup(process):
        cleanup_started.set()
        await allow_cleanup.wait()
        process.returncode = -9
        cleanup_finished.set()

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_terminate_card_ladder_process_tree",
        fake_cleanup,
        raising=False,
    )

    async def run_scenario():
        task = asyncio.create_task(
            pricing.sync_card_ladder_cli_for_item(_item(), timeout_seconds=60)
        )
        while not process_holder:
            await asyncio.sleep(0)
        await process_holder[0].started.wait()
        task.cancel()
        try:
            await asyncio.wait_for(cleanup_started.wait(), timeout=0.25)
        except asyncio.TimeoutError:
            pytest.fail("caller cancellation did not start process-tree cleanup")
        assert task.done() is False
        allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cleanup_finished.is_set()

    asyncio.run(run_scenario())


def test_card_ladder_cancellation_during_spawn_reaps_created_process(monkeypatch):
    spawn_started = asyncio.Event()
    allow_spawn_return = asyncio.Event()
    process = _BlockingProcess()
    cleanup_calls: list[_BlockingProcess] = []

    async def fake_spawn(*args, **kwargs):
        spawn_started.set()
        await allow_spawn_return.wait()
        return process

    async def fake_cleanup(created_process):
        cleanup_calls.append(created_process)
        created_process.returncode = -9

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_terminate_card_ladder_process_tree",
        fake_cleanup,
    )

    async def run_scenario():
        task = asyncio.create_task(pricing.sync_card_ladder_cli_for_item(_item()))
        await spawn_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert task.done() is False
        allow_spawn_return.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_scenario())

    assert cleanup_calls == [process]


def test_card_ladder_concurrent_call_is_rejected_without_second_spawn(monkeypatch):
    spawned: list[_BlockingProcess] = []

    async def fake_spawn(*args, **kwargs):
        if spawned:
            raise AssertionError("concurrent call spawned a second process")
        process = _BlockingProcess()
        spawned.append(process)
        return process

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_fetch_card_ladder_cli_cache",
        lambda *args, **kwargs: {"source": "card_ladder"},
    )

    async def run_scenario():
        first = asyncio.create_task(pricing.sync_card_ladder_cli_for_item(_item()))
        while not spawned:
            await asyncio.sleep(0)
        await spawned[0].started.wait()
        with pytest.raises(RuntimeError, match="already running"):
            await pricing.sync_card_ladder_cli_for_item(_item())
        assert len(spawned) == 1
        spawned[0].release.set()
        assert await first == {"source": "card_ladder"}

    asyncio.run(run_scenario())


def test_card_ladder_profile_lock_is_nonblocking_and_released_for_retry(
    monkeypatch,
):
    from scripts import cardladder_cli

    lock_factory = getattr(pricing, "_card_ladder_profile_lock", None)
    assert callable(lock_factory)
    spawned: list[_CompletedProcess] = []

    async def fake_spawn(*args, **kwargs):
        process = _CompletedProcess()
        spawned.append(process)
        return process

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_fetch_card_ladder_cli_cache",
        lambda *args, **kwargs: {"source": "card_ladder"},
    )

    profile_dir = cardladder_cli.default_profile_dir()
    with lock_factory(profile_dir):
        with pytest.raises(RuntimeError, match="already running"):
            asyncio.run(pricing.sync_card_ladder_cli_for_item(_item()))
    result = asyncio.run(pricing.sync_card_ladder_cli_for_item(_item()))

    assert result == {"source": "card_ladder"}
    assert len(spawned) == 1


def test_card_ladder_subprocess_uses_platform_process_group_flags():
    kwargs_factory = getattr(pricing, "_card_ladder_subprocess_kwargs", None)
    assert callable(kwargs_factory)

    assert kwargs_factory(is_windows=False) == {"start_new_session": True}
    windows_kwargs = kwargs_factory(is_windows=True)
    assert windows_kwargs == {
        "creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200),
    }
    assert "shell" not in windows_kwargs


def test_windows_tree_cleanup_uses_taskkill_and_reaps(monkeypatch):
    terminator = getattr(pricing, "_terminate_card_ladder_process_tree", None)
    assert callable(terminator)
    victim = _BlockingProcess(pid=43210)
    commands: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class _TaskkillProcess(_CompletedProcess):
        async def communicate(self):
            victim.returncode = -9
            victim.release.set()
            return await super().communicate()

    async def fake_spawn(*args, **kwargs):
        commands.append((args, kwargs))
        return _TaskkillProcess()

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)

    asyncio.run(terminator(victim, is_windows=True))

    assert commands[0][0] == ("taskkill", "/PID", "43210", "/T", "/F")
    assert "shell" not in commands[0][1]
    assert victim.communicate_calls >= 1


def test_windows_tree_cleanup_falls_back_to_recorded_descendants(monkeypatch):
    victim = _CompletedProcess(pid=43211, returncode=7)
    commands: list[tuple[object, ...]] = []

    async def fake_spawn(*args, **kwargs):
        commands.append(args)
        target_pid = int(args[2])
        return _CompletedProcess(returncode=128 if target_pid == victim.pid else 0)

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_windows_descendant_pids",
        lambda parent_pid: [43212, 43213],
        raising=False,
    )

    asyncio.run(pricing._terminate_card_ladder_process_tree(victim, is_windows=True))

    assert commands == [
        ("taskkill", "/PID", "43211", "/T", "/F"),
        ("taskkill", "/PID", "43212", "/T", "/F"),
        ("taskkill", "/PID", "43213", "/T", "/F"),
    ]
    assert victim.communicate_calls == 1


def test_windows_tree_cleanup_closes_job_object_fallback(monkeypatch):
    victim = _CompletedProcess(pid=43214, returncode=7)
    closed: list[_CompletedProcess] = []

    async def fake_spawn(*args, **kwargs):
        return _CompletedProcess(returncode=128)

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(pricing, "_windows_descendant_pids", lambda parent_pid: [])
    monkeypatch.setattr(
        pricing,
        "_close_card_ladder_windows_job",
        lambda process: closed.append(process),
        raising=False,
    )

    asyncio.run(pricing._terminate_card_ladder_process_tree(victim, is_windows=True))

    assert closed == [victim]
    assert victim.communicate_calls == 1


def test_posix_tree_cleanup_escalates_term_to_kill_and_reaps(monkeypatch):
    terminator = getattr(pricing, "_terminate_card_ladder_process_tree", None)
    assert callable(terminator)
    victim = _BlockingProcess(pid=54321)
    signals: list[tuple[int, signal.Signals]] = []

    def fake_killpg(pid, sent_signal):
        signals.append((pid, sent_signal))
        if sent_signal == signal.SIGKILL:
            victim.returncode = -9
            victim.release.set()

    monkeypatch.setattr(pricing.signal, "SIGKILL", 9, raising=False)
    monkeypatch.setattr(pricing.os, "killpg", fake_killpg, raising=False)
    monkeypatch.setattr(
        pricing,
        "_CARD_LADDER_TERMINATE_GRACE_SECONDS",
        0.01,
        raising=False,
    )

    asyncio.run(terminator(victim, is_windows=False))

    assert signals == [
        (54321, signal.SIGTERM),
        (54321, signal.SIGKILL),
    ]
    assert victim.communicate_calls >= 1


def test_card_ladder_communicate_failure_cleans_tree(monkeypatch):
    process = _CompletedProcess(returncode=None)  # type: ignore[arg-type]
    cleanup_calls: list[_CompletedProcess] = []

    async def broken_communicate():
        raise OSError("pipe failed")

    process.communicate = broken_communicate  # type: ignore[method-assign]

    async def fake_spawn(*args, **kwargs):
        return process

    async def fake_cleanup(failed_process):
        cleanup_calls.append(failed_process)
        failed_process.returncode = -9

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_terminate_card_ladder_process_tree",
        fake_cleanup,
        raising=False,
    )

    with pytest.raises(OSError, match="pipe failed"):
        asyncio.run(pricing.sync_card_ladder_cli_for_item(_item()))

    assert cleanup_calls == [process]


def test_card_ladder_nonzero_exit_cleans_tree_and_preserves_stderr(monkeypatch):
    process = _CompletedProcess(
        returncode=7,
        stdout=b"partial output",
        stderr=b"browser profile failed",
    )
    cleanup_calls: list[_CompletedProcess] = []

    async def fake_spawn(*args, **kwargs):
        return process

    async def fake_cleanup(failed_process):
        cleanup_calls.append(failed_process)

    monkeypatch.setattr(pricing.asyncio, "create_subprocess_exec", fake_spawn)
    monkeypatch.setattr(
        pricing,
        "_terminate_card_ladder_process_tree",
        fake_cleanup,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="browser profile failed"):
        asyncio.run(pricing.sync_card_ladder_cli_for_item(_item()))

    assert cleanup_calls == [process]
