import asyncio
import inspect
from types import SimpleNamespace

import pytest

from app.discord import worker as worker_module
from app.discord import worker_service
from app.discord.worker import periodic_stitch_audit_loop, stitch_audit_task_enabled


def _settings(*, ingest=True, parser=True, audit=True):
    return SimpleNamespace(
        discord_ingest_enabled=ingest,
        parser_worker_enabled=parser,
        periodic_stitch_audit_enabled=audit,
    )


@pytest.mark.parametrize(
    "ingest,parser,audit,expected",
    [
        (True, True, True, True),
        (True, True, False, False),  # production on 2026-11-16: restart loop every 5 seconds
        (True, False, True, False),
        (False, True, True, False),
    ],
)
def test_stitch_audit_task_starts_only_when_every_switch_is_on(ingest, parser, audit, expected):
    assert stitch_audit_task_enabled(_settings(ingest=ingest, parser=parser, audit=audit)) is expected


def test_loop_exits_at_once_whenever_the_start_check_says_no(monkeypatch):
    # The start check must cover every case in which the loop itself gives up
    # immediately; otherwise a supervisor restarts it forever.
    for parser, audit in ((True, False), (False, True), (False, False)):
        monkeypatch.setattr(worker_module.settings, "parser_worker_enabled", parser)
        monkeypatch.setattr(worker_module.settings, "periodic_stitch_audit_enabled", audit)
        assert stitch_audit_task_enabled(_settings(parser=parser, audit=audit)) is False
        asyncio.run(asyncio.wait_for(periodic_stitch_audit_loop(asyncio.Event()), timeout=1))


def test_worker_and_web_start_the_task_through_the_shared_check():
    from app import main as main_module

    assert "if stitch_audit_task_enabled(settings):" in inspect.getsource(worker_service.run_worker_service)
    assert "if stitch_audit_task_enabled(settings):" in inspect.getsource(main_module)
