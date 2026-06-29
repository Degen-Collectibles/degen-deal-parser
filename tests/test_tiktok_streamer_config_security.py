import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

import app.routers.tiktok_streamer as streamer_module


MALICIOUS_TITLE = '<img src=x onerror=alert(1)> "Rare\'s" & slabs'


def _request() -> Request:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/tiktok/streamer/config",
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    request.state.current_user = SimpleNamespace(
        id=1,
        username="admin",
        display_name="Admin User",
        role="admin",
    )
    return request


def _render_config(title: str) -> str:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    try:
        with Session(engine) as session, patch.object(
            streamer_module,
            "require_role_response",
            return_value=None,
        ), patch.object(
            streamer_module,
            "_get_live_session_snapshot",
            return_value={
                "ok": True,
                "title": title,
                "start_time": 1_710_000_000,
                "end_time": 0,
                "gmv": 123.45,
            },
        ):
            response = streamer_module.tiktok_streamer_config(_request(), session)
    finally:
        engine.dispose()

    assert response.status_code == 200
    return response.body.decode("utf-8")


def test_streamer_config_escapes_provider_controlled_live_title() -> None:
    body = _render_config(MALICIOUS_TITLE)

    assert MALICIOUS_TITLE not in body
    assert "<img" not in body.lower()
    assert "&lt;img src=x onerror=alert(1)&gt; &quot;Rare&#x27;s&quot; &amp; slabs" in body


def test_streamer_config_preserves_plain_live_title_text() -> None:
    body = _render_config("Fresh slab drops")

    assert "<strong>Fresh slab drops</strong>" in body


def test_public_live_status_json_preserves_raw_provider_title() -> None:
    checked_at = datetime.now(timezone.utc)
    sessions = [
        {
            "ok": True,
            "id": "main-live",
            "title": MALICIOUS_TITLE,
            "username": "degencollectibles",
            "start_time": int(checked_at.timestamp()) - 300,
            "end_time": 0,
        }
    ]

    with patch.object(
        streamer_module,
        "_get_live_sessions_list",
        return_value=sessions,
    ), patch.object(
        streamer_module,
        "_get_live_sessions_list_checked_at",
        return_value=checked_at,
    ), patch.object(
        streamer_module,
        "_get_live_session_snapshot",
        return_value={},
    ):
        response = streamer_module.public_tiktok_live_status()

    payload = json.loads(response.body)
    main_channel = next(channel for channel in payload["channels"] if channel["id"] == "degencollectibles")
    assert main_channel["title"] == MALICIOUS_TITLE
