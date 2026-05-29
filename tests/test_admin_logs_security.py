from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from app.routers.admin import admin_logs_page


def test_admin_logs_escape_log_content(tmp_path) -> None:
    log_path = tmp_path / "app.log"
    log_path.write_text("<script>alert('owned')</script>\nnormal line\n", encoding="utf-8")
    request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="admin")))

    with patch("app.routers.admin.require_role_response", return_value=None), patch(
        "app.routers.admin.resolve_runtime_log_path",
        return_value=log_path,
    ):
        response = admin_logs_page(request, file="app", lines=50)

    body = response.body.decode("utf-8")
    assert "&lt;script&gt;alert(&#x27;owned&#x27;)&lt;/script&gt;" in body
    assert "<script>alert('owned')</script>" not in body


def test_admin_logs_does_not_echo_invalid_file_query(tmp_path) -> None:
    log_path = tmp_path / "app.log"
    log_path.write_text("normal line\n", encoding="utf-8")
    request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role="admin")))
    injected_file = '"><script>alert("file")</script>'

    with patch("app.routers.admin.require_role_response", return_value=None), patch(
        "app.routers.admin.resolve_runtime_log_path",
        return_value=log_path,
    ):
        response = admin_logs_page(request, file=injected_file, lines=50)

    body = response.body.decode("utf-8")
    assert injected_file not in body
    assert 'href="/admin/logs?file=app&lines=50"' in body
