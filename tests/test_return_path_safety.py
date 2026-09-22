from app.shared import build_return_url, safe_return_path


def test_safe_return_path_keeps_local_paths():
    for path in ("/deals", "/table", "/review-table", "/ledger", "/deals?page=2"):
        assert safe_return_path(path) == path


def test_safe_return_path_rejects_scripts_and_external_targets():
    for value in (
        "javascript:alert(1)",
        " JavaScript:alert(1)",
        "javascript%3Aalert(1)",
        "data:text/html,<script>alert(1)</script>",
        "https://evil.example/",
        "//evil.example/",
        "/%2Fevil.example/",
        "/\\evil.example/",
        "\\\\evil.example",
        "/deals\nLocation: https://evil.example",
        "",
        None,
    ):
        assert safe_return_path(value) == "/deals", value


def test_build_return_url_never_emits_unsafe_target():
    assert build_return_url("javascript:alert(1)//", page=2) == "/deals?page=2"
    assert build_return_url("/table", status="failed") == "/table?status=failed"
