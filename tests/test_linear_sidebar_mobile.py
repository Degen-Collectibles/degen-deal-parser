from pathlib import Path


LINEAR_CSS = Path("app/static/linear.css")


def _linear_css() -> str:
    return LINEAR_CSS.read_text(encoding="utf-8")


def test_mobile_linear_sidebar_nav_scrolls_above_user_footer():
    css = _linear_css()

    assert ".linear-sidebar-nav" in css
    assert "overflow-y: auto" in css
    assert "-webkit-overflow-scrolling: touch" in css
    assert ".linear-sidebar-foot" in css
    assert "flex: 0 0 auto" in css


def test_mobile_linear_drawer_sits_above_bottom_nav():
    css = _linear_css()

    assert "z-index: 320" in css
    assert "z-index: 300" in css
    assert "height: 100dvh" in css
