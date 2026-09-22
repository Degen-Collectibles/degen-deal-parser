import html
import re
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader, select_autoescape

TEMPLATE_DIR = Path("app/templates")
PAYLOAD = "x');alert(document.cookie);//\" onmouseover=alert(1) x='"


def _render_edit_buttons() -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=select_autoescape(["html"]))
    source = (TEMPLATE_DIR / "stream_manager.html").read_text(encoding="utf-8")
    snippets = re.findall(r"<button[^\n]*onclick='openEdit(?:Streamer|Account)\([^\n]*</button>", source)
    assert len(snippets) == 2, "expected both edit buttons to use single-quoted tojson handlers"
    template = env.from_string("\n".join(snippets))
    streamer = SimpleNamespace(id=1, name=PAYLOAD, display_name=PAYLOAD, avatar_emoji=PAYLOAD, color=PAYLOAD)
    account = SimpleNamespace(id=2, name=PAYLOAD, platform=PAYLOAD, handle=PAYLOAD)
    return template.render(s=streamer, acct=account)


def test_edit_handlers_keep_hostile_names_inside_js_string_literals():
    rendered = _render_edit_buttons()
    handlers = re.findall(r"onclick='([^']*)'", rendered)
    assert len(handlers) == 2
    for handler in handlers:
        js = html.unescape(handler)
        # Every hostile value is a JSON string literal: the payload's quote
        # characters never terminate the attribute or the JS string.
        assert "alert(document.cookie);//" in js
        assert js.count("'") == 0
    # Outside the two onclick attributes, no attacker text leaked into markup.
    outside = re.sub(r"onclick='[^']*'", "", rendered)
    assert "onmouseover" not in outside
    assert "alert" not in outside
