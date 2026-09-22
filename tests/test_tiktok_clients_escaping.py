import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

TEMPLATE = Path("app/templates/tiktok_clients.html")


def _esc_source() -> str:
    source = TEMPLATE.read_text(encoding="utf-8")
    match = re.search(r"function esc\(s\) \{.*?\n    \}", source, re.S)
    assert match, "esc() helper not found in tiktok_clients.html"
    return match.group(0)


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_esc_escapes_quotes_for_attribute_contexts():
    payload = 'x" autofocus onfocus=alert(1) x=\'<b>&'
    script = _esc_source() + f"\nprocess.stdout.write(esc({json.dumps(payload)}) + '|' + esc(null));"
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True).stdout

    escaped, empty = out.split("|")
    assert escaped == "x&quot; autofocus onfocus=alert(1) x=&#39;&lt;b&gt;&amp;"
    assert empty == ""
