"""Build the team Surprise Set guide PDF from the Markdown source."""
from __future__ import annotations

import html
import base64
import re
import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "team" / "surprise-set-guide.md"
STATIC_DIR = ROOT / "app" / "static" / "team-documents"
PDF_OUT = STATIC_DIR / "surprise-set-guide.pdf"
STATIC_MD_OUT = STATIC_DIR / "surprise-set-guide.md"


def _inline(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    return escaped


def _image_src(src: str) -> str:
    clean = src.strip()
    if clean.startswith("/static/"):
        path = ROOT / "app" / clean.lstrip("/")
    else:
        path = (SOURCE.parent / clean).resolve()
    if path.exists():
        if path.suffix.lower() == ".svg":
            encoded = base64.b64encode(path.read_bytes()).decode("ascii")
            return f"data:image/svg+xml;base64,{encoded}"
        return path.as_uri()
    return html.escape(clean, quote=True)


def markdown_to_html(markdown: str) -> str:
    lines = markdown.splitlines()
    out: list[str] = []
    paragraph: list[str] = []
    list_type: str | None = None
    in_code = False
    code_lines: list[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            out.append(f"<p>{_inline(' '.join(paragraph))}</p>")
            paragraph = []

    def close_list() -> None:
        nonlocal list_type
        if list_type:
            out.append(f"</{list_type}>")
            list_type = None

    for raw in lines:
        line = raw.rstrip()
        if line.startswith("```"):
            flush_paragraph()
            close_list()
            if in_code:
                out.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
                code_lines = []
                in_code = False
            else:
                in_code = True
            continue

        if in_code:
            code_lines.append(line)
            continue

        if not line.strip():
            flush_paragraph()
            close_list()
            continue

        image = re.match(r"^!\[([^\]]*)\]\(([^)]+)\)$", line)
        if image:
            flush_paragraph()
            close_list()
            alt = html.escape(image.group(1), quote=True)
            src = _image_src(image.group(2))
            out.append(f'<figure><img src="{src}" alt="{alt}"><figcaption>{alt}</figcaption></figure>')
            continue

        heading = re.match(r"^(#{1,4})\s+(.+)$", line)
        if heading:
            flush_paragraph()
            close_list()
            level = len(heading.group(1))
            out.append(f"<h{level}>{_inline(heading.group(2))}</h{level}>")
            continue

        bullet = re.match(r"^-\s+(.+)$", line)
        if bullet:
            flush_paragraph()
            if list_type != "ul":
                close_list()
                list_type = "ul"
                out.append("<ul>")
            out.append(f"<li>{_inline(bullet.group(1))}</li>")
            continue

        ordered = re.match(r"^\d+\.\s+(.+)$", line)
        if ordered:
            flush_paragraph()
            if list_type != "ol":
                close_list()
                list_type = "ol"
                out.append("<ol>")
            out.append(f"<li>{_inline(ordered.group(1))}</li>")
            continue

        paragraph.append(line.strip())

    flush_paragraph()
    close_list()
    if in_code:
        out.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
    return "\n".join(out)


def build_html(body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>TikTok Surprise Set Streamer Guide</title>
  <style>
    @page {{ size: Letter; margin: 0.58in 0.62in; }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, Arial, Helvetica, sans-serif;
      color: #171717;
      background: #ffffff;
      font-size: 10.4pt;
      line-height: 1.43;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 25pt;
      line-height: 1.02;
      color: #111111;
      letter-spacing: 0;
    }}
    h2 {{
      margin: 21px 0 7px;
      padding-top: 9px;
      border-top: 1px solid #ded7ce;
      font-size: 13.2pt;
      line-height: 1.14;
      color: #2f1f15;
    }}
    h3 {{
      margin: 15px 0 6px;
      font-size: 11.4pt;
      color: #222222;
    }}
    p {{ margin: 0 0 7px; }}
    ul, ol {{ margin: 4px 0 10px 18px; padding: 0; }}
    li {{ margin: 2px 0; padding-left: 2px; }}
    pre {{
      margin: 8px 0 12px;
      padding: 9px 11px;
      border: 1px solid #d8cec3;
      border-radius: 7px;
      background: #f7f3ef;
      color: #1f1a17;
      white-space: pre-wrap;
      font-size: 9.3pt;
      line-height: 1.38;
    }}
    code {{ font-family: Consolas, Menlo, monospace; }}
    figure {{
      margin: 12px 0 16px;
      padding: 8px;
      border: 1px solid #ded7ce;
      border-radius: 10px;
      background: #fffaf4;
      break-inside: avoid;
    }}
    figure img {{
      display: block;
      width: 100%;
      max-height: 4.2in;
      object-fit: contain;
    }}
    figcaption {{
      margin-top: 5px;
      color: #6b625a;
      font-size: 8.8pt;
      text-align: center;
    }}
    h1 + p {{
      color: #6b625a;
      font-size: 10pt;
      margin-bottom: 3px;
    }}
    h1 + p + p {{
      color: #6b625a;
      font-size: 10pt;
      margin-bottom: 12px;
    }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def _launch_chromium(playwright):
    chromium = playwright.chromium
    for kwargs in ({"channel": "chrome"}, {"channel": "msedge"}, {}):
        try:
            return chromium.launch(headless=True, **kwargs)
        except Exception:
            continue
    raise RuntimeError("Could not launch Chromium, Chrome, or Edge through Playwright.")


def main() -> int:
    if not SOURCE.exists():
        print(f"Missing source: {SOURCE}", file=sys.stderr)
        return 1

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print(f"Playwright is required to build the PDF: {exc}", file=sys.stderr)
        return 1

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SOURCE, STATIC_MD_OUT)

    html_text = build_html(markdown_to_html(SOURCE.read_text(encoding="utf-8")))
    with sync_playwright() as playwright:
        browser = _launch_chromium(playwright)
        page = browser.new_page(viewport={"width": 816, "height": 1056})
        page.set_content(html_text, wait_until="load")
        page.pdf(
            path=str(PDF_OUT),
            format="Letter",
            print_background=True,
            margin={"top": "0.58in", "right": "0.62in", "bottom": "0.58in", "left": "0.62in"},
        )
        browser.close()

    print(PDF_OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
