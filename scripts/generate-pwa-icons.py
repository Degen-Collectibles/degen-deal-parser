"""Generate PWA icons and favicon from the Degen master logo.

Reads the RGBA master at ``app/static/degen-logo.png`` (the red "DEGEN"
wordmark with a real alpha channel — no baked-in background) and writes:

  app/static/icons/icon-192.png            (regular, opaque black bg)
  app/static/icons/icon-512.png            (regular, opaque black bg)
  app/static/icons/icon-maskable-192.png   (wordmark in inner 80% safe zone)
  app/static/icons/icon-maskable-512.png   (wordmark in inner 80% safe zone)
  app/static/icons/apple-touch-icon-180.png
  app/static/favicon.ico

Design notes:
  * Background is pure black (#000) with a faint red radial glow behind the
    wordmark so the tile looks alive against a dark home screen instead of a
    flat red-on-white rectangle.
  * Uses the RGBA master so we don't bake a white JPG background into the
    icon. Alpha channel is respected when compositing on black.
  * Apple Touch Icons do not support transparency — iOS will fill any alpha
    with white — so we intentionally emit opaque tiles with a real black
    background.

Re-run:

    python scripts/generate-pwa-icons.py

Idempotent; overwrites targets.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter

# Pure black tile. We considered #0a0a0a to match manifest.background_color
# but true black reads crisper on iOS home screens and against other icons.
BG_COLOR = (0, 0, 0)

# Degen red (#C8102E) — used for the soft glow behind the wordmark.
GLOW_COLOR = (200, 16, 46)

REGULAR_FILL_RATIO = 0.86   # wordmark fills 86% of the square for regular icons
MASKABLE_FILL_RATIO = 0.68  # keep wordmark comfortably inside the 80% safe zone


def _trim_alpha(src: Image.Image) -> Image.Image:
    """Crop the RGBA master to the tight bbox of its non-transparent pixels."""
    if src.mode != "RGBA":
        return src
    bbox = src.getbbox()
    if bbox:
        return src.crop(bbox)
    return src


def _make_glow(size: int) -> Image.Image:
    """Paint a soft red radial glow on a black canvas."""
    canvas = Image.new("RGB", (size, size), BG_COLOR)
    glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(glow)

    # Elliptical glow, wider than tall, centered slightly above vertical center
    # so the wordmark feels grounded.
    cx, cy = size // 2, int(size * 0.52)
    rx, ry = int(size * 0.55), int(size * 0.30)
    draw.ellipse(
        [cx - rx, cy - ry, cx + rx, cy + ry],
        fill=(*GLOW_COLOR, 110),  # ~43% alpha
    )
    glow = glow.filter(ImageFilter.GaussianBlur(radius=size * 0.10))

    canvas.paste(glow, (0, 0), glow)
    return canvas


def _fit_on_square(src: Image.Image, size: int, fill_ratio: float) -> Image.Image:
    """Composite the RGBA wordmark on a glowing black square."""
    canvas = _make_glow(size)

    target_w = int(size * fill_ratio)
    scale = target_w / src.width
    target_h = max(1, int(src.height * scale))
    resized = src.resize((target_w, target_h), Image.LANCZOS)

    x = (size - target_w) // 2
    y = (size - target_h) // 2

    # If RGBA, use alpha as mask so black shows through transparent pixels.
    if resized.mode == "RGBA":
        canvas.paste(resized, (x, y), resized)
    else:
        canvas.paste(resized, (x, y))
    return canvas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--master",
        default=str(Path(__file__).resolve().parent.parent / "app" / "static" / "degen-logo.png"),
        help="Path to the master logo image (RGBA PNG preferred)",
    )
    parser.add_argument(
        "--static-dir",
        default=str(Path(__file__).resolve().parent.parent / "app" / "static"),
        help="Path to app/static/",
    )
    args = parser.parse_args()

    master_path = Path(args.master)
    static_dir = Path(args.static_dir)
    icons_dir = static_dir / "icons"
    icons_dir.mkdir(parents=True, exist_ok=True)

    src = Image.open(master_path)
    if src.mode != "RGBA":
        src = src.convert("RGBA")
    src = _trim_alpha(src)

    targets = [
        (icons_dir / "icon-192.png", 192, REGULAR_FILL_RATIO),
        (icons_dir / "icon-512.png", 512, REGULAR_FILL_RATIO),
        (icons_dir / "icon-maskable-192.png", 192, MASKABLE_FILL_RATIO),
        (icons_dir / "icon-maskable-512.png", 512, MASKABLE_FILL_RATIO),
        (icons_dir / "apple-touch-icon-180.png", 180, REGULAR_FILL_RATIO),
    ]

    for out_path, size, ratio in targets:
        img = _fit_on_square(src, size, ratio)
        img.save(out_path, "PNG", optimize=True)
        print(f"wrote {out_path} ({size}x{size})")

    favicon_path = static_dir / "favicon.ico"
    favicon_img = _fit_on_square(src, 64, REGULAR_FILL_RATIO)
    favicon_img.save(favicon_path, sizes=[(16, 16), (32, 32), (48, 48), (64, 64)])
    print(f"wrote {favicon_path}")


if __name__ == "__main__":
    main()
