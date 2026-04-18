"""Generate PWA icons and favicon from the Degen master logo.

Reads the master JPG (red "DEGEN" wordmark on black) and writes:
  app/static/icons/icon-192.png            (regular)
  app/static/icons/icon-512.png            (regular)
  app/static/icons/icon-maskable-192.png   (wordmark in inner 80% safe zone)
  app/static/icons/icon-maskable-512.png   (wordmark in inner 80% safe zone)
  app/static/icons/apple-touch-icon-180.png
  app/static/favicon.ico

Re-run:
    python scripts/generate-pwa-icons.py

By default it reads /tmp/degen-deal-parser-uploads/degen-logo-master.jpg; pass
--master PATH to override. The script is idempotent and will overwrite targets.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image

BG_COLOR = (10, 10, 10)  # #0a0a0a — matches manifest background_color

REGULAR_FILL_RATIO = 0.90   # wordmark fills 90% of the square for regular icons
MASKABLE_FILL_RATIO = 0.72  # keep wordmark comfortably inside the 80% safe zone


def _fit_on_square(src: Image.Image, size: int, fill_ratio: float) -> Image.Image:
    canvas = Image.new("RGB", (size, size), BG_COLOR)
    target_w = int(size * fill_ratio)
    scale = target_w / src.width
    target_h = max(1, int(src.height * scale))
    resized = src.resize((target_w, target_h), Image.LANCZOS)
    x = (size - target_w) // 2
    y = (size - target_h) // 2
    canvas.paste(resized, (x, y))
    return canvas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--master",
        default="/tmp/degen-deal-parser-uploads/degen-logo-master.jpg",
        help="Path to the master logo image",
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

    src = Image.open(master_path).convert("RGB")

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
