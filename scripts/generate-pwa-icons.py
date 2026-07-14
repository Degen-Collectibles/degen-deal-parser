"""Generate every managed Degen brand derivative from the approved wordmark.

The source is app/static/degen-logo.png. It must be the character-free Degen
Collectibles wordmark. The script writes PWA icons, employee/team icons, the
print-label derivative, and the favicon. It never downloads artwork.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter


BG_COLOR = (0, 0, 0)
GLOW_COLOR = (200, 16, 46)
REGULAR_FILL_RATIO = 0.86
MASKABLE_FILL_RATIO = 0.68
LABEL_MAX_WIDTH = 900


def _trim_alpha(src: Image.Image) -> Image.Image:
    """Crop an RGBA source to the non-transparent bounding box."""
    if src.mode != "RGBA":
        return src
    bbox = src.getbbox()
    return src.crop(bbox) if bbox else src


def _make_glow(size: int) -> Image.Image:
    """Paint a soft red radial glow on an opaque black square."""
    canvas = Image.new("RGB", (size, size), BG_COLOR)
    glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(glow)
    center_x, center_y = size // 2, int(size * 0.52)
    radius_x, radius_y = int(size * 0.55), int(size * 0.30)
    draw.ellipse(
        [
            center_x - radius_x,
            center_y - radius_y,
            center_x + radius_x,
            center_y + radius_y,
        ],
        fill=(*GLOW_COLOR, 110),
    )
    glow = glow.filter(ImageFilter.GaussianBlur(radius=size * 0.10))
    canvas.paste(glow, (0, 0), glow)
    return canvas


def _fit_on_square(src: Image.Image, size: int, fill_ratio: float) -> Image.Image:
    """Composite the approved wordmark on a glowing black square."""
    canvas = _make_glow(size)
    target_width = int(size * fill_ratio)
    scale = target_width / src.width
    target_height = max(1, int(src.height * scale))
    resized = src.resize((target_width, target_height), Image.Resampling.LANCZOS)
    position = ((size - target_width) // 2, (size - target_height) // 2)
    canvas.paste(resized, position, resized)
    return canvas


def _write_label(src: Image.Image, output_path: Path, max_width: int = LABEL_MAX_WIDTH) -> None:
    """Write an aspect-preserving RGBA derivative for physical labels."""
    scale = min(1.0, max_width / src.width)
    size = (
        max(1, round(src.width * scale)),
        max(1, round(src.height * scale)),
    )
    resized = src.resize(size, Image.Resampling.LANCZOS)
    quantized = resized.convert(
        "P",
        palette=Image.Palette.ADAPTIVE,
        colors=256,
    )
    quantized.save(
        output_path,
        "PNG",
        optimize=True,
    )
    print(f"wrote {output_path} ({size[0]}x{size[1]})")


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--master",
        default=str(root / "app" / "static" / "degen-logo.png"),
        help="Path to the approved character-free RGBA PNG",
    )
    parser.add_argument(
        "--static-dir",
        default=str(root / "app" / "static"),
        help="Path to app/static",
    )
    args = parser.parse_args()

    master_path = Path(args.master)
    static_dir = Path(args.static_dir)
    icons_dir = static_dir / "icons"
    icons_dir.mkdir(parents=True, exist_ok=True)

    with Image.open(master_path) as image:
        source = _trim_alpha(image.convert("RGBA"))

    targets = (
        ("icon-192.png", 192, REGULAR_FILL_RATIO),
        ("icon-512.png", 512, REGULAR_FILL_RATIO),
        ("icon-maskable-192.png", 192, MASKABLE_FILL_RATIO),
        ("icon-maskable-512.png", 512, MASKABLE_FILL_RATIO),
        ("apple-touch-icon-180.png", 180, REGULAR_FILL_RATIO),
        ("degen-collectibles-180.png", 180, REGULAR_FILL_RATIO),
        ("degen-collectibles-192.png", 192, REGULAR_FILL_RATIO),
        ("degen-collectibles-512.png", 512, REGULAR_FILL_RATIO),
    )
    for filename, size, fill_ratio in targets:
        output_path = icons_dir / filename
        icon = _fit_on_square(source, size, fill_ratio)
        icon.save(output_path, "PNG", optimize=True)
        print(f"wrote {output_path} ({size}x{size})")

    _write_label(source, static_dir / "degen-logo-label.png")
    favicon = _fit_on_square(source, 64, REGULAR_FILL_RATIO)
    favicon.save(
        static_dir / "favicon.ico",
        sizes=[(16, 16), (32, 32), (48, 48), (64, 64)],
    )
    print(f"wrote {static_dir / 'favicon.ico'}")


if __name__ == "__main__":
    main()
