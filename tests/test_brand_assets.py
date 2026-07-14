from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import TestCase

from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "app" / "static"
APPROVED_MASTER_SHA256 = (
    "2CA3065BC1F45CA1EE0B5196322725A5B405FAD9534D71C2F6D3BE6232782561"
)
RETIRED_SHA256 = {
    "F0420343AE82811DB997D2A17B2EC020F75A450BB913FCD21F87070567EE4EBB",
    "A8923E4B08C71AACFABE80791A16C31755852B2856E3550404A370C21A124280",
    "A05D89FC2057333A1019CE04E91F289D5A1712B1861A629046B19CAFB1264B34",
    "87A20C72B9A6175BCD058FDBAC3E042323CA7B1749468561F01013A1D563A312",
    "870CCCA8E05D2A89DBE40BEA1C96159BBB7943B2ADE3C2C239C872D13699167C",
    "3608555D121D66EDA534F37DC5822579224CC1D8AF667E131484AAE6D1DD193F",
    "57ECC655DBD3B2F97E77B2565D8D3FCC41526CB3A6B7687AF1FF6ABA594FADA7",
    "6FE8BB962347FFC81C54FB7F9CEDF1D27922A9E09775CF6E3ED1F3D1E01C6BA5",
    "0AC811426780F9AC00F671CC5D1A900F73CD3AB813D5048E1E8CDE9F51092B7B",
    "E91BFC727E2F9224296D29899B9CDABEC6EF86270EAA4EB63F45C8BB9E008562",
    "4A3CAE0C9E5B1B9B279DFC59364F9EE6DB8AEABA48AEB7BB8DFDCC65FCB46425",
}
EXPECTED_SQUARE_SIZES = {
    "icons/icon-192.png": (192, 192),
    "icons/icon-512.png": (512, 512),
    "icons/icon-maskable-192.png": (192, 192),
    "icons/icon-maskable-512.png": (512, 512),
    "icons/apple-touch-icon-180.png": (180, 180),
    "icons/degen-collectibles-180.png": (180, 180),
    "icons/degen-collectibles-192.png": (192, 192),
    "icons/degen-collectibles-512.png": (512, 512),
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return [
        ROOT / item.decode("utf-8")
        for item in result.stdout.split(b"\0")
        if item and (ROOT / item.decode("utf-8")).is_file()
    ]


class BrandAssetComplianceTests(TestCase):
    def test_master_is_the_approved_character_free_wordmark(self) -> None:
        self.assertEqual(sha256(STATIC / "degen-logo.png"), APPROVED_MASTER_SHA256)

    def test_retired_artwork_bytes_are_absent_from_tracked_files(self) -> None:
        matches = [
            str(path.relative_to(ROOT))
            for path in tracked_files()
            if sha256(path) in RETIRED_SHA256
        ]
        self.assertEqual(matches, [])

    def test_managed_square_icons_have_expected_dimensions(self) -> None:
        for relative_path, expected_size in EXPECTED_SQUARE_SIZES.items():
            with self.subTest(path=relative_path):
                with Image.open(STATIC / relative_path) as image:
                    self.assertEqual(image.size, expected_size)
                    self.assertEqual(image.mode, "RGB")

    def test_label_is_a_small_aspect_preserving_derivative(self) -> None:
        with Image.open(STATIC / "degen-logo.png") as master:
            with Image.open(STATIC / "degen-logo-label.png") as label:
                self.assertLessEqual(label.width, 900)
                self.assertAlmostEqual(
                    label.width / label.height,
                    master.width / master.height,
                    places=2,
                )
        self.assertLess(
            (STATIC / "degen-logo-label.png").stat().st_size,
            (STATIC / "degen-logo.png").stat().st_size,
        )

    def test_manifests_reference_only_managed_square_icons(self) -> None:
        expected = {
            "manifest.webmanifest": {
                "/static/icons/icon-192.png",
                "/static/icons/icon-512.png",
                "/static/icons/icon-maskable-192.png",
                "/static/icons/icon-maskable-512.png",
            },
            "team.webmanifest": {
                "/static/icons/degen-collectibles-192.png",
                "/static/icons/degen-collectibles-512.png",
            },
        }
        for manifest_name, expected_sources in expected.items():
            with self.subTest(manifest=manifest_name):
                payload = json.loads((STATIC / manifest_name).read_text(encoding="utf-8"))
                actual_sources = {icon["src"] for icon in payload["icons"]}
                self.assertEqual(actual_sources, expected_sources)

    def test_square_template_slots_use_the_generated_square_icon(self) -> None:
        for relative_path in (
            "app/templates/login.html",
            "app/templates/_linear_sidebar.html",
            "app/templates/_linear_topbar_mobile.html",
        ):
            with self.subTest(template=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn("/static/icons/icon-192.png", content)
                self.assertNotIn("/static/degen-logo.png", content)

    def test_tracked_derivatives_match_fresh_generation_from_approved_master(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            regenerated_static = Path(temp_dir) / "static"
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "generate-pwa-icons.py"),
                    "--master",
                    str(STATIC / "degen-logo.png"),
                    "--static-dir",
                    str(regenerated_static),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )

            for relative_path in EXPECTED_SQUARE_SIZES:
                with self.subTest(derivative=relative_path):
                    with Image.open(STATIC / relative_path) as tracked:
                        with Image.open(regenerated_static / relative_path) as regenerated:
                            self.assertEqual(
                                tracked.size,
                                regenerated.size,
                                f"{relative_path}: size differs",
                            )
                            self.assertEqual(
                                tracked.convert("RGBA").tobytes(),
                                regenerated.convert("RGBA").tobytes(),
                                f"{relative_path}: rendered RGBA pixels differ",
                            )

            label_path = "degen-logo-label.png"
            with Image.open(STATIC / "degen-logo.png") as master:
                scale = min(1.0, 900 / master.width)
                expected_label_size = (
                    max(1, round(master.width * scale)),
                    max(1, round(master.height * scale)),
                )
                master_aspect_ratio = master.width / master.height

            label_images = {
                "tracked": STATIC / label_path,
                "regenerated": regenerated_static / label_path,
            }
            for source, path in label_images.items():
                with self.subTest(derivative=label_path, source=source):
                    with Image.open(path) as label:
                        colors = label.getcolors(maxcolors=256)
                        self.assertEqual(label.mode, "P", f"{source} {label_path}: mode")
                        self.assertIsNotNone(
                            colors,
                            f"{source} {label_path}: exceeds 256 colors",
                        )
                        self.assertLessEqual(
                            len(colors or ()),
                            256,
                            f"{source} {label_path}: exceeds 256 colors",
                        )
                        self.assertIn(
                            "transparency",
                            label.info,
                            f"{source} {label_path}: transparency is missing",
                        )
                        self.assertEqual(
                            label.size,
                            expected_label_size,
                            f"{source} {label_path}: size differs",
                        )
                        self.assertAlmostEqual(
                            label.width / label.height,
                            master_aspect_ratio,
                            places=2,
                            msg=f"{source} {label_path}: aspect ratio differs",
                        )

            with Image.open(STATIC / label_path) as tracked_label:
                with Image.open(regenerated_static / label_path) as regenerated_label:
                    self.assertEqual(
                        tracked_label.convert("RGBA").tobytes(),
                        regenerated_label.convert("RGBA").tobytes(),
                        f"{label_path}: rendered RGBA pixels differ",
                    )

            favicon_path = "favicon.ico"
            expected_favicon_sizes = {
                (16, 16),
                (32, 32),
                (48, 48),
                (64, 64),
            }
            with Image.open(STATIC / favicon_path) as tracked_favicon:
                with Image.open(regenerated_static / favicon_path) as regenerated_favicon:
                    self.assertEqual(
                        tracked_favicon.ico.sizes(),
                        expected_favicon_sizes,
                        f"tracked {favicon_path}: frame set differs",
                    )
                    self.assertEqual(
                        regenerated_favicon.ico.sizes(),
                        expected_favicon_sizes,
                        f"regenerated {favicon_path}: frame set differs",
                    )
                    for frame_size in sorted(expected_favicon_sizes):
                        frame_name = f"{favicon_path}[{frame_size[0]}x{frame_size[1]}]"
                        with self.subTest(derivative=frame_name):
                            tracked_frame = tracked_favicon.ico.getimage(frame_size)
                            regenerated_frame = regenerated_favicon.ico.getimage(frame_size)
                            self.assertEqual(
                                tracked_frame.convert("RGBA").tobytes(),
                                regenerated_frame.convert("RGBA").tobytes(),
                                f"{frame_name}: rendered RGBA pixels differ",
                            )
