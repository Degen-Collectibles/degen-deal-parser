from __future__ import annotations

import hashlib
import json
import subprocess
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
