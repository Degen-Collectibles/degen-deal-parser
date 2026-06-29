import base64
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

from app.main import attachment_asset, attachment_thumbnail
from app.models import AttachmentAsset


ACTIVE_SVG = b"""<svg xmlns="http://www.w3.org/2000/svg">
<script>alert(document.domain)</script>
</svg>"""

SAFE_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMB"
    "AQDJ/pLvAAAAAElFTkSuQmCC"
)


def make_request(path: str, *, if_none_match: str | None = None) -> Request:
    headers = []
    if if_none_match is not None:
        headers.append((b"if-none-match", if_none_match.encode("ascii")))
    return Request({"type": "http", "method": "GET", "path": path, "headers": headers})


class AttachmentActiveContentSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_attachment_active_content" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "attachments.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _store_asset(
        self,
        session: Session,
        *,
        filename: str,
        content_type: str,
        data: bytes,
        is_image: bool = True,
    ) -> AttachmentAsset:
        asset = AttachmentAsset(
            message_id=1,
            source_url=f"https://cdn.example.com/{filename}",
            filename=filename,
            content_type=content_type,
            is_image=is_image,
            data=data,
        )
        session.add(asset)
        session.commit()
        session.refresh(asset)
        return asset

    def _route_with_cached_file(self, route, *, session: Session, asset: AttachmentAsset, file_path: Path):
        file_path.write_bytes(asset.data)
        with patch("app.main.require_role_response", return_value=None), patch(
            "app.main.attachment_cache_path",
            return_value=file_path,
        ):
            return route(
                request=make_request(
                    f"/attachments/{asset.id}/thumb" if route is attachment_thumbnail else f"/attachments/{asset.id}"
                ),
                asset_id=asset.id,
                session=session,
            )

    def test_svg_direct_response_is_forced_to_safe_download(self) -> None:
        with Session(self.engine) as session:
            asset = self._store_asset(
                session,
                filename='active"\r\nX-Injected: yes.svg',
                content_type="image/svg+xml",
                data=ACTIVE_SVG,
            )
            response = self._route_with_cached_file(
                attachment_asset,
                session=session,
                asset=asset,
                file_path=self.temp_dir / "active.svg",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.media_type, "application/octet-stream")
        self.assertTrue(response.headers["content-disposition"].startswith("attachment;"))
        self.assertNotIn("\r", response.headers["content-disposition"])
        self.assertNotIn("\n", response.headers["content-disposition"])
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["content-security-policy"], "sandbox; default-src 'none'")

    def test_svg_thumbnail_failure_never_returns_original_active_bytes(self) -> None:
        with Session(self.engine) as session:
            asset = self._store_asset(
                session,
                filename="active.svg",
                content_type="image/svg+xml",
                data=ACTIVE_SVG,
            )
            original_path = self.temp_dir / "active.svg"
            with patch("app.main.generate_thumbnail", return_value=None):
                response = self._route_with_cached_file(
                    attachment_thumbnail,
                    session=session,
                    asset=asset,
                    file_path=original_path,
                )

        self.assertEqual(response.status_code, 415)
        self.assertEqual(response.body, b"")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["content-security-policy"], "sandbox; default-src 'none'")

    def test_safe_png_remains_inline(self) -> None:
        with Session(self.engine) as session:
            asset = self._store_asset(
                session,
                filename="safe.png",
                content_type="image/png",
                data=SAFE_PNG,
            )
            response = self._route_with_cached_file(
                attachment_asset,
                session=session,
                asset=asset,
                file_path=self.temp_dir / "safe.png",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.media_type, "image/png")
        self.assertTrue(response.headers["content-disposition"].startswith("inline;"))
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["content-security-policy"], "sandbox; default-src 'none'")

    def test_safe_png_thumbnail_failure_returns_415_without_original_fallback(self) -> None:
        with Session(self.engine) as session:
            asset = self._store_asset(
                session,
                filename="safe.png",
                content_type="image/png",
                data=SAFE_PNG,
            )
            with patch("app.main.generate_thumbnail", return_value=None):
                response = self._route_with_cached_file(
                    attachment_thumbnail,
                    session=session,
                    asset=asset,
                    file_path=self.temp_dir / "safe.png",
                )

        self.assertEqual(response.status_code, 415)
        self.assertEqual(response.body, b"")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["content-security-policy"], "sandbox; default-src 'none'")

    def test_generated_png_thumbnail_remains_inline_jpeg(self) -> None:
        with Session(self.engine) as session:
            asset = self._store_asset(
                session,
                filename="safe.png",
                content_type="image/png",
                data=SAFE_PNG,
            )
            source_path = self.temp_dir / "safe.png"
            thumbnail_path = self.temp_dir / "safe-thumb.jpg"
            with patch("app.attachment_storage.thumbnail_cache_path", return_value=thumbnail_path):
                response = self._route_with_cached_file(
                    attachment_thumbnail,
                    session=session,
                    asset=asset,
                    file_path=source_path,
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.media_type, "image/jpeg")
        self.assertTrue(thumbnail_path.exists())
        self.assertIsNone(
            response.headers.get("content-disposition"),
            "no Content-Disposition header preserves HTTP's default inline rendering",
        )
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["content-security-policy"], "sandbox; default-src 'none'")

    def test_matching_legacy_etag_cannot_bypass_thumbnail_validation(self) -> None:
        with Session(self.engine) as session:
            asset = self._store_asset(
                session,
                filename="unsafe.png",
                content_type="image/png",
                data=SAFE_PNG,
            )
            source_path = self.temp_dir / "unsafe.png"
            source_path.write_bytes(asset.data)
            with patch("app.main.require_role_response", return_value=None), patch(
                "app.main.attachment_cache_path",
                return_value=source_path,
            ), patch("app.main.generate_thumbnail", return_value=None) as generate:
                response = attachment_thumbnail(
                    request=make_request(
                        f"/attachments/{asset.id}/thumb",
                        if_none_match=f'"thumb-{asset.id}"',
                    ),
                    asset_id=asset.id,
                    session=session,
                )

        self.assertEqual(response.status_code, 415)
        self.assertEqual(response.body, b"")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["content-security-policy"], "sandbox; default-src 'none'")
        generate.assert_called_once_with(source_path, asset.id)


if __name__ == "__main__":
    unittest.main()
