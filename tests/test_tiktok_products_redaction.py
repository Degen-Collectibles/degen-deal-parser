import json
import unittest
from unittest.mock import patch

import httpx

from app.routers import tiktok_products


class TikTokProductSyncRedactionTests(unittest.TestCase):
    def tearDown(self) -> None:
        with tiktok_products._tiktok_product_sync_lock:
            tiktok_products._tiktok_product_sync_state.update(
                {"is_running": False, "last_finished_at": None, "last_error": None}
            )

    def test_product_sync_failure_state_and_log_redact_shop_cipher(self) -> None:
        def boom(*args, **kwargs):
            raise RuntimeError(
                "403 for https://open-api.tiktokglobalshop.com/product?shop_cipher=cipher-product-secret"
            )

        with patch.object(tiktok_products, "pull_tiktok_products", side_effect=boom), patch.object(
            tiktok_products, "managed_session"
        ) as managed_session, patch.object(
            tiktok_products, "ensure_tiktok_auth_row", return_value=object()
        ), patch.object(
            tiktok_products,
            "_resolve_tiktok_pull_credentials",
            return_value=("", "cipher-product-secret", "tok"),
        ), patch.object(
            tiktok_products, "resolve_tiktok_shop_pull_base_url", return_value="https://example/"
        ), patch.object(
            tiktok_products.settings, "tiktok_app_key", "key", create=True
        ), patch.object(
            tiktok_products.settings, "tiktok_app_secret", "secret", create=True
        ), patch.object(tiktok_products, "print") as fake_print:
            session = managed_session.return_value.__enter__.return_value
            session.commit.return_value = None
            tiktok_products.run_tiktok_product_sync_background(limit=1, trigger="manual")

        state = tiktok_products._read_tiktok_product_sync_state()
        state_text = json.dumps(state, default=str)
        printed_text = "\n".join(str(call) for call in fake_print.call_args_list)
        self.assertIn("403", str(state.get("last_error") or ""))
        self.assertNotIn("cipher-product-secret", state_text)
        self.assertNotIn("cipher-product-secret", printed_text)
        self.assertIn("[REDACTED]", state_text)
        self.assertIn("[REDACTED]", printed_text)

    def test_upload_image_failure_response_redacts_tiktok_url_credentials(self) -> None:
        async def run_case():
            class FakeUpload:
                filename = "image.jpg"

                async def read(self):
                    return b"image-bytes"

            req = httpx.Request(
                "POST",
                "https://open-api.tiktokglobalshop.com/product?access_token=tok-product-secret&shop_cipher=cipher-product-secret",
            )
            resp = httpx.Response(401, request=req)
            exc = httpx.HTTPStatusError(
                "Client error '401 Unauthorized' for url 'https://open-api.tiktokglobalshop.com/product?access_token=tok-product-secret&shop_cipher=cipher-product-secret'",
                request=req,
                response=resp,
            )
            with patch.object(tiktok_products, "require_role_response", return_value=None), patch.object(
                tiktok_products, "_upload_tiktok_product_image", side_effect=exc
            ), patch.object(
                tiktok_products, "_get_tiktok_api_client_context", return_value={"access_token": "tok-product-secret"}
            ):
                return await tiktok_products.tiktok_products_upload_image(object(), FakeUpload(), object())  # type: ignore[arg-type]

        response = __import__("asyncio").run(run_case())
        body = response.body.decode("utf-8")
        self.assertNotIn("tok-product-secret", body)
        self.assertNotIn("cipher-product-secret", body)
        self.assertIn("[REDACTED]", body)


if __name__ == "__main__":
    unittest.main()
