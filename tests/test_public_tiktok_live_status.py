import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import app.routers.tiktok_streamer as streamer_module


class PublicTikTokLiveStatusTests(unittest.TestCase):
    def _payload(self) -> dict:
        response = streamer_module.public_tiktok_live_status()
        self.assertEqual(response.headers["access-control-allow-origin"], "*")
        self.assertEqual(response.headers["cache-control"], "no-store, max-age=0")
        return json.loads(response.body)

    def test_public_status_reports_each_channel_without_internal_metrics(self) -> None:
        checked_at = datetime(2026, 5, 25, 18, 30, tzinfo=timezone.utc)
        sessions = [
            {
                "ok": True,
                "id": "main-live",
                "title": "Fresh slab drops",
                "username": "degencollectibles",
                "start_time": 1779730200,
                "end_time": 0,
                "gmv": 9999.99,
                "sku_orders": 42,
            },
            {
                "ok": True,
                "id": "boss-ended",
                "title": "Behind the counter",
                "username": "degenboss0",
                "start_time": 1,
                "end_time": 2,
                "gmv": 123.45,
            },
        ]

        with patch.object(streamer_module, "_get_live_sessions_list", return_value=sessions), patch.object(
            streamer_module,
            "_get_live_sessions_list_checked_at",
            return_value=checked_at,
        ), patch.object(streamer_module, "_get_live_session_snapshot", return_value={}):
            payload = self._payload()

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["source"], "ops-tiktok-live-session-cache")
        self.assertEqual(payload["updatedAt"], "2026-05-25T18:30:00+00:00")
        self.assertEqual(payload["staleAfterSeconds"], 900)
        self.assertEqual([channel["handle"] for channel in payload["channels"]], ["@degencollectibles", "@degenboss0"])
        self.assertTrue(payload["channels"][0]["isLive"])
        self.assertFalse(payload["channels"][1]["isLive"])
        self.assertEqual(payload["channels"][0]["title"], "Fresh slab drops")

        serialized = json.dumps(payload)
        for internal_field in ("gmv", "sku_orders", "orders", "live_room_id", "vip_buyer_threshold"):
            self.assertNotIn(internal_field, serialized)

    def test_public_status_is_unknown_until_ops_has_checked_tiktok(self) -> None:
        with patch.object(streamer_module, "_get_live_sessions_list", return_value=[]), patch.object(
            streamer_module,
            "_get_live_sessions_list_checked_at",
            return_value=None,
        ), patch.object(streamer_module, "_get_live_session_snapshot", return_value={}):
            payload = self._payload()

        self.assertFalse(payload["ok"])
        self.assertIsNone(payload["updatedAt"])
        self.assertFalse(any(channel["isLive"] for channel in payload["channels"]))


if __name__ == "__main__":
    unittest.main()
