import time
import unittest
from unittest.mock import patch

import app.cache as cache_module
from app.cache import cache_get, cache_invalidate, cache_set


class CacheTests(unittest.TestCase):
    def setUp(self):
        cache_module._cache.clear()

    def tearDown(self):
        cache_module._cache.clear()

    def test_miss_returns_none(self):
        self.assertIsNone(cache_get("missing_key"))

    def test_hit_returns_value(self):
        cache_set("k", {"data": 42})
        result = cache_get("k")
        self.assertEqual(result, {"data": 42})

    def test_hit_returns_zero_falsy_value(self):
        cache_set("k", 0)
        self.assertEqual(cache_get("k"), 0)

    def test_hit_returns_empty_list(self):
        cache_set("k", [])
        self.assertEqual(cache_get("k"), [])

    def test_expires_after_ttl(self):
        cache_set("k", "value")
        with patch("app.cache.time") as mock_time:
            mock_time.monotonic.return_value = time.monotonic() + 120.0
            result = cache_get("k", ttl=60.0)
        self.assertIsNone(result)

    def test_not_expired_before_ttl(self):
        cache_set("k", "value")
        with patch("app.cache.time") as mock_time:
            mock_time.monotonic.return_value = time.monotonic() + 30.0
            result = cache_get("k", ttl=60.0)
        self.assertEqual(result, "value")

    def test_invalidate_by_prefix_removes_matching(self):
        cache_set("reports:2024-01::", "r1")
        cache_set("reports:2024-02::", "r2")
        cache_set("finance:2024-01:", "f1")
        cache_invalidate("reports:")
        self.assertIsNone(cache_get("reports:2024-01::"))
        self.assertIsNone(cache_get("reports:2024-02::"))
        self.assertIsNotNone(cache_get("finance:2024-01:"))

    def test_invalidate_nonexistent_prefix_is_noop(self):
        cache_set("k", "v")
        cache_invalidate("other:")
        self.assertEqual(cache_get("k"), "v")

    def test_overwrite_updates_value(self):
        cache_set("k", "first")
        cache_set("k", "second")
        self.assertEqual(cache_get("k"), "second")

    def test_custom_ttl_short_expires(self):
        cache_set("k", "v")
        with patch("app.cache.time") as mock_time:
            mock_time.monotonic.return_value = time.monotonic() + 5.0
            self.assertIsNone(cache_get("k", ttl=3.0))

    def test_custom_ttl_long_survives(self):
        cache_set("k", "v")
        with patch("app.cache.time") as mock_time:
            mock_time.monotonic.return_value = time.monotonic() + 5.0
            self.assertEqual(cache_get("k", ttl=10.0), "v")


if __name__ == "__main__":
    unittest.main()
