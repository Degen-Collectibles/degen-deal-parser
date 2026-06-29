import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import app.cache as cache_module
import app.routers.reports as reports_module
from fastapi import HTTPException
from app.cache import cache_get, cache_invalidate, cache_set


class _StopAfterCacheFill(Exception):
    pass


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

    def test_reports_route_does_not_read_or_populate_process_local_cache(self):
        request = SimpleNamespace(
            state=SimpleNamespace(current_user=SimpleNamespace(role="viewer"))
        )
        discord_summary = {
            "totals": {"money_in": 0.0, "money_out": 0.0, "net": 0.0},
            "expense_categories": {},
            "channel_net": {},
        }
        shopify_summary = {
            "gross_revenue": 0.0,
            "total_tax": 0.0,
            "net_revenue": 0.0,
        }
        tiktok_summary = {
            "gross_revenue": 0.0,
            "total_tax": 0.0,
            "net_revenue": 0.0,
            "daily_totals": [],
        }

        with patch.object(
            reports_module, "require_role_response", return_value=None
        ), patch.object(
            reports_module, "cache_get", return_value=None
        ) as cache_get_mock, patch.object(
            reports_module, "cache_set"
        ) as cache_set_mock, patch.object(
            reports_module, "cache_set_if_generation", create=True
        ) as guarded_set_mock, patch.object(
            reports_module,
            "get_transactions",
            side_effect=[[], _StopAfterCacheFill()],
        ), patch.object(
            reports_module,
            "build_transaction_summary",
            return_value=discord_summary,
        ), patch.object(
            reports_module, "get_shopify_reporting_rows", return_value=[]
        ), patch.object(
            reports_module,
            "build_shopify_reporting_summary",
            return_value=shopify_summary,
        ), patch.object(
            reports_module, "get_tiktok_reporting_rows", return_value=[]
        ), patch.object(
            reports_module,
            "build_tiktok_reporting_summary",
            return_value=tiktok_summary,
        ), patch.object(
            reports_module, "build_shopify_daily_totals", return_value=[]
        ), patch.object(
            reports_module, "build_reporting_periods", return_value=[]
        ), patch.object(
            reports_module, "build_report_period_comparison_rows", return_value=[]
        ):
            with self.assertRaises(_StopAfterCacheFill):
                reports_module.reports_page(
                    request,
                    start=None,
                    end=None,
                    channel_id=None,
                    entry_kind=None,
                    source=reports_module.REPORT_SOURCE_ALL,
                    session=object(),
                )

        cache_get_mock.assert_not_called()
        cache_set_mock.assert_not_called()
        guarded_set_mock.assert_not_called()

    def test_finance_route_does_not_read_or_populate_process_local_cache(self):
        request = SimpleNamespace(
            state=SimpleNamespace(current_user=SimpleNamespace(role="viewer"))
        )
        range_data = {
            "start_dt": None,
            "end_dt": None,
            "previous_start_dt": None,
            "previous_end_dt": None,
            "day_count": 1,
        }
        snapshot = {
            "statement": {},
            "transactions": [],
            "bank_expense_data": {},
            "daily_rows": [],
        }

        with patch.object(
            reports_module, "require_role_response", return_value=None
        ), patch.object(
            reports_module, "resolve_finance_range", return_value=range_data
        ), patch.object(
            reports_module, "normalize_bank_account_filter", return_value="all"
        ), patch.object(
            reports_module, "cache_get", return_value=None
        ) as cache_get_mock, patch.object(
            reports_module, "cache_set"
        ) as cache_set_mock, patch.object(
            reports_module, "cache_set_if_generation", create=True
        ) as guarded_set_mock, patch.object(
            reports_module,
            "build_finance_range_snapshot",
            side_effect=[snapshot, snapshot],
        ), patch.object(
            reports_module,
            "build_finance_source_mix_rows",
            side_effect=_StopAfterCacheFill(),
        ):
            with self.assertRaises(_StopAfterCacheFill):
                reports_module.finance_page(
                    request,
                    start=None,
                    end=None,
                    window=reports_module.FINANCE_WINDOW_MTD,
                    bank_account="all",
                    session=object(),
                )

        cache_get_mock.assert_not_called()
        cache_set_mock.assert_not_called()
        guarded_set_mock.assert_not_called()

    def test_finance_route_rejects_oversized_range_before_snapshot_queries(self):
        request = SimpleNamespace(
            state=SimpleNamespace(current_user=SimpleNamespace(role="viewer"))
        )

        with patch.object(
            reports_module, "require_role_response", return_value=None
        ), patch.object(
            reports_module, "build_finance_range_snapshot"
        ) as build_snapshot:
            with self.assertRaises(HTTPException) as exc_info:
                reports_module.finance_page(
                    request,
                    start="1900-01-01",
                    end="2026-01-01",
                    window="custom",
                    bank_account="all",
                    session=object(),
                )

        self.assertEqual(exc_info.exception.status_code, 400)
        self.assertIn("366 days", str(exc_info.exception.detail))
        build_snapshot.assert_not_called()


if __name__ == "__main__":
    unittest.main()
