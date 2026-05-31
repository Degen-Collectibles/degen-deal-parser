import json
import unittest
from datetime import datetime, timedelta, timezone

import app.routers.tiktok_streamer as streamer_module
from app.models import TikTokOrder


def _order(
    order_id: str,
    created_at: datetime,
    items: list[dict],
    *,
    financial_status: str = "paid",
    order_status: str = "completed",
    subtotal_price: float = 0.0,
) -> TikTokOrder:
    return TikTokOrder(
        tiktok_order_id=order_id,
        order_number=order_id,
        created_at=created_at,
        updated_at=created_at,
        customer_name="Buyer",
        subtotal_price=subtotal_price,
        total_price=subtotal_price,
        financial_status=financial_status,
        order_status=order_status,
        line_items_json=json.dumps(items),
        line_items_summary_json=json.dumps(
            [{"title": item.get("product_name"), "quantity": item.get("quantity", 1)} for item in items]
        ),
    )


class SurpriseSetGmvTests(unittest.TestCase):
    def test_counts_only_paid_unknown_sku_line_items_without_double_counting_summary_items(self) -> None:
        created_at = datetime(2026, 5, 30, 17, 0, tzinfo=timezone.utc)
        orders = [
            _order(
                "paid-mixed",
                created_at,
                [
                    {
                        "product_id": "set-1",
                        "product_name": "DEGEN SATURDAY EVENING!!!",
                        "sku_type": "UNKNOWN",
                        "sale_price": "12.00",
                        "quantity": 2,
                    },
                    {
                        "product_id": "fixed-1",
                        "product_name": "Rip ship pack",
                        "sku_type": "NORMAL",
                        "sale_price": "500.00",
                        "quantity": 1,
                    },
                ],
                subtotal_price=524.00,
            ),
            _order(
                "pending-auction",
                created_at + timedelta(minutes=1),
                [
                    {
                        "product_id": "set-1",
                        "product_name": "DEGEN SATURDAY EVENING!!!",
                        "sku_type": "UNKNOWN",
                        "sale_price": "99.00",
                        "quantity": 1,
                    }
                ],
                financial_status="pending",
                order_status="awaiting_payment",
                subtotal_price=99.00,
            ),
        ]

        summaries = streamer_module._build_surprise_set_summaries(orders)

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["name"], "DEGEN SATURDAY EVENING!!!")
        self.assertEqual(summaries[0]["name_source"], "auction_product_title")
        self.assertEqual(summaries[0]["gmv"], 24.00)
        self.assertEqual(summaries[0]["auction_orders"], 1)
        self.assertEqual(summaries[0]["item_count"], 2)
        self.assertEqual(summaries[0]["top_items"][0]["gmv"], 24.00)

    def test_splits_sets_when_auction_gap_exceeds_threshold(self) -> None:
        start = datetime(2026, 5, 30, 18, 0, tzinfo=timezone.utc)
        orders = [
            _order(
                "first-1",
                start,
                [{"product_name": "DEGEN SATURDAY EVENING!!!", "sku_type": "UNKNOWN", "sale_price": 10, "quantity": 1}],
                subtotal_price=10,
            ),
            _order(
                "first-2",
                start + timedelta(minutes=14),
                [{"product_name": "DEGEN SATURDAY EVENING!!!", "sku_type": "UNKNOWN", "sale_price": 15, "quantity": 1}],
                subtotal_price=15,
            ),
            _order(
                "second-1",
                start + timedelta(minutes=30),
                [{"product_name": "DEGEN LATE NIGHT!!!", "sku_type": "UNKNOWN", "sale_price": 25, "quantity": 1}],
                subtotal_price=25,
            ),
        ]

        summaries = streamer_module._build_surprise_set_summaries(orders, gap_minutes=15)

        self.assertEqual([row["name"] for row in summaries], ["DEGEN SATURDAY EVENING!!!", "DEGEN LATE NIGHT!!!"])
        self.assertEqual([row["gmv"] for row in summaries], [25.00, 25.00])
        self.assertEqual([row["auction_orders"] for row in summaries], [2, 1])

    def test_names_mixed_sets_when_no_title_dominates(self) -> None:
        start = datetime(2026, 5, 30, 19, 0, tzinfo=timezone.utc)
        orders = [
            _order(
                "mixed-1",
                start,
                [{"product_name": "Gem 5 Booster", "sku_type": "UNKNOWN", "sale_price": 10, "quantity": 1}],
                subtotal_price=10,
            ),
            _order(
                "mixed-2",
                start + timedelta(minutes=2),
                [{"product_name": "Nihil Zero", "sku_type": "UNKNOWN", "sale_price": 9, "quantity": 1}],
                subtotal_price=9,
            ),
            _order(
                "mixed-3",
                start + timedelta(minutes=4),
                [{"product_name": "Gem 4", "sku_type": "UNKNOWN", "sale_price": 8, "quantity": 1}],
                subtotal_price=8,
            ),
        ]

        summaries = streamer_module._build_surprise_set_summaries(orders)

        self.assertEqual(summaries[0]["name"], "Mixed: Gem 5 Booster / Nihil Zero / Gem 4")
        self.assertEqual(summaries[0]["name_source"], "mixed_auction_titles")


class StreamerFreshOrderFallbackTests(unittest.TestCase):
    def test_fallback_window_uses_recent_activity_across_pacific_midnight(self) -> None:
        now = datetime(2026, 5, 31, 7, 3, tzinfo=timezone.utc)
        recent_cutoff = now - timedelta(minutes=streamer_module.RECENT_ORDER_ACTIVITY_FALLBACK_MINUTES)
        stream_context = {
            "live_session": {
                "end_time": int((now - timedelta(minutes=30)).timestamp()),
            },
            "sessions": [],
        }

        fallback_start = streamer_module._recent_order_activity_fallback_start(stream_context, now=now)

        self.assertEqual(fallback_start, recent_cutoff)


if __name__ == "__main__":
    unittest.main()
