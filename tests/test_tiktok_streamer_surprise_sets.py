import json
import shutil
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import app.routers.tiktok_streamer as streamer_module
from app.models import TikTokOrder
from sqlmodel import Session, SQLModel, create_engine


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

    def test_values_floors_and_renames_set_to_highest_value_chases(self) -> None:
        start = datetime(2026, 5, 31, 12, 0, tzinfo=timezone.utc)
        orders = [
            _order(
                "bank-floor",
                start,
                [{"product_name": "BAGN EX !!!!", "sku_type": "UNKNOWN", "sale_price": 1, "quantity": 20}],
                subtotal_price=20,
            ),
            _order(
                "pack-floor",
                start + timedelta(minutes=1),
                [{"product_name": "GEM 5 BOOSTER PACK", "sku_type": "UNKNOWN", "sale_price": 2, "quantity": 8}],
                subtotal_price=16,
            ),
            _order(
                "chase-1",
                start + timedelta(minutes=2),
                [{"product_name": "Charizard ex PSA 9 ENG", "sku_type": "UNKNOWN", "sale_price": 12, "quantity": 1}],
                subtotal_price=12,
            ),
            _order(
                "chase-2",
                start + timedelta(minutes=3),
                [{"product_name": "Venusaur BGS 10", "sku_type": "UNKNOWN", "sale_price": 10, "quantity": 1}],
                subtotal_price=10,
            ),
        ]

        def fake_price_lookup(titles: list[str]) -> dict[str, dict]:
            return {
                "Charizard ex PSA 9 ENG": {
                    "status": "priced",
                    "market_price": 40.0,
                    "matched_title": "Charizard ex",
                    "source": "tcgtracking",
                    "confidence": "medium",
                    "query": "charizard ex psa 9 eng",
                },
                "Venusaur BGS 10": {
                    "status": "priced",
                    "market_price": 120.0,
                    "matched_title": "Venusaur",
                    "source": "tcgtracking",
                    "confidence": "medium",
                    "query": "venusaur bgs 10",
                },
            }

        summaries = streamer_module._build_surprise_set_summaries(
            orders,
            include_valuation=True,
            price_lookup=fake_price_lookup,
        )

        valuation = summaries[0]["valuation"]
        self.assertEqual(summaries[0]["name"], "Venusaur BGS 10 / Charizard ex PSA 9 ENG")
        self.assertEqual(summaries[0]["name_source"], "highest_value_chases")
        self.assertEqual(valuation["floor_value"], 18.00)
        self.assertEqual(valuation["chase_value"], 160.00)
        self.assertEqual(valuation["game_value"], 178.00)
        self.assertEqual(summaries[0]["estimated_spread"], -120.00)
        self.assertEqual([row["rule"] for row in valuation["floors"]], ["pack_floor", "bank_floor"])
        self.assertEqual([row["title"] for row in valuation["chases"]], ["Venusaur BGS 10", "Charizard ex PSA 9 ENG"])

    def test_missing_chase_prices_are_flagged_for_review(self) -> None:
        start = datetime(2026, 5, 31, 13, 0, tzinfo=timezone.utc)
        orders = [
            _order(
                "missing-chase",
                start,
                [{"product_name": "Mystery Typo Chase", "sku_type": "UNKNOWN", "sale_price": 7, "quantity": 1}],
                subtotal_price=7,
            ),
        ]

        summaries = streamer_module._build_surprise_set_summaries(
            orders,
            include_valuation=True,
            price_lookup=lambda titles: {},
        )

        valuation = summaries[0]["valuation"]
        self.assertEqual(valuation["game_value"], 0.0)
        self.assertEqual(valuation["unpriced_chase_count"], 1)
        self.assertEqual(valuation["confidence"], "partial")
        self.assertEqual(valuation["chases"][0]["status"], "missing")


class StreamerFreshOrderFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_streamer_surprise_sets" / str(uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            f"sqlite:///{(self.temp_dir / 'streamer.db').as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

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

    def test_fallback_start_extends_to_current_continuous_order_activity(self) -> None:
        now = datetime(2026, 5, 31, 17, 0, tzinfo=timezone.utc)
        current_run_start = now - timedelta(minutes=50)
        stale_prior_run = now - timedelta(minutes=95)
        stream_context = {
            "selected_creator": streamer_module.DEFAULT_STREAM_CREATOR,
            "creator_filter_enabled": True,
            "live_session": {},
            "sessions": [],
        }

        with Session(self.engine) as session:
            for idx, created_at in enumerate(
                [
                    stale_prior_run,
                    current_run_start,
                    now - timedelta(minutes=20),
                    now - timedelta(minutes=2),
                ],
                start=1,
            ):
                session.add(
                    _order(
                        f"activity-{idx}",
                        created_at,
                        [{"product_name": "BANG EX !!!", "sku_type": "UNKNOWN", "sale_price": 5, "quantity": 1}],
                        subtotal_price=5,
                    )
                )
            session.commit()

            recent_cutoff = streamer_module._recent_order_activity_fallback_start(stream_context, now=now)
            fallback_start = streamer_module._infer_order_activity_fallback_start(
                session,
                stream_context,
                recent_cutoff,
                now=now,
            )

        self.assertEqual(recent_cutoff, now - timedelta(minutes=streamer_module.RECENT_ORDER_ACTIVITY_FALLBACK_MINUTES))
        self.assertEqual(fallback_start, current_run_start)

    def test_fresh_order_fallback_uses_persisted_stream_start_when_available(self) -> None:
        now = datetime(2026, 5, 31, 18, 45, tzinfo=timezone.utc)
        persisted_start = datetime(2026, 5, 27, 19, 1, 13, tzinfo=timezone.utc)
        previous_range = dict(streamer_module._stream_range)
        try:
            streamer_module._stream_range["start"] = persisted_start
            streamer_module._stream_range["end"] = datetime(2026, 5, 30, 9, 29, 48, tzinfo=timezone.utc)
            stream_context = {
                "selected_creator": streamer_module.DEFAULT_STREAM_CREATOR,
                "creator_filter_enabled": True,
                "live_session": {},
                "sessions": [],
                "is_live": False,
            }

            with Session(self.engine) as session:
                session.add(
                    _order(
                        "recent-activity",
                        now - timedelta(minutes=5),
                        [{"product_name": "BANG EX !!!", "sku_type": "UNKNOWN", "sale_price": 93, "quantity": 1}],
                        subtotal_price=93,
                    )
                )
                session.commit()

                fallback_context = streamer_module._apply_order_activity_fallback(
                    session,
                    stream_context,
                    now=now,
                )

            self.assertEqual(fallback_context["start"], persisted_start)
            self.assertIsNone(fallback_context["end"])
        finally:
            streamer_module._stream_range.clear()
            streamer_module._stream_range.update(previous_range)


if __name__ == "__main__":
    unittest.main()
