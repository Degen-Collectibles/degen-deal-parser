import json
import shutil
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import app.routers.tiktok_streamer as streamer_module
from app.models import AppSetting, TikTokOrder
from jinja2 import Environment, FileSystemLoader, select_autoescape
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
                [{"product_name": "GEM 5 BOOSTER PACK", "sku_type": "UNKNOWN", "sale_price": 2, "quantity": 20}],
                subtotal_price=40,
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
        self.assertEqual(valuation["floor_value"], 36.00)
        self.assertEqual(valuation["chase_value"], 160.00)
        self.assertEqual(valuation["game_value"], 196.00)
        self.assertEqual(summaries[0]["estimated_spread"], -114.00)
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

    def test_ten_packs_are_chases_and_etb_query_expands_for_pricing(self) -> None:
        self.assertFalse(streamer_module._surprise_set_is_pack_floor("TWILIGHT MASQUERADE SLEEVED BOOSTER PACK", 10))
        self.assertTrue(streamer_module._surprise_set_is_pack_floor("GEM 5 BOOSTER PACK", 20))
        self.assertFalse(streamer_module._surprise_set_is_pack_floor("Charizard pack fresh single", 25))
        self.assertFalse(streamer_module._surprise_set_is_pack_floor("6-pack tin", 20))
        self.assertEqual(
            streamer_module._canonical_surprise_set_price_query("CAHOS RISING ETB"),
            "chaos rising elite trainer box",
        )
        self.assertEqual(
            streamer_module._canonical_surprise_set_price_query("CAHOS RISING ETB BOX"),
            "chaos rising elite trainer box",
        )
        self.assertEqual(
            streamer_module._canonical_surprise_set_price_query("Lances Charizard V Box"),
            streamer_module._canonical_surprise_set_price_query("Lance's Charizard V Box"),
        )
        self.assertEqual(
            streamer_module._surprise_set_pricing_item_type("Twilight masquerade sleeve pack"),
            streamer_module.ITEM_TYPE_SEALED,
        )

    def test_bank_floor_matches_known_bang_ex_typos_without_loose_fuzzy_matches(self) -> None:
        self.assertTrue(streamer_module._surprise_set_is_bank_floor("BANG EX"))
        self.assertTrue(streamer_module._surprise_set_is_bank_floor("BAGN EX !!!!"))
        self.assertFalse(streamer_module._surprise_set_is_bank_floor("Range EX"))
        self.assertFalse(streamer_module._surprise_set_is_bank_floor("Bag EX"))
        self.assertFalse(streamer_module._surprise_set_is_bank_floor("Bangle"))

    def test_async_price_lookup_runtime_error_is_logged_and_returns_missing(self) -> None:
        with patch.object(streamer_module.asyncio, "run", side_effect=RuntimeError("loop already running")):
            with self.assertLogs("app.routers.tiktok_streamer", level="WARNING") as logs:
                prices = streamer_module._lookup_surprise_set_chase_prices(["Mystery Chase"])

        self.assertEqual(prices["Mystery Chase"]["status"], "missing")
        self.assertIn("surprise set chase price lookup failed", "\n".join(logs.output).lower())

    def test_manual_chase_price_overrides_market_lookup(self) -> None:
        start = datetime(2026, 5, 31, 14, 0, tzinfo=timezone.utc)
        orders = [
            _order(
                "ten-pack-chase",
                start,
                [
                    {
                        "product_name": "TWILIGHT MASQUERADE SLEEVED BOOSTER PACK",
                        "sku_type": "UNKNOWN",
                        "sale_price": 8,
                        "quantity": 10,
                    }
                ],
                subtotal_price=80,
            ),
        ]
        title = "TWILIGHT MASQUERADE SLEEVED BOOSTER PACK"
        manual_key = streamer_module._surprise_set_manual_price_key(title)

        summaries = streamer_module._build_surprise_set_summaries(
            orders,
            include_valuation=True,
            manual_prices={
                manual_key: {
                    "unit_value": 7.25,
                    "title": title,
                    "updated_by": "Jeffrey",
                }
            },
            price_lookup=lambda titles: {
                title: {
                    "status": "priced",
                    "market_price": 99.0,
                    "matched_title": title,
                    "source": "tcgtracking",
                    "confidence": "high",
                }
            },
        )

        chase = summaries[0]["valuation"]["chases"][0]
        self.assertEqual(summaries[0]["valuation"]["floor_value"], 0.0)
        self.assertEqual(chase["unit_value"], 7.25)
        self.assertEqual(chase["total_value"], 72.50)
        self.assertEqual(chase["source"], "manual")
        self.assertTrue(chase["manual_override"])

    def test_price_editor_sets_merge_current_chases_and_saved_manual_prices(self) -> None:
        current_title = "TWILIGHT MASQUERADE SLEEVED BOOSTER PACK"
        saved_title = "CAHOS RISING ETB"
        current_key = streamer_module._surprise_set_manual_price_key(current_title)
        saved_key = streamer_module._surprise_set_manual_price_key(saved_title)

        sets = streamer_module._build_surprise_set_price_editor_sets(
            [
                {
                    "id": "set-1",
                    "name": "Twilight set",
                    "gmv": 80.0,
                    "start_label": "12:00 PM",
                    "end_label": "12:12 PM",
                    "valuation": {
                        "floor_value": 0.0,
                        "chases": [
                            {
                                "title": current_title,
                                "qty": 10,
                                "unit_value": None,
                                "total_value": 0.0,
                                "source": "tcgtracking",
                                "status": "missing",
                                "confidence": "missing",
                                "query": "twilight masquerade sleeved booster pack",
                                "price_key": current_key,
                            }
                        ]
                    },
                }
            ],
            {
                current_key: {"title": current_title, "unit_value": 7.25, "query": "twilight masquerade sleeved booster pack"},
                saved_key: {"title": saved_title, "unit_value": 42.50, "query": "chaos rising elite trainer box"},
            },
        )

        self.assertEqual([row["name"] for row in sets], ["Twilight set", "Saved manual prices"])
        chase = sets[0]["valuation"]["chases"][0]
        self.assertEqual(chase["title"], current_title)
        self.assertEqual(chase["unit_value"], 7.25)
        self.assertEqual(chase["total_value"], 72.50)
        self.assertTrue(chase["manual_override"])
        self.assertEqual(chase["seen_in_current_sets"], True)
        self.assertEqual(sets[0]["game_value"], 72.50)
        saved_chase = sets[1]["valuation"]["chases"][0]
        self.assertEqual(saved_chase["title"], saved_title)
        self.assertEqual(saved_chase["query"], "chaos rising elite trainer box")
        self.assertEqual(saved_chase["seen_in_current_sets"], False)
        self.assertEqual(saved_chase["source"], "manual")

    def test_price_editor_route_is_registered(self) -> None:
        paths = {route.path for route in streamer_module.router.routes}

        self.assertIn("/tiktok/streamer/surprise-set-prices", paths)

    def test_price_editor_prefills_current_price_and_clear_posts_explicit_action(self) -> None:
        env = Environment(
            loader=FileSystemLoader(Path.cwd() / "app" / "templates"),
            autoescape=select_autoescape(),
        )
        html = env.get_template("tiktok_surprise_set_prices.html").render(
            request=None,
            title="Surprise Set Prices",
            sets=[
                {
                    "name": "Twilight set",
                    "gmv": 80.0,
                    "game_value": 123.40,
                    "estimated_spread": -43.40,
                    "valuation": {
                        "floors": [],
                        "chases": [
                            {
                                "key": "twilightmasqueradesleevedboosterpack",
                                "title": "TWILIGHT MASQUERADE SLEEVED BOOSTER PACK",
                                "query": "twilight masquerade sleeved booster pack",
                                "qty": 10,
                                "unit_value": 12.34,
                                "total_value": 123.40,
                                "source": "tcgtracking",
                                "confidence": "high",
                                "manual_override": False,
                                "seen_in_current_sets": True,
                            }
                        ],
                    },
                }
            ],
            chase_count=1,
            surprise_sets_scope_label="Selected stream",
            selected_creator_label="Main",
            selected_stream_label="",
            surprise_sets_total_gmv=80.0,
            streamer_url="/tiktok/streamer",
            csrf_token="test",
        )

        self.assertIn('value="12.34"', html)
        self.assertIn("clear: clear", html)

    def test_streamer_dashboard_links_to_editor_instead_of_inline_price_inputs(self) -> None:
        source = (Path.cwd() / "app" / "templates" / "tiktok_streamer.html").read_text()

        self.assertIn("surprise_set_price_editor_url", source)
        self.assertNotIn("surprise-set-price-input", source)


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

    def _fake_request(self):
        return SimpleNamespace(
            state=SimpleNamespace(
                current_user=SimpleNamespace(display_name="Jeffrey", username="jeffrey")
            )
        )

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

    def test_manual_surprise_set_prices_round_trip_through_app_settings(self) -> None:
        with Session(self.engine) as session:
            saved = streamer_module._save_surprise_set_manual_price(
                session,
                "CAHOS RISING ETB",
                42.499,
                updated_by="Jeffrey",
            )

            loaded = streamer_module._load_surprise_set_manual_prices(session)
            row = loaded[saved["key"]]

            self.assertEqual(saved["unit_value"], 42.50)
            self.assertEqual(row["unit_value"], 42.50)
            self.assertEqual(row["query"], "chaos rising elite trainer box")
            self.assertEqual(session.get(AppSetting, streamer_module.SURPRISE_SET_MANUAL_PRICE_SETTING_KEY) is not None, True)

            streamer_module._delete_surprise_set_manual_price(session, "CAHOS RISING ETB")

            self.assertEqual(streamer_module._load_surprise_set_manual_prices(session), {})

    def test_empty_manual_price_save_keeps_existing_price(self) -> None:
        with Session(self.engine) as session:
            saved = streamer_module._save_surprise_set_manual_price(
                session,
                "CAHOS RISING ETB",
                42.50,
                updated_by="Jeffrey",
            )

            with patch.object(streamer_module, "require_role_response", return_value=None):
                response = streamer_module.set_surprise_set_manual_price(
                    self._fake_request(),
                    session,
                    {"title": "CAHOS RISING ETB", "unit_price": ""},
                )

            loaded = streamer_module._load_surprise_set_manual_prices(session)
            self.assertEqual(response["price"]["unit_value"], 42.50)
            self.assertEqual(loaded[saved["key"]]["unit_value"], 42.50)

    def test_manual_price_clear_action_deletes_existing_price(self) -> None:
        with Session(self.engine) as session:
            saved = streamer_module._save_surprise_set_manual_price(
                session,
                "CAHOS RISING ETB",
                42.50,
                updated_by="Jeffrey",
            )

            with patch.object(streamer_module, "require_role_response", return_value=None):
                response = streamer_module.set_surprise_set_manual_price(
                    self._fake_request(),
                    session,
                    {"title": "CAHOS RISING ETB", "unit_price": "", "clear": True},
                )

            self.assertIsNone(response["price"])
            self.assertNotIn(saved["key"], streamer_module._load_surprise_set_manual_prices(session))

    def test_manual_price_write_requires_reviewer_role(self) -> None:
        denial = {"blocked": True}
        request = self._fake_request()
        with Session(self.engine) as session:
            with patch.object(streamer_module, "require_role_response", return_value=denial) as require_role:
                response = streamer_module.set_surprise_set_manual_price(
                    request,
                    session,
                    {"title": "CAHOS RISING ETB", "unit_price": "42.50"},
                )

        self.assertIs(response, denial)
        require_role.assert_called_once_with(request, "reviewer")


if __name__ == "__main__":
    unittest.main()
