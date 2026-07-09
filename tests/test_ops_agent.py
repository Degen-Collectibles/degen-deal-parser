import json
from datetime import datetime, timedelta, timezone
from time import perf_counter

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.discord.message_revisions import ensure_message_revision
from app.discord.transactions import sync_transaction_from_message
from app.models import (
    BankFeedAccount,
    BankStatementImport,
    BankTransaction,
    DiscordMessage,
    InventoryItem,
    ShopifyOrder,
    TikTokOrder,
    Transaction,
    PARSE_PARSED,
)
from app.ops_agent import (
    MAX_TARGET_PAYBACK_WEEKS,
    _build_channel_velocity,
    _target_payback_weeks,
    build_ops_agent_context,
    build_ops_agent_recommendation,
)


def make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def add_bank_import(session: Session) -> BankStatementImport:
    row = BankStatementImport(
        label="Chase feed",
        account_label="Chase Checking",
        account_type="checking",
        source_kind="plaid",
        provider="plaid",
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _add_velocity_transaction(
    session: Session,
    *,
    message_id: int,
    category: str,
    occurred_at: datetime,
) -> tuple[DiscordMessage, Transaction]:
    row = DiscordMessage(
        id=message_id,
        discord_message_id=f"ops-velocity-reportability-{message_id}",
        channel_id="shows",
        channel_name="Shows",
        author_name="tester",
        content=f"sold {category} 170 zelle",
        created_at=occurred_at,
        parse_status=PARSE_PARSED,
        deal_type="sale",
        entry_kind="sale",
        payment_method="zelle",
        category=category,
        amount=170.0,
        money_in=170.0,
        money_out=0.0,
        expense_category="sales",
        needs_review=False,
    )
    session.add(row)
    session.flush()
    transaction = sync_transaction_from_message(session, row)
    assert transaction is not None
    return row, transaction


def _add_legacy_source_backed_velocity_transaction(
    session: Session,
    transaction: Transaction,
) -> Transaction:
    """Add a valid pre-revision Discord source for an older velocity fixture."""

    transaction.parse_status = transaction.parse_status or PARSE_PARSED
    session.add(
        DiscordMessage(
            id=transaction.source_message_id,
            discord_message_id=f"legacy-ops-source-{transaction.source_message_id}",
            channel_id=transaction.channel_id or "legacy-ops-channel",
            channel_name=transaction.channel_name,
            author_name="tester",
            content=transaction.source_content,
            created_at=transaction.occurred_at,
            parse_status=PARSE_PARSED,
            needs_review=False,
        )
    )
    session.add(transaction)
    return transaction


def add_feed_account(
    session: Session,
    *,
    provider_account_id: str,
    account_type: str,
    current_balance=None,
    available_balance=None,
    updated_at: datetime,
    is_active: bool = True,
    iso_currency_code: str | None = "USD",
) -> BankFeedAccount:
    row = BankFeedAccount(
        connection_id=1,
        provider_account_id=provider_account_id,
        account_label=f"Account {provider_account_id}",
        account_type=account_type,
        current_balance=current_balance,
        available_balance=available_balance,
        iso_currency_code=iso_currency_code,
        is_active=is_active,
        updated_at=updated_at,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def test_ops_agent_marks_high_margin_fast_velocity_buy_safe_with_evidence():
    scenario = {
        "lot_name": "Pokemon sealed weekend lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": 40,
        "cash_on_hand": 12000.0,
        "minimum_cash_reserve": 6000.0,
        "target_payback_weeks": 4,
        "financing_amount": 0.0,
        "categories": ["Pokemon sealed", "ETB"],
    }
    context = {
        "finance_statement": {
            "revenue": 18000.0,
            "operating_profit": 2400.0,
            "operating_expenses": 1200.0,
            "inventory_spend": 6200.0,
            "avg_daily_profit": 240.0,
            "revenue_display": "$18,000",
            "operating_profit_display": "$2,400",
            "operating_expenses_display": "$1,200",
            "inventory_spend_display": "$6,200",
        },
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 18.0,
                "revenue_per_week": 1350.0,
                "avg_price": 75.0,
                "confidence": "high",
                "evidence_url": "/tiktok/analytics/api/products?days=90",
            },
            {
                "channel": "Shopify",
                "matched_category": "Pokemon sealed",
                "units_per_week": 5.0,
                "revenue_per_week": 425.0,
                "avg_price": 85.0,
                "confidence": "medium",
                "evidence_url": "/shopify/orders",
            },
        ],
        "loan_snapshot": {
            "observed_loan_proceeds": 0.0,
            "observed_paybacks": 1200.0,
            "evidence_url": "/bookkeeping/bank?expense_category=loan_owner_payments",
        },
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "safe"
    assert result["sell_through"]["estimated_weeks"] == 2
    assert result["payback_plan"]["weekly_payback"] == 500.0
    assert result["routing"][0]["channel"] == "TikTok"
    assert result["routing"][0]["recommended_units"] > result["routing"][1]["recommended_units"]
    assert result["cash_flow"]["post_buy_cash"] == 10000.0
    assert result["cash_flow"]["reserve_gap"] == 4000.0
    assert result["read_only_guardrails"] == [
        "No money movement",
        "No inventory changes",
        "No production writes",
        "No customer or partner messages without approval",
    ]
    assert any(row["source"] == "finance_statement" for row in result["evidence"])
    assert any(row["source"] == "channel_velocity" and row["url"] for row in result["evidence"])
    assert "Weekly business update" in result["partner_update"]
    assert "safe" in result["partner_update"].lower()


_MISSING = object()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("purchase_cost", _MISSING),
        ("purchase_cost", -1),
        ("expected_revenue", _MISSING),
        ("expected_revenue", 0),
        ("unit_count", _MISSING),
        ("unit_count", 0),
        ("unit_count", True),
        ("unit_count", 1.5),
        ("unit_count", 10**10000),
        ("financing_amount", -1),
        ("cash_on_hand", True),
        ("cash_on_hand", False),
        ("target_payback_weeks", _MISSING),
        ("target_payback_weeks", None),
        ("target_payback_weeks", True),
        ("target_payback_weeks", 1.9),
    ],
    ids=[
        "missing-purchase-cost",
        "negative-purchase-cost",
        "missing-expected-revenue",
        "zero-expected-revenue",
        "missing-unit-count",
        "zero-unit-count",
        "boolean-unit-count",
        "fractional-unit-count",
        "huge-unit-count",
        "negative-financing",
        "true-cash-on-hand",
        "false-cash-on-hand",
        "missing-target-payback-weeks",
        "null-target-payback-weeks",
        "boolean-target-payback-weeks",
        "fractional-target-payback-weeks",
    ],
)
def test_ops_agent_invalid_scenario_fields_are_field_specific_and_never_safe(field, value):
    scenario = {
        "lot_name": "Pokemon sealed weekend lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": 40,
        "cash_on_hand": 12000.0,
        "minimum_cash_reserve": 6000.0,
        "target_payback_weeks": 4,
        "financing_amount": 0.0,
        "categories": ["Pokemon sealed"],
    }
    if value is _MISSING:
        scenario.pop(field)
    else:
        scenario[field] = value
    context = {
        "finance_statement": {"avg_daily_profit": 240.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "confidence": "high",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] != "safe"
    assert any(f"Invalid {field}" in flag for flag in result["risk_flags"])
    if field == "unit_count":
        assert result["unit_economics"]["unit_count"] == 0


@pytest.mark.parametrize("reserve_floor", [True, False], ids=["true", "false"])
def test_ops_agent_boolean_reserve_floor_is_unconfigured_and_never_safe(reserve_floor):
    scenario = {
        "lot_name": "Pokemon sealed weekend lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": 40,
        "cash_on_hand": 12000.0,
        "minimum_cash_reserve": reserve_floor,
        "target_payback_weeks": 4,
        "financing_amount": 0.0,
        "categories": ["Pokemon sealed"],
    }
    context = {
        "finance_statement": {"avg_daily_profit": 240.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "confidence": "high",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] != "safe"
    assert result["cash_flow"]["minimum_cash_reserve"] is None
    assert result["cash_flow"]["reserve_floor_configured"] is False
    assert "Reserve floor is not configured" in result["risk_flags"]


def test_ops_agent_missing_reserve_floor_is_not_assessed_and_cannot_be_safe():
    scenario = {
        "lot_name": "Pokemon sealed weekend lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": 40,
        "cash_on_hand": 12000.0,
        "target_payback_weeks": 4,
        "financing_amount": 0.0,
        "categories": ["Pokemon sealed"],
    }
    context = {
        "finance_statement": {"avg_daily_profit": 240.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "confidence": "high",
                "evidence_url": "/tiktok/analytics/api/products?days=90",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "risky"
    assert result["cash_flow"]["minimum_cash_reserve"] is None
    assert result["cash_flow"]["reserve_gap"] is None
    assert result["cash_flow"]["reserve_floor_configured"] is False
    assert all(week["below_reserve"] is None for week in result["payback_plan"]["weeks"])
    assert result["risk_flags"] == ["Reserve floor is not configured"]
    assert "reserve safety was not assessed" in result["partner_update"].lower()
    assert "reserve gap:" not in result["partner_update"].lower()
    json.dumps(result, allow_nan=False)


def test_ops_agent_explicit_unconfigured_flag_overrides_internal_floor_value():
    scenario = {
        "lot_name": "Pokemon sealed weekend lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": 40,
        "cash_on_hand": 12000.0,
        "minimum_cash_reserve": 6000.0,
        "reserve_floor_configured": False,
        "target_payback_weeks": 4,
        "categories": ["Pokemon sealed"],
    }
    context = {
        "finance_statement": {"avg_daily_profit": 240.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "confidence": "high",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["cash_flow"]["reserve_floor_configured"] is False
    assert result["cash_flow"]["minimum_cash_reserve"] is None
    assert "Reserve floor is not configured" in result["risk_flags"]


def test_ops_agent_extreme_finite_money_inputs_are_bounded_and_json_safe():
    scenario = {
        "lot_name": "Impossible scale lot",
        "purchase_cost": 1e308,
        "expected_revenue": -1e308,
        "unit_count": 1,
        "cash_on_hand": -1e308,
        "minimum_cash_reserve": 6000.0,
        "target_payback_weeks": 1,
        "financing_amount": 1e308,
        "categories": ["Pokemon sealed"],
    }
    context = {
        "finance_statement": {"avg_daily_profit": 1e308},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 1.0,
                "revenue_per_week": 1e308,
                "confidence": "high",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["unit_economics"]["purchase_cost"] == 1_000_000_000.0
    assert result["unit_economics"]["expected_revenue"] == -1_000_000_000.0
    json.dumps(result, allow_nan=False)


def test_ops_agent_numeric_conversion_overflow_is_validation_risk_not_crash():
    scenario = {
        "lot_name": "Hostile numeric input lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": float("inf"),
        "cash_on_hand": 12000.0,
        "minimum_cash_reserve": 6000.0,
        "target_payback_weeks": 4,
        "financing_amount": 10**10000,
        "categories": ["Pokemon sealed"],
    }
    context = {
        "finance_statement": {"avg_daily_profit": 240.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "confidence": "high",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "risky"
    assert "Scenario contains invalid or unsupported numeric inputs" in result["risk_flags"]
    assert result["unit_economics"]["unit_count"] == 0
    assert result["payback_plan"]["financing_amount"] == 0.0
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize(
    "target_weeks",
    [MAX_TARGET_PAYBACK_WEEKS + 1, 10**10000],
    ids=["over_maximum", "huge_integer"],
)
def test_ops_agent_caps_unsupported_payback_horizon_and_marks_it_risky(target_weeks):
    scenario = {
        "lot_name": "Unbounded schedule lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": 40,
        "cash_on_hand": 12000.0,
        "minimum_cash_reserve": 6000.0,
        "target_payback_weeks": target_weeks,
        "financing_amount": 0.0,
        "categories": ["Pokemon sealed"],
    }
    context = {
        "finance_statement": {"avg_daily_profit": 240.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 20.0,
                "revenue_per_week": 1500.0,
                "confidence": "high",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "risky"
    assert result["payback_plan"]["target_weeks"] == MAX_TARGET_PAYBACK_WEEKS
    assert len(result["payback_plan"]["weeks"]) == MAX_TARGET_PAYBACK_WEEKS
    assert (
        f"Target payback window must be between 1 and {MAX_TARGET_PAYBACK_WEEKS} weeks"
        in result["risk_flags"]
    )
    json.dumps(result, allow_nan=False)


def test_ops_agent_infinite_payback_horizon_falls_back_without_crashing():
    scenario = {
        "lot_name": "Overflow schedule lot",
        "purchase_cost": 2000.0,
        "expected_revenue": 3600.0,
        "unit_count": 40,
        "cash_on_hand": 12000.0,
        "minimum_cash_reserve": 6000.0,
        "target_payback_weeks": float("inf"),
        "financing_amount": 0.0,
        "categories": ["Pokemon sealed"],
    }
    context = {
        "finance_statement": {"avg_daily_profit": 240.0},
        "channel_velocity": [],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "risky"
    assert result["payback_plan"]["target_weeks"] == 1
    assert len(result["payback_plan"]["weeks"]) == 1
    assert (
        f"Target payback window must be between 1 and {MAX_TARGET_PAYBACK_WEEKS} weeks"
        in result["risk_flags"]
    )
    json.dumps(result, allow_nan=False)


def test_ops_agent_huge_negative_payback_horizon_rejects_before_integer_materialization():
    started_at = perf_counter()

    normalized = _target_payback_weeks("-1e1000000")

    assert normalized == (1, False)
    assert perf_counter() - started_at < 1.0


def test_ops_agent_single_category_filters_velocity_evidence():
    scenario = {
        "lot_name": "Pokemon sealed lot",
        "purchase_cost": 1000.0,
        "expected_revenue": 1800.0,
        "unit_count": 10,
        "cash_on_hand": 5000.0,
        "minimum_cash_reserve": 1000.0,
        "target_payback_weeks": 2,
        "category": "Pokemon sealed",
    }
    context = {
        "finance_statement": {"avg_daily_profit": 100.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 10.0,
                "revenue_per_week": 900.0,
                "confidence": "high",
                "evidence_url": "/tiktok/analytics",
            },
            {
                "channel": "Shopify",
                "matched_category": "slab",
                "units_per_week": 50.0,
                "revenue_per_week": 10000.0,
                "confidence": "high",
                "evidence_url": "/shopify/orders",
            },
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    channel_evidence = [row for row in result["evidence"] if row["source"] == "channel_velocity"]
    assert result["sell_through"]["units_per_week"] == 10.0
    assert [row["label"] for row in channel_evidence] == ["TikTok sell-through"]
    assert [row["channel"] for row in result["routing"]] == ["TikTok"]


def test_ops_agent_without_category_does_not_use_all_velocity_as_evidence():
    scenario = {
        "lot_name": "Ambiguous lot",
        "purchase_cost": 1000.0,
        "expected_revenue": 1800.0,
        "unit_count": 10,
        "cash_on_hand": 5000.0,
        "minimum_cash_reserve": 1000.0,
        "target_payback_weeks": 2,
    }
    context = {
        "finance_statement": {"avg_daily_profit": 100.0},
        "channel_velocity": [
            {
                "channel": "TikTok",
                "matched_category": "Pokemon sealed",
                "units_per_week": 10.0,
                "revenue_per_week": 900.0,
                "confidence": "high",
                "evidence_url": "/tiktok/analytics",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "risky"
    assert result["sell_through"]["estimated_weeks"] is None
    assert "No matching sell-through evidence found" in result["risk_flags"]
    assert not [row for row in result["evidence"] if row["source"] == "channel_velocity"]


def test_ops_agent_rejects_negative_profit_lot_even_when_cash_is_available():
    scenario = {
        "lot_name": "Low-margin slabs",
        "purchase_cost": 5000.0,
        "expected_revenue": 5200.0,
        "unit_count": 25,
        "cash_on_hand": 20000.0,
        "minimum_cash_reserve": 8000.0,
        "target_payback_weeks": 6,
        "financing_amount": 0.0,
        "categories": ["slab"],
    }
    context = {
        "finance_statement": {
            "revenue": 30000.0,
            "operating_profit": 1000.0,
            "operating_expenses": 5000.0,
            "inventory_spend": 9000.0,
            "avg_daily_profit": 75.0,
        },
        "channel_velocity": [
            {
                "channel": "Discord",
                "matched_category": "slab",
                "units_per_week": 4.0,
                "revenue_per_week": 800.0,
                "avg_price": 200.0,
                "confidence": "medium",
                "evidence_url": "/reports?source=discord",
            }
        ],
        "loan_snapshot": {},
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "not worth doing"
    assert "Expected gross profit is too thin" in result["risk_flags"]
    assert result["unit_economics"]["expected_profit"] == 200.0
    assert result["unit_economics"]["expected_margin_pct"] == 3.8


def test_ops_agent_flags_cash_reserve_and_slow_sell_through_risk():
    scenario = {
        "lot_name": "Mixed bulk buy",
        "purchase_cost": 9000.0,
        "expected_revenue": 14000.0,
        "unit_count": 120,
        "cash_on_hand": 11000.0,
        "minimum_cash_reserve": 6000.0,
        "target_payback_weeks": 6,
        "financing_amount": 4000.0,
        "categories": ["bulk"],
    }
    context = {
        "finance_statement": {
            "revenue": 15000.0,
            "operating_profit": 500.0,
            "operating_expenses": 4500.0,
            "inventory_spend": 11000.0,
            "avg_daily_profit": 40.0,
        },
        "channel_velocity": [
            {
                "channel": "Shows",
                "matched_category": "bulk",
                "units_per_week": 6.0,
                "revenue_per_week": 650.0,
                "avg_price": 108.33,
                "confidence": "low",
                "evidence_url": "/reports",
            }
        ],
        "loan_snapshot": {
            "observed_loan_proceeds": 12000.0,
            "observed_paybacks": 3000.0,
            "evidence_url": "/bookkeeping/bank?expense_category=loan_owner_payments",
        },
    }

    result = build_ops_agent_recommendation(scenario, context)

    assert result["verdict"] == "risky"
    assert "Post-buy cash falls below the minimum reserve" in result["risk_flags"]
    assert "Sell-through is slower than the target payback window" in result["risk_flags"]
    assert "Financing adds repayment pressure" in result["risk_flags"]
    assert result["sell_through"]["estimated_weeks"] == 20
    assert result["payback_plan"]["weeks"][0]["ending_cash"] < scenario["minimum_cash_reserve"]


def test_ops_agent_cash_snapshot_is_unavailable_when_no_active_depository_accounts_exist():
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        snapshot = build_ops_agent_context(session, now=now, days=30)["cash_snapshot"]

    assert snapshot["available"] is False
    assert snapshot["complete"] is False
    assert snapshot["fresh"] is False
    assert snapshot["as_of"] is None
    assert snapshot["latest_known_cash"] is None
    assert snapshot["accounts"] == []
    assert snapshot["data_gaps"]


@pytest.mark.parametrize(
    ("account_type", "current_balance", "updated_delta", "expected_complete", "expected_fresh"),
    [
        ("checking", None, timedelta(hours=1), False, True),
        ("checking", 1250.0, timedelta(days=8), True, False),
        ("credit_card", 1250.0, timedelta(hours=1), False, False),
    ],
    ids=["missing-balance", "stale-balance", "liability-only"],
)
def test_ops_agent_cash_snapshot_rejects_incomplete_stale_or_liability_only_sources(
    account_type,
    current_balance,
    updated_delta,
    expected_complete,
    expected_fresh,
):
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        add_feed_account(
            session,
            provider_account_id="candidate",
            account_type=account_type,
            current_balance=current_balance,
            updated_at=now - updated_delta,
        )
        snapshot = build_ops_agent_context(session, now=now, days=30)["cash_snapshot"]

    assert snapshot["available"] is False
    assert snapshot["complete"] is expected_complete
    assert snapshot["fresh"] is expected_fresh
    assert snapshot["latest_known_cash"] is None
    assert snapshot["data_gaps"]


def test_ops_agent_cash_snapshot_sums_fresh_active_depository_available_balances():
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        add_feed_account(
            session,
            provider_account_id="checking",
            account_type="checking",
            current_balance=12000.0,
            available_balance=11500.0,
            updated_at=now - timedelta(hours=1),
        )
        add_feed_account(
            session,
            provider_account_id="savings",
            account_type="savings",
            current_balance=2500.0,
            updated_at=now - timedelta(hours=2),
        )
        add_feed_account(
            session,
            provider_account_id="card",
            account_type="credit_card",
            current_balance=900.0,
            updated_at=now - timedelta(hours=1),
        )
        snapshot = build_ops_agent_context(session, now=now, days=30)["cash_snapshot"]

    assert snapshot["available"] is True
    assert snapshot["complete"] is True
    assert snapshot["fresh"] is True
    assert snapshot["latest_known_cash"] == 14000.0
    assert snapshot["as_of"]
    assert {row["account_type"] for row in snapshot["accounts"]} == {"checking", "savings"}
    assert snapshot["excluded_accounts"] == [
        {
            "account_label": "Account card",
            "account_type": "credit_card",
            "reason": "non_depository_account",
        }
    ]


def test_ops_agent_cash_snapshot_rejects_missing_currency_code():
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        add_feed_account(
            session,
            provider_account_id="unknown-currency",
            account_type="checking",
            current_balance=5000.0,
            iso_currency_code=None,
            updated_at=now - timedelta(hours=1),
        )
        snapshot = build_ops_agent_context(session, now=now, days=30)["cash_snapshot"]

    assert snapshot["available"] is False
    assert snapshot["complete"] is False
    assert snapshot["latest_known_cash"] is None
    assert snapshot["accounts"][0]["currency"] is None
    assert any("currency" in gap.lower() for gap in snapshot["data_gaps"])


def test_ops_agent_context_caps_untrusted_history_days():
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        context = build_ops_agent_context(session, now=now, days=10**10000)

    assert context["range"]["days"] == 365
    assert context["range"]["start"] == "2025-06-01"


def test_ops_agent_context_collects_existing_orders_inventory_and_loan_evidence_read_only():
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
    tiktok_items = [{"product_name": "Pokemon 151 ETB", "quantity": 4, "sale_price": 80.0}]
    shopify_items = [{"title": "Pokemon 151 ETB", "quantity": 2, "price": 84.0}]

    with Session(engine) as session:
        bank_import = add_bank_import(session)
        session.add(
            TikTokOrder(
                tiktok_order_id="tt-1",
                order_number="TT1001",
                created_at=now - timedelta(days=7),
                updated_at=now - timedelta(days=7),
                total_price=320.0,
                subtotal_price=320.0,
                financial_status=None,
                order_status="COMPLETED",
                line_items_json=json.dumps(tiktok_items),
            )
        )
        session.add(
            ShopifyOrder(
                shopify_order_id="sh-1",
                order_number="SH1001",
                created_at=now - timedelta(days=6),
                updated_at=now - timedelta(days=6),
                total_price=168.0,
                subtotal_price=168.0,
                subtotal_ex_tax=168.0,
                financial_status="paid",
                line_items_json=json.dumps(shopify_items),
            )
        )
        session.add(
            InventoryItem(
                barcode="DGN-000001",
                item_type="sealed",
                game="Pokemon",
                card_name="Pokemon 151 ETB",
                set_name="151",
                quantity=6,
                cost_basis=52.0,
                list_price=85.0,
                status="in_stock",
            )
        )
        _add_legacy_source_backed_velocity_transaction(
            session,
            Transaction(
                source_message_id=501,
                channel_id="shows",
                channel_name="Shows",
                occurred_at=now - timedelta(days=4),
                entry_kind="sale",
                category="Pokemon 151 ETB",
                amount=170.0,
                money_in=170.0,
                money_out=0.0,
                source_content="show sale Pokemon 151 ETB",
            ),
        )
        session.add(
            BankTransaction(
                import_id=bank_import.id,
                row_index=1,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=now - timedelta(days=5),
                description="AXOS BANK LOAN PROCEEDS",
                amount=5000.0,
                balance=12000.0,
                classification="credit",
                expense_category="loan_proceeds",
            )
        )
        session.add(
            BankTransaction(
                import_id=bank_import.id,
                row_index=2,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=now - timedelta(days=2),
                description="Owner loan payback",
                amount=-750.0,
                balance=11250.0,
                classification="debit",
                expense_category="loan_owner_payments",
            )
        )
        session.add(
            BankFeedAccount(
                connection_id=1,
                bank_import_id=bank_import.id,
                provider_account_id="chase-checking",
                account_label="Chase Checking",
                account_type="checking",
                current_balance=11250.0,
                available_balance=11250.0,
                iso_currency_code="USD",
                is_active=True,
                updated_at=now - timedelta(days=2),
            )
        )
        session.commit()

        before_inventory_count = len(session.exec(select(InventoryItem)).all())
        context = build_ops_agent_context(session, now=now, days=30)
        after_inventory_count = len(session.exec(select(InventoryItem)).all())

    assert before_inventory_count == after_inventory_count
    assert context["finance_statement"]["tiktok_paid_orders"] == 1
    assert context["finance_statement"]["shopify_paid_orders"] == 1
    assert context["loan_snapshot"]["observed_loan_proceeds"] == 5000.0
    assert context["loan_snapshot"]["observed_paybacks"] == 750.0
    assert context["cash_snapshot"]["latest_known_cash"] == 11250.0
    assert context["cash_snapshot"]["available"] is True
    assert context["cash_snapshot"]["accounts"][0]["account_label"] == "Chase Checking"
    assert any(row["channel"] == "TikTok" and row["matched_category"] == "Pokemon 151 ETB" for row in context["channel_velocity"])
    assert any(row["channel"] == "Shopify" and row["matched_category"] == "Pokemon 151 ETB" for row in context["channel_velocity"])
    assert any(row["channel"] == "Shows" and row["matched_category"] == "Pokemon 151 ETB" for row in context["channel_velocity"])
    assert context["inventory_snapshot"]["active_items"] == 1
    assert context["inventory_snapshot"]["estimated_list_value"] == 510.0


def test_channel_velocity_excludes_live_transactions_with_unreportable_discord_sources():
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        valid_row, valid_tx = _add_velocity_transaction(
            session,
            message_id=9600,
            category="Valid Pokemon Sale",
            occurred_at=now - timedelta(days=1),
        )
        del valid_row

        quarantined_row, quarantined_tx = _add_velocity_transaction(
            session,
            message_id=9601,
            category="Quarantined Pokemon Sale",
            occurred_at=now - timedelta(days=1),
        )
        quarantined_row.source_refresh_required = True

        pending_row, pending_tx = _add_velocity_transaction(
            session,
            message_id=9602,
            category="Pending Pokemon Sale",
            occurred_at=now - timedelta(days=1),
        )
        pending_row.parse_status = "pending"

        advanced_row, advanced_tx = _add_velocity_transaction(
            session,
            message_id=9603,
            category="Stale Revision Pokemon Sale",
            occurred_at=now - timedelta(days=1),
        )
        advanced_row.content = f"{advanced_row.content} corrected"
        ensure_message_revision(session, advanced_row)
        session.add(quarantined_row)
        session.add(pending_row)
        session.add(advanced_row)
        session.commit()

        assert all(
            transaction.is_deleted is False
            for transaction in (valid_tx, quarantined_tx, pending_tx, advanced_tx)
        )
        rows = _build_channel_velocity(
            session,
            start=now - timedelta(days=30),
            end=now,
            days=30,
        )

    assert [row["matched_category"] for row in rows] == ["Valid Pokemon Sale"]
    engine.dispose()


def test_ops_agent_loan_snapshot_dedupes_relinked_bank_rows():
    engine = make_engine()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
    posted_at = now - timedelta(days=2)

    with Session(engine) as session:
        first_import = add_bank_import(session)
        second_import = BankStatementImport(
            label="Relinked Chase feed",
            account_label="Chase Checking",
            account_type="checking",
            source_kind="plaid",
            provider="plaid",
        )
        session.add(second_import)
        session.commit()
        session.refresh(second_import)

        for import_id, prefix in ((first_import.id, "old"), (second_import.id, "new")):
            session.add(
                BankTransaction(
                    import_id=import_id,
                    row_index=1,
                    account_label="Chase Checking",
                    account_type="checking",
                    posted_at=posted_at,
                    description="AXOS BANK LOAN PROCEEDS",
                    description_stem="AXOS BANK LOAN PROCEEDS",
                    amount=5000.0,
                    classification="credit",
                    expense_category="loan_proceeds",
                    provider_transaction_id=f"{prefix}_loan",
                )
            )
            session.add(
                BankTransaction(
                    import_id=import_id,
                    row_index=2,
                    account_label="Chase Checking",
                    account_type="checking",
                    posted_at=posted_at,
                    description="Owner loan payback",
                    description_stem="OWNER LOAN PAYBACK",
                    amount=-750.0,
                    classification="debit",
                    expense_category="loan_owner_payments",
                    provider_transaction_id=f"{prefix}_payback",
                )
            )
            session.add(
                BankTransaction(
                    import_id=import_id,
                    row_index=3,
                    account_label="Chase Checking",
                    account_type="checking",
                    posted_at=posted_at,
                    description="SHOPIFY PAYOUT",
                    description_stem="SHOPIFY PAYOUT",
                    amount=1200.0,
                    classification="shopify_payout",
                    expense_category="platform_payouts",
                    provider_transaction_id=f"{prefix}_payout",
                )
            )
        session.commit()

        context = build_ops_agent_context(session, now=now, days=30)

    assert context["loan_snapshot"]["observed_loan_proceeds"] == 5000.0
    assert context["loan_snapshot"]["observed_paybacks"] == 750.0
    assert context["loan_snapshot"]["observed_platform_payouts"] == 1200.0
