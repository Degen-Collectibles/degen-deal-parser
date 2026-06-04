import json
from datetime import datetime, timedelta, timezone

from sqlmodel import Session, SQLModel, create_engine, select

from app.models import BankStatementImport, BankTransaction, InventoryItem, ShopifyOrder, TikTokOrder, Transaction
from app.ops_agent import build_ops_agent_context, build_ops_agent_recommendation


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
                financial_status="paid",
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
        session.add(
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
            )
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
    assert context["cash_snapshot"]["accounts"][0]["account_label"] == "Chase Checking"
    assert any(row["channel"] == "TikTok" and row["matched_category"] == "Pokemon 151 ETB" for row in context["channel_velocity"])
    assert any(row["channel"] == "Shopify" and row["matched_category"] == "Pokemon 151 ETB" for row in context["channel_velocity"])
    assert any(row["channel"] == "Shows" and row["matched_category"] == "Pokemon 151 ETB" for row in context["channel_velocity"])
    assert context["inventory_snapshot"]["active_items"] == 1
    assert context["inventory_snapshot"]["estimated_list_value"] == 510.0


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
