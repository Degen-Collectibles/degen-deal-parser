from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any

from sqlmodel import Session, func, select

from .discord.bank_reconciliation import dedupe_bank_rows_for_reporting
from .models import BankTransaction, InventoryItem, ShopifyOrder, TikTokOrder, Transaction
from .shared import build_finance_range_snapshot


READ_ONLY_GUARDRAILS = [
    "No money movement",
    "No inventory changes",
    "No production writes",
    "No customer or partner messages without approval",
]


def _money(value: Any) -> float:
    try:
        return round(float(value or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _positive_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        return default
    return max(parsed, 0)


def _percent(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return round((numerator / denominator) * 100.0, 1)


def _format_money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.0f}"


def _build_unit_economics(scenario: dict[str, Any]) -> dict[str, float]:
    purchase_cost = _money(scenario.get("purchase_cost"))
    expected_revenue = _money(scenario.get("expected_revenue"))
    expected_profit = round(expected_revenue - purchase_cost, 2)
    unit_count = _positive_int(scenario.get("unit_count"))
    return {
        "purchase_cost": purchase_cost,
        "expected_revenue": expected_revenue,
        "expected_profit": expected_profit,
        "expected_margin_pct": _percent(expected_profit, expected_revenue),
        "roi_pct": _percent(expected_profit, purchase_cost),
        "unit_count": unit_count,
        "avg_cost_per_unit": round(purchase_cost / unit_count, 2) if unit_count else 0.0,
        "avg_revenue_per_unit": round(expected_revenue / unit_count, 2) if unit_count else 0.0,
    }


def _matching_velocity_rows(scenario: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    categories = {
        str(category).strip().lower()
        for category in scenario.get("categories", [])
        if str(category).strip()
    }
    rows = []
    for row in context.get("channel_velocity", []) or []:
        if not isinstance(row, dict):
            continue
        matched_category = str(row.get("matched_category") or "").strip().lower()
        if categories and matched_category and matched_category not in categories:
            continue
        rows.append(
            {
                "channel": str(row.get("channel") or "Unknown"),
                "matched_category": row.get("matched_category") or "",
                "units_per_week": _money(row.get("units_per_week")),
                "revenue_per_week": _money(row.get("revenue_per_week")),
                "avg_price": _money(row.get("avg_price")),
                "confidence": str(row.get("confidence") or "low"),
                "evidence_url": str(row.get("evidence_url") or ""),
            }
        )
    return rows


def _build_sell_through(scenario: dict[str, Any], velocity_rows: list[dict[str, Any]]) -> dict[str, Any]:
    unit_count = _positive_int(scenario.get("unit_count"))
    total_units_per_week = round(sum(_money(row.get("units_per_week")) for row in velocity_rows), 2)
    estimated_weeks = ceil(unit_count / total_units_per_week) if unit_count and total_units_per_week > 0 else None
    confidence_order = {"high": 3, "medium": 2, "low": 1}
    confidence_score = max(
        (confidence_order.get(str(row.get("confidence") or "low").lower(), 1) for row in velocity_rows),
        default=1,
    )
    confidence = {3: "high", 2: "medium", 1: "low"}[confidence_score]
    return {
        "unit_count": unit_count,
        "units_per_week": total_units_per_week,
        "estimated_weeks": estimated_weeks,
        "confidence": confidence if velocity_rows else "low",
    }


def _build_routing(scenario: dict[str, Any], velocity_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unit_count = _positive_int(scenario.get("unit_count"))
    total_units = sum(_money(row.get("units_per_week")) for row in velocity_rows)
    if not velocity_rows:
        return [
            {
                "channel": "Manual review",
                "recommended_units": unit_count,
                "reason": "No matching channel velocity evidence found.",
                "evidence_url": "",
            }
        ]

    routing = []
    assigned = 0
    sorted_rows = sorted(
        velocity_rows,
        key=lambda row: (_money(row.get("units_per_week")), _money(row.get("revenue_per_week"))),
        reverse=True,
    )
    for index, row in enumerate(sorted_rows):
        if index == len(sorted_rows) - 1:
            recommended_units = max(unit_count - assigned, 0)
        else:
            share = _money(row.get("units_per_week")) / total_units if total_units else 0
            recommended_units = int(round(unit_count * share))
            assigned += recommended_units
        routing.append(
            {
                "channel": row["channel"],
                "recommended_units": recommended_units,
                "reason": (
                    f"{row['channel']} has {row['units_per_week']} matched units/week "
                    f"and {row['confidence']} confidence."
                ),
                "evidence_url": row.get("evidence_url", ""),
            }
        )
    return routing


def _build_cash_flow(scenario: dict[str, Any]) -> dict[str, float]:
    cash_on_hand = _money(scenario.get("cash_on_hand"))
    purchase_cost = _money(scenario.get("purchase_cost"))
    minimum_cash_reserve = _money(scenario.get("minimum_cash_reserve"))
    post_buy_cash = round(cash_on_hand - purchase_cost, 2)
    reserve_gap = round(post_buy_cash - minimum_cash_reserve, 2)
    return {
        "cash_on_hand": cash_on_hand,
        "purchase_cost": purchase_cost,
        "post_buy_cash": post_buy_cash,
        "minimum_cash_reserve": minimum_cash_reserve,
        "reserve_gap": reserve_gap,
    }


def _build_payback_plan(
    scenario: dict[str, Any],
    context: dict[str, Any],
    cash_flow: dict[str, float],
) -> dict[str, Any]:
    target_weeks = max(_positive_int(scenario.get("target_payback_weeks"), default=1), 1)
    financing_amount = _money(scenario.get("financing_amount"))
    purchase_cost = _money(scenario.get("purchase_cost"))
    weekly_payback_base = financing_amount if financing_amount > 0 else purchase_cost
    weekly_payback = round(weekly_payback_base / target_weeks, 2)
    avg_daily_profit = _money((context.get("finance_statement") or {}).get("avg_daily_profit"))
    weekly_profit = round(avg_daily_profit * 7, 2)

    ending_cash = cash_flow["post_buy_cash"]
    weeks = []
    for week in range(1, target_weeks + 1):
        ending_cash = round(ending_cash + weekly_profit - weekly_payback, 2)
        weeks.append(
            {
                "week": week,
                "planned_payback": weekly_payback,
                "estimated_operating_profit": weekly_profit,
                "ending_cash": ending_cash,
                "below_reserve": ending_cash < cash_flow["minimum_cash_reserve"],
            }
        )

    return {
        "target_weeks": target_weeks,
        "weekly_payback": weekly_payback,
        "financing_amount": financing_amount,
        "model_note": "Planning model only; no money movement is performed.",
        "weeks": weeks,
    }


def _build_risk_flags(
    scenario: dict[str, Any],
    unit_economics: dict[str, float],
    sell_through: dict[str, Any],
    cash_flow: dict[str, float],
) -> list[str]:
    flags: list[str] = []
    if unit_economics["expected_margin_pct"] < 8.0:
        flags.append("Expected gross profit is too thin")
    if cash_flow["reserve_gap"] < 0:
        flags.append("Post-buy cash falls below the minimum reserve")
    estimated_weeks = sell_through.get("estimated_weeks")
    target_weeks = max(_positive_int(scenario.get("target_payback_weeks"), default=1), 1)
    if estimated_weeks is None:
        flags.append("No matching sell-through evidence found")
    elif estimated_weeks > target_weeks:
        flags.append("Sell-through is slower than the target payback window")
    if _money(scenario.get("financing_amount")) > 0:
        flags.append("Financing adds repayment pressure")
    return flags


def _choose_verdict(unit_economics: dict[str, float], risk_flags: list[str]) -> str:
    if unit_economics["expected_margin_pct"] < 8.0 or unit_economics["expected_profit"] <= 0:
        return "not worth doing"
    if risk_flags:
        return "risky"
    return "safe"


def _build_evidence(
    context: dict[str, Any],
    velocity_rows: list[dict[str, Any]],
    loan_snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    statement = context.get("finance_statement") or {}
    evidence = [
        {
            "source": "finance_statement",
            "label": "Current finance posture",
            "detail": (
                f"Revenue {statement.get('revenue_display') or _format_money(_money(statement.get('revenue')))}, "
                f"operating profit {statement.get('operating_profit_display') or _format_money(_money(statement.get('operating_profit')))}, "
                f"inventory deployed {statement.get('inventory_spend_display') or _format_money(_money(statement.get('inventory_spend')))}."
            ),
            "url": "/finance",
        }
    ]
    for row in velocity_rows:
        evidence.append(
            {
                "source": "channel_velocity",
                "label": f"{row['channel']} sell-through",
                "detail": (
                    f"{row['units_per_week']} units/week, "
                    f"{_format_money(_money(row['revenue_per_week']))}/week, "
                    f"{row['confidence']} confidence."
                ),
                "url": row.get("evidence_url", ""),
            }
        )
    if loan_snapshot:
        evidence.append(
            {
                "source": "loan_snapshot",
                "label": "Loan/payback context",
                "detail": (
                    f"Observed loan proceeds {_format_money(_money(loan_snapshot.get('observed_loan_proceeds')))}; "
                    f"observed paybacks {_format_money(_money(loan_snapshot.get('observed_paybacks')))}."
                ),
                "url": str(loan_snapshot.get("evidence_url") or "/bookkeeping/bank"),
            }
        )
    return evidence


def _build_partner_update(
    scenario: dict[str, Any],
    verdict: str,
    unit_economics: dict[str, float],
    sell_through: dict[str, Any],
    cash_flow: dict[str, float],
    payback_plan: dict[str, Any],
    risk_flags: list[str],
) -> str:
    lot_name = str(scenario.get("lot_name") or "Proposed lot").strip()
    risks = "; ".join(risk_flags) if risk_flags else "No major cash-flow flags in this model."
    sell_weeks = sell_through.get("estimated_weeks")
    sell_label = f"{sell_weeks} week(s)" if sell_weeks is not None else "unknown"
    return (
        "Weekly business update\n"
        f"Buy decision: {verdict.upper()} for {lot_name}.\n"
        f"Expected profit: {_format_money(unit_economics['expected_profit'])} "
        f"at {unit_economics['expected_margin_pct']}% margin.\n"
        f"Estimated sell-through: {sell_label} using matched channel evidence.\n"
        f"Cash after buy: {_format_money(cash_flow['post_buy_cash'])}; "
        f"reserve gap: {_format_money(cash_flow['reserve_gap'])}.\n"
        f"Weekly payback plan: {_format_money(payback_plan['weekly_payback'])}/week "
        f"for {payback_plan['target_weeks']} week(s).\n"
        f"Risks: {risks}\n"
        "This is read-only decision support; no payments, listings, inventory, or messages were changed."
    )


def build_ops_agent_recommendation(scenario: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    unit_economics = _build_unit_economics(scenario)
    velocity_rows = _matching_velocity_rows(scenario, context)
    sell_through = _build_sell_through(scenario, velocity_rows)
    routing = _build_routing(scenario, velocity_rows)
    cash_flow = _build_cash_flow(scenario)
    payback_plan = _build_payback_plan(scenario, context, cash_flow)
    risk_flags = _build_risk_flags(scenario, unit_economics, sell_through, cash_flow)
    verdict = _choose_verdict(unit_economics, risk_flags)
    loan_snapshot = context.get("loan_snapshot") or {}
    evidence = _build_evidence(context, velocity_rows, loan_snapshot)
    partner_update = _build_partner_update(
        scenario,
        verdict,
        unit_economics,
        sell_through,
        cash_flow,
        payback_plan,
        risk_flags,
    )

    return {
        "verdict": verdict,
        "unit_economics": unit_economics,
        "cash_flow": cash_flow,
        "sell_through": sell_through,
        "routing": routing,
        "payback_plan": payback_plan,
        "risk_flags": risk_flags,
        "evidence": evidence,
        "partner_update": partner_update,
        "read_only_guardrails": READ_ONLY_GUARDRAILS[:],
    }


def _load_line_items(raw_json: str) -> list[dict[str, Any]]:
    try:
        data = json.loads(raw_json or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    return data if isinstance(data, list) else []


def _line_item_title(item: dict[str, Any]) -> str:
    return str(
        item.get("product_name")
        or item.get("title")
        or item.get("name")
        or item.get("sku_name")
        or "Unknown product"
    ).strip()


def _line_item_quantity(item: dict[str, Any]) -> int:
    return max(_positive_int(item.get("quantity"), default=1), 1)


def _line_item_price(item: dict[str, Any]) -> float:
    for key in ("sale_price", "sku_sale_price", "price", "unit_price"):
        value = _money(item.get(key))
        if value:
            return value
    return 0.0


def _add_velocity_item(
    buckets: dict[tuple[str, str], dict[str, Any]],
    *,
    channel: str,
    item: dict[str, Any],
    weeks: float,
    evidence_url: str,
) -> None:
    title = _line_item_title(item)
    qty = _line_item_quantity(item)
    revenue = round(_line_item_price(item) * qty, 2)
    key = (channel, title.lower())
    bucket = buckets.setdefault(
        key,
        {
            "channel": channel,
            "matched_category": title,
            "qty": 0,
            "revenue": 0.0,
            "orders": 0,
            "evidence_url": evidence_url,
        },
    )
    bucket["qty"] = int(bucket["qty"]) + qty
    bucket["revenue"] = round(float(bucket["revenue"]) + revenue, 2)
    bucket["orders"] = int(bucket["orders"]) + 1


def _build_channel_velocity(session: Session, *, start: datetime, end: datetime, days: int) -> list[dict[str, Any]]:
    weeks = max(days / 7.0, 1.0)
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    tiktok_rows = session.exec(
        select(TikTokOrder)
        .where(TikTokOrder.created_at >= start)
        .where(TikTokOrder.created_at <= end)
    ).all()
    for order in tiktok_rows:
        if str(order.financial_status or "").strip().lower() != "paid":
            continue
        for item in _load_line_items(order.line_items_json):
            if isinstance(item, dict):
                _add_velocity_item(
                    buckets,
                    channel="TikTok",
                    item=item,
                    weeks=weeks,
                    evidence_url="/tiktok/analytics/api/products?days=90",
                )

    shopify_rows = session.exec(
        select(ShopifyOrder)
        .where(ShopifyOrder.created_at >= start)
        .where(ShopifyOrder.created_at <= end)
    ).all()
    for order in shopify_rows:
        if str(order.financial_status or "").strip().lower() != "paid":
            continue
        for item in _load_line_items(order.line_items_json):
            if isinstance(item, dict):
                _add_velocity_item(
                    buckets,
                    channel="Shopify",
                    item=item,
                    weeks=weeks,
                    evidence_url="/shopify/orders",
                )

    transaction_rows = session.exec(
        select(Transaction)
        .where(Transaction.is_deleted == False)  # noqa: E712
        .where(Transaction.occurred_at >= start)
        .where(Transaction.occurred_at <= end)
    ).all()
    for row in transaction_rows:
        entry_kind = str(row.entry_kind or "").strip().lower()
        if entry_kind not in {"sale", "trade"}:
            continue
        revenue = _money(row.money_in or row.amount)
        if revenue <= 0:
            continue
        channel = str(row.channel_name or row.channel_id or "Discord").strip() or "Discord"
        matched_category = str(row.category or row.source_content or channel).strip() or channel
        channel_id = str(row.channel_id or "").strip()
        evidence_url = "/reports?source=discord"
        if channel_id:
            evidence_url += f"&channel_id={channel_id}"
        _add_velocity_item(
            buckets,
            channel=channel,
            item={"title": matched_category, "quantity": 1, "price": revenue},
            weeks=weeks,
            evidence_url=evidence_url,
        )

    rows = []
    for bucket in buckets.values():
        qty = int(bucket["qty"])
        revenue = _money(bucket["revenue"])
        rows.append(
            {
                "channel": bucket["channel"],
                "matched_category": bucket["matched_category"],
                "units_per_week": round(qty / weeks, 2),
                "revenue_per_week": round(revenue / weeks, 2),
                "avg_price": round(revenue / qty, 2) if qty else 0.0,
                "confidence": "high" if qty >= 10 else "medium" if qty >= 3 else "low",
                "evidence_url": bucket["evidence_url"],
            }
        )
    rows.sort(key=lambda row: (_money(row["revenue_per_week"]), _money(row["units_per_week"])), reverse=True)
    return rows


def _build_inventory_snapshot(session: Session) -> dict[str, Any]:
    items = session.exec(
        select(InventoryItem).where(InventoryItem.archived_at == None)  # noqa: E711
    ).all()
    active_items = [
        item
        for item in items
        if str(item.status or "").strip().lower() not in {"sold", "archived"}
    ]
    estimated_list_value = 0.0
    cost_basis_total = 0.0
    for item in active_items:
        quantity = max(int(item.quantity or 0), 0)
        estimated_list_value += _money(item.list_price or item.auto_price) * quantity
        cost_basis_total += _money(item.cost_basis) * quantity
    return {
        "active_items": len(active_items),
        "estimated_list_value": round(estimated_list_value, 2),
        "cost_basis_total": round(cost_basis_total, 2),
        "evidence_url": "/inventory",
    }


def _build_loan_snapshot(session: Session, *, start: datetime, end: datetime) -> dict[str, Any]:
    rows = session.exec(
        select(BankTransaction)
        .where(BankTransaction.is_removed == False)  # noqa: E712
        .where(BankTransaction.posted_at >= start)
        .where(BankTransaction.posted_at <= end)
    ).all()
    rows = dedupe_bank_rows_for_reporting(list(rows))
    loan_proceeds = 0.0
    paybacks = 0.0
    platform_payouts = 0.0
    for row in rows:
        amount = _money(row.amount)
        category = str(row.expense_category or "").strip().lower()
        classification = str(row.classification or "").strip().lower()
        if category == "loan_proceeds" and amount > 0:
            loan_proceeds += amount
        if category == "loan_owner_payments" and amount < 0:
            paybacks += abs(amount)
        if category == "platform_payouts" or classification.endswith("_payout"):
            platform_payouts += abs(amount)
    return {
        "observed_loan_proceeds": round(loan_proceeds, 2),
        "observed_paybacks": round(paybacks, 2),
        "observed_platform_payouts": round(platform_payouts, 2),
        "evidence_url": "/bookkeeping/bank?expense_category=loan_owner_payments",
    }


def _build_cash_snapshot(session: Session) -> dict[str, Any]:
    rows = session.exec(
        select(BankTransaction)
        .where(BankTransaction.is_removed == False)  # noqa: E712
        .where(BankTransaction.balance != None)  # noqa: E711
    ).all()
    latest_by_account: dict[str, BankTransaction] = {}
    for row in rows:
        account_key = row.account_label or row.account_type or "Unknown account"
        current = latest_by_account.get(account_key)
        row_sort = (row.posted_at or row.transaction_at or row.created_at, row.id or 0)
        current_sort = (
            current.posted_at or current.transaction_at or current.created_at,
            current.id or 0,
        ) if current else None
        if current is None or row_sort > current_sort:
            latest_by_account[account_key] = row

    accounts = []
    latest_known_cash = 0.0
    for account_label, row in sorted(latest_by_account.items()):
        balance = _money(row.balance)
        latest_known_cash += balance
        accounts.append(
            {
                "account_label": account_label,
                "account_type": row.account_type or "unknown",
                "balance": balance,
                "posted_at": (row.posted_at or row.transaction_at or row.created_at).date().isoformat(),
                "evidence_url": "/bookkeeping/bank",
            }
        )
    return {
        "latest_known_cash": round(latest_known_cash, 2),
        "accounts": accounts,
        "evidence_url": "/bookkeeping/bank",
    }


def build_ops_agent_context(
    session: Session,
    *,
    now: datetime | None = None,
    days: int = 90,
) -> dict[str, Any]:
    safe_days = max(_positive_int(days, default=90), 1)
    end = now or datetime.now(timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    start = end - timedelta(days=safe_days)
    finance_snapshot = build_finance_range_snapshot(
        session,
        start=start,
        end=end,
        day_count=safe_days,
    )
    return {
        "finance_statement": finance_snapshot["statement"],
        "channel_velocity": _build_channel_velocity(session, start=start, end=end, days=safe_days),
        "inventory_snapshot": _build_inventory_snapshot(session),
        "loan_snapshot": _build_loan_snapshot(session, start=start, end=end),
        "cash_snapshot": _build_cash_snapshot(session),
        "range": {
            "start": start.date().isoformat(),
            "end": end.date().isoformat(),
            "days": safe_days,
        },
    }
