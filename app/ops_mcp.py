from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import os
from typing import Any, Callable, Iterator

from sqlalchemy import text
from sqlmodel import Session

from .db import Session as DbSession
from .db import database_url
from .db import engine
from .ops_agent import READ_ONLY_GUARDRAILS, build_ops_agent_context, build_ops_agent_recommendation


DEGEN_OPS_MCP_TOOL_NAMES = [
    "get_ops_agent_manifest",
    "get_finance_snapshot",
    "get_cash_snapshot",
    "get_inventory_snapshot",
    "get_channel_velocity",
    "get_loan_and_payback_snapshot",
    "evaluate_inventory_buy",
    "generate_partner_update",
]

DEGEN_OPS_SCOPE_TOOL_NAMES = {
    "owner": DEGEN_OPS_MCP_TOOL_NAMES,
    "partner": [
        "get_ops_agent_manifest",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "evaluate_inventory_buy",
        "generate_partner_update",
    ],
    "employee": [
        "get_ops_agent_manifest",
        "get_inventory_snapshot",
        "get_channel_velocity",
    ],
}

OWNER_ONLY_TOOL_NAMES = [
    "get_cash_snapshot",
    "get_loan_and_payback_snapshot",
]

PARTNER_REDACTION_NOTE = (
    "Partner scope hides raw cash balances, account balances, reserve-gap dollars, "
    "and owner loan/payback totals. Use owner scope for exact cash and loan ledger evidence."
)


@contextmanager
def _default_session_factory() -> Iterator[Session]:
    with DbSession(engine) as session:
        _apply_session_read_only(session, database_url)
        try:
            yield session
        finally:
            _reset_session_read_only(session, database_url)


def _is_postgres_url(db_url: str) -> bool:
    normalized = (db_url or "").lower()
    return normalized.startswith(("postgresql://", "postgresql+", "postgres://"))


def _apply_session_read_only(session: Any, db_url: str) -> None:
    if not hasattr(session, "exec"):
        return
    if _is_postgres_url(db_url):
        session.exec(text("SET TRANSACTION READ ONLY"))
    elif db_url.startswith("sqlite"):
        session.exec(text("PRAGMA query_only = ON"))


def _reset_session_read_only(session: Any, db_url: str) -> None:
    if not hasattr(session, "exec"):
        return
    if db_url.startswith("sqlite"):
        session.exec(text("PRAGMA query_only = OFF"))


def _money(value: Any) -> float:
    try:
        return round(float(value or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _format_money(value: Any) -> str:
    amount = _money(value)
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.0f}"


def _cash_safety_status(cash_flow: dict[str, Any]) -> str:
    reserve_gap = _money(cash_flow.get("reserve_gap"))
    return "below_minimum_reserve" if reserve_gap < 0 else "at_or_above_minimum_reserve"


def _cash_safety_sentence(cash_flow: dict[str, Any]) -> str:
    if _cash_safety_status(cash_flow) == "below_minimum_reserve":
        return "The modeled buy falls below the configured reserve."
    return "The modeled buy stays at or above the configured reserve."


def _redacted_partner_update(result: dict[str, Any], scenario: dict[str, Any]) -> str:
    unit_economics = result.get("unit_economics") or {}
    sell_through = result.get("sell_through") or {}
    payback_plan = result.get("payback_plan") or {}
    cash_flow = result.get("cash_flow") or {}
    risk_flags = result.get("risk_flags") or []
    lot_name = str(scenario.get("lot_name") or "Proposed lot").strip()
    sell_weeks = sell_through.get("estimated_weeks")
    sell_label = f"{sell_weeks} week(s)" if sell_weeks is not None else "unknown"
    risks = "; ".join(risk_flags) if risk_flags else "No major cash-flow flags in this model."
    return (
        "Weekly business update\n"
        f"Buy decision: {str(result.get('verdict') or '').upper()} for {lot_name}.\n"
        f"Expected profit: {_format_money(unit_economics.get('expected_profit'))} "
        f"at {_money(unit_economics.get('expected_margin_pct'))}% margin.\n"
        f"Estimated sell-through: {sell_label} using matched channel evidence.\n"
        f"Cash safety: {_cash_safety_sentence(cash_flow)} "
        "Exact cash balances and reserve-gap dollars are owner-scope only.\n"
        f"Weekly payback plan: {_format_money(payback_plan.get('weekly_payback'))}/week "
        f"for {payback_plan.get('target_weeks') or 0} week(s).\n"
        f"Risks: {risks}\n"
        "This is read-only decision support; no payments, listings, inventory, or messages were changed."
    )


def _redact_partner_recommendation(result: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    redacted = deepcopy(result)
    cash_flow = result.get("cash_flow") or {}
    redacted["cash_flow"] = {
        "purchase_cost": _money(cash_flow.get("purchase_cost")),
        "cash_safety": _cash_safety_status(cash_flow),
        "cash_safety_summary": _cash_safety_sentence(cash_flow),
        "owner_scope_required_for": [
            "cash_on_hand",
            "post_buy_cash",
            "minimum_cash_reserve",
            "reserve_gap",
            "account_balances",
        ],
    }

    payback_plan = redacted.get("payback_plan")
    if isinstance(payback_plan, dict):
        payback_plan["weeks"] = [
            {
                "week": week.get("week"),
                "planned_payback": week.get("planned_payback"),
                "below_reserve": week.get("below_reserve"),
            }
            for week in payback_plan.get("weeks", [])
            if isinstance(week, dict)
        ]

    evidence = []
    for row in redacted.get("evidence", []):
        if not isinstance(row, dict):
            continue
        if row.get("source") == "loan_snapshot":
            safe_row = dict(row)
            safe_row["detail"] = "Loan/payback context checked. Exact owner loan/payback totals are owner-scope only."
            safe_row["url"] = ""
            evidence.append(safe_row)
        else:
            evidence.append(row)
    redacted["evidence"] = evidence
    redacted["partner_update"] = _redacted_partner_update(result, scenario)
    redacted["redaction_note"] = PARTNER_REDACTION_NOTE
    redacted["owner_scope_required_for"] = OWNER_ONLY_TOOL_NAMES[:]
    redacted["read_only"] = True
    return redacted


class DegenOpsMcpHarness:
    def __init__(self, session_factory: Callable[[], Any] | None = None):
        self._session_factory = session_factory or _default_session_factory

    @contextmanager
    def _session(self):
        session_or_cm = self._session_factory()
        if hasattr(session_or_cm, "__enter__") and hasattr(session_or_cm, "__exit__"):
            with session_or_cm as session:
                yield session
            return
        yield session_or_cm

    def get_context(self, days: int = 90) -> dict[str, Any]:
        with self._session() as session:
            return build_ops_agent_context(session, days=days)

    def get_manifest(self, *, scope: str, tools: list[str]) -> dict[str, Any]:
        return {
            "name": "degen-ops-readonly",
            "scope": scope,
            "tools": tools,
            "read_only": True,
            "guardrails": READ_ONLY_GUARDRAILS[:],
            "notes": [
                "This MCP server exposes bounded Degen business tools only.",
                "It does not expose arbitrary SQL or mutation tools.",
                "Use separate Hermes instances or configs for owner, partner, and employee scopes.",
                "Partner scope can evaluate buys but hides raw cash and owner loan/payback details by default.",
            ],
            "owner_only_tools": OWNER_ONLY_TOOL_NAMES[:],
        }

    def get_finance_snapshot(self, days: int = 90) -> dict[str, Any]:
        context = self.get_context(days=days)
        return {
            "finance_statement": context["finance_statement"],
            "range": context["range"],
            "evidence": [{"source": "finance", "url": "/finance"}],
            "read_only": True,
        }

    def get_cash_snapshot(self) -> dict[str, Any]:
        context = self.get_context(days=90)
        return {
            "cash_snapshot": context["cash_snapshot"],
            "evidence": [{"source": "bank_transactions", "url": context["cash_snapshot"].get("evidence_url", "/bookkeeping/bank")}],
            "read_only": True,
        }

    def get_inventory_snapshot(self) -> dict[str, Any]:
        context = self.get_context(days=90)
        return {
            "inventory_snapshot": context["inventory_snapshot"],
            "evidence": [{"source": "inventory_items", "url": context["inventory_snapshot"].get("evidence_url", "/inventory")}],
            "read_only": True,
        }

    def get_channel_velocity(self, days: int = 90, category: str = "") -> dict[str, Any]:
        context = self.get_context(days=days)
        rows = context["channel_velocity"]
        category_norm = (category or "").strip().lower()
        if category_norm:
            rows = [
                row
                for row in rows
                if category_norm in str(row.get("matched_category") or "").strip().lower()
            ]
        return {
            "channel_velocity": rows,
            "range": context["range"],
            "evidence": [{"source": "orders_and_transactions", "url": "/reports"}],
            "read_only": True,
        }

    def get_loan_and_payback_snapshot(self, days: int = 90) -> dict[str, Any]:
        context = self.get_context(days=days)
        return {
            "loan_snapshot": context["loan_snapshot"],
            "range": context["range"],
            "evidence": [
                {
                    "source": "bank_transactions",
                    "url": context["loan_snapshot"].get("evidence_url", "/bookkeeping/bank"),
                }
            ],
            "read_only": True,
        }

    def evaluate_inventory_buy(
        self,
        scenario: dict[str, Any],
        days: int = 90,
        *,
        audience_scope: str = "owner",
    ) -> dict[str, Any]:
        context = self.get_context(days=days)
        enriched_scenario = dict(scenario or {})
        if not enriched_scenario.get("cash_on_hand"):
            enriched_scenario["cash_on_hand"] = (context.get("cash_snapshot") or {}).get("latest_known_cash", 0.0)
        result = build_ops_agent_recommendation(enriched_scenario, context)
        result["read_only"] = True
        if _normalize_scope(audience_scope) == "partner":
            return _redact_partner_recommendation(result, enriched_scenario)
        return result

    def generate_partner_update(
        self,
        scenario: dict[str, Any],
        days: int = 90,
        *,
        audience_scope: str = "owner",
    ) -> dict[str, Any]:
        result = self.evaluate_inventory_buy(scenario, days=days, audience_scope=audience_scope)
        return {
            "partner_update": result["partner_update"],
            "verdict": result.get("verdict"),
            "risk_flags": result.get("risk_flags", []),
            "evidence": result.get("evidence", []),
            "read_only_guardrails": result.get("read_only_guardrails", []),
            "redaction_note": result.get("redaction_note", ""),
            "read_only": True,
        }


def _normalize_scope(scope: str | None = None) -> str:
    raw = (scope if scope is not None else os.getenv("DEGEN_OPS_MCP_SCOPE", "employee")).strip().lower()
    normalized = raw or "employee"
    if normalized not in DEGEN_OPS_SCOPE_TOOL_NAMES:
        supported = ", ".join(sorted(DEGEN_OPS_SCOPE_TOOL_NAMES))
        raise ValueError(f"Unsupported Degen Ops MCP scope {normalized!r}. Supported scopes: {supported}.")
    return normalized


def register_degen_ops_tools(
    mcp: Any,
    *,
    harness: DegenOpsMcpHarness | None = None,
    scope: str | None = None,
) -> Any:
    ops = harness or DegenOpsMcpHarness()
    normalized_scope = _normalize_scope(scope)
    allowed_tools = DEGEN_OPS_SCOPE_TOOL_NAMES[normalized_scope]

    def should_register(tool_name: str) -> bool:
        return tool_name in allowed_tools

    @mcp.tool(description="Read-only manifest listing Degen Ops MCP scope, exposed tools, and guardrails.")
    def get_ops_agent_manifest() -> dict[str, Any]:
        return ops.get_manifest(scope=normalized_scope, tools=allowed_tools)

    if should_register("get_finance_snapshot"):
        @mcp.tool(description="Read-only Degen finance snapshot from existing finance/reporting helpers.")
        def get_finance_snapshot(days: int = 90) -> dict[str, Any]:
            return ops.get_finance_snapshot(days=days)

    if should_register("get_cash_snapshot"):
        @mcp.tool(description="Read-only latest known cash snapshot from bank rows with balances.")
        def get_cash_snapshot() -> dict[str, Any]:
            return ops.get_cash_snapshot()

    if should_register("get_inventory_snapshot"):
        @mcp.tool(description="Read-only inventory count, cost basis, and list-value snapshot.")
        def get_inventory_snapshot() -> dict[str, Any]:
            return ops.get_inventory_snapshot()

    if should_register("get_channel_velocity"):
        @mcp.tool(description="Read-only sell-through velocity by TikTok, Shopify, Discord, and show/channel sales.")
        def get_channel_velocity(days: int = 90, category: str = "") -> dict[str, Any]:
            return ops.get_channel_velocity(days=days, category=category)

    if should_register("get_loan_and_payback_snapshot"):
        @mcp.tool(description="Read-only loan proceeds, owner payback, and payout timing evidence.")
        def get_loan_and_payback_snapshot(days: int = 90) -> dict[str, Any]:
            return ops.get_loan_and_payback_snapshot(days=days)

    if should_register("evaluate_inventory_buy"):
        @mcp.tool(description="Read-only proposed inventory-buy evaluation with evidence-backed routing and cash plan.")
        def evaluate_inventory_buy(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
            return ops.evaluate_inventory_buy(scenario=scenario, days=days, audience_scope=normalized_scope)

    if should_register("generate_partner_update"):
        @mcp.tool(description="Read-only partner-ready weekly update generated from a proposed buy scenario.")
        def generate_partner_update(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
            return ops.generate_partner_update(scenario=scenario, days=days, audience_scope=normalized_scope)

    return mcp


def create_mcp_server():
    try:
        from mcp.server.fastmcp import FastMCP
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "The 'mcp' package is required to run the Degen Ops MCP server. "
            "Install repo requirements first: .\\.venv\\Scripts\\python.exe -m pip install -r requirements.txt"
        ) from exc

    return register_degen_ops_tools(FastMCP("degen-ops-readonly"))


def main() -> None:
    server = create_mcp_server()
    server.run()


if __name__ == "__main__":
    main()
