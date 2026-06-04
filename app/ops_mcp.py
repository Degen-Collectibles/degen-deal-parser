from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterator

from sqlmodel import Session

from .db import Session as DbSession
from .db import engine
from .ops_agent import build_ops_agent_context, build_ops_agent_recommendation


DEGEN_OPS_MCP_TOOL_NAMES = [
    "get_finance_snapshot",
    "get_cash_snapshot",
    "get_inventory_snapshot",
    "get_channel_velocity",
    "get_loan_and_payback_snapshot",
    "evaluate_inventory_buy",
    "generate_partner_update",
]


@contextmanager
def _default_session_factory() -> Iterator[Session]:
    with DbSession(engine) as session:
        yield session


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

    def evaluate_inventory_buy(self, scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
        context = self.get_context(days=days)
        enriched_scenario = dict(scenario or {})
        if not enriched_scenario.get("cash_on_hand"):
            enriched_scenario["cash_on_hand"] = (context.get("cash_snapshot") or {}).get("latest_known_cash", 0.0)
        return build_ops_agent_recommendation(enriched_scenario, context)

    def generate_partner_update(self, scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
        result = self.evaluate_inventory_buy(scenario, days=days)
        return {
            "partner_update": result["partner_update"],
            "verdict": result.get("verdict"),
            "risk_flags": result.get("risk_flags", []),
            "evidence": result.get("evidence", []),
            "read_only_guardrails": result.get("read_only_guardrails", []),
            "read_only": True,
        }


def register_degen_ops_tools(mcp: Any, *, harness: DegenOpsMcpHarness | None = None) -> Any:
    ops = harness or DegenOpsMcpHarness()

    @mcp.tool(description="Read-only Degen finance snapshot from existing finance/reporting helpers.")
    def get_finance_snapshot(days: int = 90) -> dict[str, Any]:
        return ops.get_finance_snapshot(days=days)

    @mcp.tool(description="Read-only latest known cash snapshot from bank rows with balances.")
    def get_cash_snapshot() -> dict[str, Any]:
        return ops.get_cash_snapshot()

    @mcp.tool(description="Read-only inventory count, cost basis, and list-value snapshot.")
    def get_inventory_snapshot() -> dict[str, Any]:
        return ops.get_inventory_snapshot()

    @mcp.tool(description="Read-only sell-through velocity by TikTok, Shopify, Discord, and show/channel sales.")
    def get_channel_velocity(days: int = 90, category: str = "") -> dict[str, Any]:
        return ops.get_channel_velocity(days=days, category=category)

    @mcp.tool(description="Read-only loan proceeds, owner payback, and payout timing evidence.")
    def get_loan_and_payback_snapshot(days: int = 90) -> dict[str, Any]:
        return ops.get_loan_and_payback_snapshot(days=days)

    @mcp.tool(description="Read-only proposed inventory-buy evaluation with evidence-backed routing and cash plan.")
    def evaluate_inventory_buy(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
        return ops.evaluate_inventory_buy(scenario=scenario, days=days)

    @mcp.tool(description="Read-only partner-ready weekly update generated from a proposed buy scenario.")
    def generate_partner_update(scenario: dict[str, Any], days: int = 90) -> dict[str, Any]:
        return ops.generate_partner_update(scenario=scenario, days=days)

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
