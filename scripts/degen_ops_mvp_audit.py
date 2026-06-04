from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]


def _contains(repo_root: Path, path: str, *needles: str) -> bool:
    text = (repo_root / path).read_text(encoding="utf-8")
    return all(needle in text for needle in needles)


def _criterion(
    *,
    item_id: str,
    label: str,
    status: str,
    evidence: list[str],
    remaining: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": item_id,
        "label": label,
        "status": status,
        "evidence": evidence,
        "remaining": remaining or [],
    }


def build_mvp_audit(*, repo_root: Path | str = REPO_ROOT) -> dict[str, Any]:
    root = Path(repo_root)
    criteria = [
        _criterion(
            item_id="cash_revenue_profit_expenses",
            label="Can summarize current cash/revenue/profit/expenses",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "get_finance_snapshot", "get_cash_snapshot")
                and _contains(root, "app/ops_agent.py", "build_finance_range_snapshot", "_build_cash_snapshot")
                else "missing"
            ),
            evidence=[
                "get_finance_snapshot",
                "get_cash_snapshot",
                "app/ops_agent.py:_build_cash_snapshot",
            ],
        ),
        _criterion(
            item_id="sell_through_by_channel",
            label="Can estimate sell-through speed by channel",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "get_channel_velocity")
                and _contains(root, "app/ops_agent.py", "_build_channel_velocity", "_build_sell_through")
                else "missing"
            ),
            evidence=[
                "get_channel_velocity",
                "app/ops_agent.py:_build_channel_velocity",
                "app/ops_agent.py:_build_sell_through",
            ],
        ),
        _criterion(
            item_id="loan_repayment_timing",
            label="Can model loan repayment timing",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "get_loan_and_payback_snapshot")
                and _contains(root, "app/ops_agent.py", "_build_payback_plan", "payback_plan")
                else "missing"
            ),
            evidence=[
                "get_loan_and_payback_snapshot",
                "app/ops_agent.py:_build_payback_plan",
                "payback_plan",
            ],
        ),
        _criterion(
            item_id="cash_flow_risks",
            label="Can flag cash-flow risks",
            status=(
                "satisfied"
                if _contains(root, "app/ops_agent.py", "_build_risk_flags", "risk_flags", "reserve_gap")
                else "missing"
            ),
            evidence=[
                "risk_flags",
                "app/ops_agent.py:_build_risk_flags",
                "cash_flow.reserve_gap",
            ],
        ),
        _criterion(
            item_id="partner_weekly_update",
            label="Can generate a partner-ready weekly business update",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "generate_partner_update")
                and _contains(root, "app/ops_agent.py", "_build_partner_update")
                else "missing"
            ),
            evidence=[
                "generate_partner_update",
                "app/ops_agent.py:_build_partner_update",
            ],
        ),
        _criterion(
            item_id="buy_recommendation",
            label="Can recommend whether a proposed buy is safe, risky, or not worth doing",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "evaluate_inventory_buy")
                and _contains(root, "app/ops_agent.py", "_choose_verdict", "not worth doing", "risky", "safe")
                else "missing"
            ),
            evidence=[
                "evaluate_inventory_buy",
                "app/ops_agent.py:_choose_verdict",
                "safe/risky/not worth doing",
            ],
        ),
        _criterion(
            item_id="evidence_backed_recommendations",
            label="Shows the evidence behind every recommendation",
            status=(
                "satisfied"
                if _contains(root, "scripts/degen_ops_pilot_demo.py", "evidence", "routing", "cash_flow", "payback_plan")
                and _contains(root, "tests/test_degen_ops_pilot_demo.py", "evidence", "routing", "cash_flow", "payback_plan")
                else "missing"
            ),
            evidence=[
                "scripts/degen_ops_pilot_demo.py",
                "tests/test_degen_ops_pilot_demo.py",
                "evidence/routing/cash_flow/payback_plan",
            ],
        ),
        _criterion(
            item_id="read_only_guardrails",
            label="Read-only first with no money, inventory, production, or messaging writes",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "SET TRANSACTION READ ONLY", "PRAGMA query_only = ON")
                and _contains(root, "app/ops_agent.py", "No money movement", "No inventory changes", "No production writes")
                else "missing"
            ),
            evidence=[
                "app/ops_mcp.py:_apply_session_read_only",
                "READ_ONLY_GUARDRAILS",
            ],
        ),
        _criterion(
            item_id="chatbot_and_mcp_harness",
            label="Can be used as chatbot plus MCP harness",
            status=(
                "satisfied"
                if _contains(root, "app/ops_chat.py", "run_chat_turn", "tool_schemas_for_scope")
                and _contains(root, "scripts/degen_ops_mcp.py", "app.ops_mcp")
                else "missing"
            ),
            evidence=[
                "app/ops_chat.py",
                "scripts/degen_ops_chat.py",
                "scripts/degen_ops_mcp.py",
            ],
        ),
        _criterion(
            item_id="team_live_rollout",
            label="Employees and partners can use live data safely",
            status="pending_decision",
            evidence=[
                "scripts/degen_ops_approval_packet.py",
                "scripts/degen_ops_live_data.py",
                "scripts/degen_ops_team_package.py",
            ],
            remaining=[
                "live_data_access_topology",
                "read_only_db_credential_or_green_hosted_client",
            ],
        ),
    ]
    blocking_items = [item for item in criteria if item["status"] == "missing"]
    return {
        "name": "degen_ops_mvp_audit",
        "mvp_code_ready": not blocking_items,
        "team_live_rollout_ready": False,
        "criteria": criteria,
        "blocking_items": [item["id"] for item in blocking_items],
        "remaining_decisions": [
            "live_data_access_topology",
            "read_only_db_credential_or_green_hosted_client",
        ],
    }


def _render_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops MVP Audit",
        "",
        f"- mvp_code_ready: {str(audit['mvp_code_ready']).lower()}",
        f"- team_live_rollout_ready: {str(audit['team_live_rollout_ready']).lower()}",
        "",
        "## Criteria",
        "",
    ]
    lines.extend(f"- {item['status']}: {item['label']}" for item in audit["criteria"])
    lines.extend(["", "## Remaining Decisions", ""])
    lines.extend(f"- {item}" for item in audit["remaining_decisions"])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Degen Ops MVP criteria against current repo artifacts.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit = build_mvp_audit(repo_root=args.repo_root)
    if args.json:
        print(json.dumps(audit, indent=2, sort_keys=True))
    else:
        print(_render_markdown(audit), end="")
    return 0 if audit["mvp_code_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
