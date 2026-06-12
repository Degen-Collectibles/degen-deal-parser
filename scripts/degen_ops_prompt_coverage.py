from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["LOG_TO_FILE"] = "false"

from app.ops_mcp import DEGEN_OPS_SCOPE_TOOL_NAMES, _normalize_scope


DEFAULT_PROMPT_CASES = [
    {
        "id": "partner_buy_decision",
        "scope": "partner",
        "prompt": "Should we buy this lot, how fast can we sell it, where should it go, and what payback plan is safe?",
        "required_tools": ["evaluate_inventory_buy", "get_channel_velocity", "get_inventory_snapshot", "get_finance_snapshot"],
        "forbidden_tools": ["get_cash_snapshot", "get_loan_and_payback_snapshot"],
    },
    {
        "id": "tiktok_151_sales",
        "scope": "employee",
        "prompt": "How many 151 packs have we sold in the last seven days on TikTok?",
        "required_tools": ["get_tiktok_product_sales"],
        "forbidden_tools": ["get_cash_snapshot", "get_loan_and_payback_snapshot"],
    },
    {
        "id": "top_products_followup_context",
        "scope": "employee",
        "prompt": "Top 5 selling products. No, I mean on TikTok.",
        "required_tools": ["get_tiktok_top_products", "get_sales_summary"],
        "forbidden_tools": ["get_cash_snapshot", "get_loan_and_payback_snapshot"],
    },
    {
        "id": "price_and_market_trend",
        "scope": "employee",
        "prompt": "What is the price of 151 packs and is the market trending up or down?",
        "required_tools": ["get_price_lookup", "get_market_trend_lookup", "get_web_search"],
        "forbidden_tools": ["get_cash_snapshot", "get_loan_and_payback_snapshot"],
    },
    {
        "id": "discord_show_sales",
        "scope": "employee",
        "prompt": "How many 151 packs did we sell on Discord or shows?",
        "required_tools": ["get_discord_sales_summary"],
        "forbidden_tools": ["get_cash_snapshot", "get_loan_and_payback_snapshot"],
    },
    {
        "id": "owner_employee_clock_status",
        "scope": "owner",
        "prompt": "Has Alex clocked out?",
        "required_tools": ["get_employee_clock_status"],
        "forbidden_tools": [],
    },
    {
        "id": "finance_today",
        "scope": "owner",
        "prompt": "How much money have we made today?",
        "required_tools": ["get_finance_snapshot", "get_sales_summary"],
        "forbidden_tools": [],
    },
    {
        "id": "weekly_partner_update_draft",
        "scope": "partner",
        "prompt": "Draft this week's partner update.",
        "required_tools": ["generate_weekly_partner_update_draft"],
        "forbidden_tools": ["get_cash_snapshot", "get_loan_and_payback_snapshot"],
    },
]


def build_prompt_coverage_report(*, cases: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    rows = []
    for case in cases or DEFAULT_PROMPT_CASES:
        scope = _normalize_scope(str(case.get("scope") or "employee"))
        tools = set(DEGEN_OPS_SCOPE_TOOL_NAMES[scope])
        required = [str(tool) for tool in case.get("required_tools", [])]
        forbidden = [str(tool) for tool in case.get("forbidden_tools", [])]
        missing_required = [tool for tool in required if tool not in tools]
        forbidden_present = [tool for tool in forbidden if tool in tools]
        rows.append(
            {
                "id": str(case.get("id") or ""),
                "scope": scope,
                "prompt": str(case.get("prompt") or ""),
                "status": "pass" if not missing_required and not forbidden_present else "fail",
                "required_tools": required,
                "available_tools": sorted(tool for tool in required if tool in tools),
                "missing_required_tools": missing_required,
                "forbidden_tools": forbidden,
                "forbidden_tools_absent": sorted(tool for tool in forbidden if tool not in tools),
                "forbidden_tools_present": forbidden_present,
                "read_only": True,
            }
        )
    return {
        "name": "degen_ops_prompt_coverage",
        "ok": all(row["status"] == "pass" for row in rows),
        "case_count": len(rows),
        "cases": rows,
        "note": "Static prompt-to-tool coverage only; this does not call an LLM or live data.",
        "read_only": True,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = ["# Degen Ops Prompt Coverage", "", f"- ok: {str(report['ok']).lower()}", ""]
    for case in report["cases"]:
        lines.extend(
            [
                f"## {case['id']}",
                "",
                f"- scope: {case['scope']}",
                f"- status: {case['status']}",
                f"- required_tools: {', '.join(case['required_tools']) or 'none'}",
                f"- missing_required_tools: {', '.join(case['missing_required_tools']) or 'none'}",
                f"- forbidden_tools_present: {', '.join(case['forbidden_tools_present']) or 'none'}",
                "",
            ]
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check static prompt-to-tool coverage for Degen Ops questions.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_prompt_coverage_report()
    print(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
