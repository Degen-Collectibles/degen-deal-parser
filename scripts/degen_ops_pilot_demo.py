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


DEFAULT_SCENARIO = {
    "lot_name": "Pilot demo Pokemon sealed lot",
    "category": "Pokemon sealed",
    "purchase_cost": 2000.0,
    "expected_revenue": 3600.0,
    "unit_count": 40,
    "target_payback_weeks": 4,
    "financing_amount": 0.0,
}

SCENARIO_FIELDS = {
    "lot_name",
    "category",
    "categories",
    "matched_category",
    "inventory_category",
    "purchase_cost",
    "expected_revenue",
    "unit_count",
    "target_payback_weeks",
    "financing_amount",
}

WORKFLOW_TOOLS = [
    "get_ops_agent_manifest",
    "get_finance_snapshot",
    "get_cash_snapshot",
    "get_inventory_snapshot",
    "get_channel_velocity",
    "get_loan_and_payback_snapshot",
    "evaluate_inventory_buy",
    "generate_partner_update",
]

MAX_PILOT_HISTORY_DAYS = 365


def _bounded_history_days(value: Any) -> int:
    try:
        parsed = int(value)
    except (OverflowError, TypeError, ValueError):
        parsed = 90
    return max(1, min(parsed, MAX_PILOT_HISTORY_DAYS))


def build_pilot_demo_report(
    *,
    scope: str,
    runner: Any,
    scenario: dict[str, Any] | None = None,
    days: int = 90,
) -> dict[str, Any]:
    normalized_scope = str(scope or "").strip().lower()
    safe_days = _bounded_history_days(days)
    manifest = runner.call_tool("get_ops_agent_manifest", {})
    if normalized_scope == "employee" or "evaluate_inventory_buy" not in set(getattr(runner, "allowed_tools", [])):
        return {
            "ok": False,
            "scope": normalized_scope,
            "read_only": True,
            "reason": "employee scope does not expose buy-decision finance tools",
            "manifest": manifest,
        }

    scenario_payload = {
        key: value
        for key, value in dict(scenario or DEFAULT_SCENARIO).items()
        if key in SCENARIO_FIELDS
    }
    allowed_tools = set(getattr(runner, "allowed_tools", []))
    snapshots = {}
    if "get_finance_snapshot" in allowed_tools:
        snapshots["finance"] = runner.call_tool("get_finance_snapshot", {"days": safe_days})
    if "get_cash_snapshot" in allowed_tools:
        snapshots["cash"] = runner.call_tool("get_cash_snapshot", {})
    if "get_inventory_snapshot" in allowed_tools:
        snapshots["inventory"] = runner.call_tool("get_inventory_snapshot", {})
    if "get_channel_velocity" in allowed_tools:
        snapshots["channel_velocity"] = runner.call_tool(
            "get_channel_velocity",
            {"days": safe_days, "category": scenario_payload.get("category", "")},
        )
    if "get_loan_and_payback_snapshot" in allowed_tools:
        snapshots["loan_and_payback"] = runner.call_tool("get_loan_and_payback_snapshot", {"days": safe_days})
    evaluation = runner.call_tool(
        "evaluate_inventory_buy",
        {"scenario": scenario_payload, "days": safe_days},
    )
    update = runner.call_tool(
        "generate_partner_update",
        {"scenario": scenario_payload, "days": safe_days},
    )
    input_warnings = list(
        dict.fromkeys(
            [
                *(evaluation.get("input_warnings", []) or []),
                *(update.get("input_warnings", []) or []),
            ]
        )
    )
    report = {
        "ok": not bool(evaluation.get("error") or update.get("error")),
        "scope": normalized_scope,
        "read_only": True,
        "scenario": scenario_payload,
        "verdict": evaluation.get("verdict"),
        "risk_flags": evaluation.get("risk_flags", []),
        "evidence": evaluation.get("evidence", []) or [],
        "evidence_count": len(evaluation.get("evidence", []) or []),
        "routing": evaluation.get("routing", []) or [],
        "cash_flow": evaluation.get("cash_flow", {}) or {},
        "sell_through": evaluation.get("sell_through", {}) or {},
        "payback_plan": evaluation.get("payback_plan", {}) or {},
        "input_warnings": input_warnings,
        "partner_update": update.get("partner_update", ""),
        "snapshots_checked": list(snapshots),
        "redaction_note": evaluation.get("redaction_note", ""),
        "workflow_tools": [tool for tool in WORKFLOW_TOOLS if tool in allowed_tools],
        "read_only_guardrails": evaluation.get("read_only_guardrails", []),
    }
    if normalized_scope == "owner" and "reserve_floor" in evaluation:
        report["reserve_floor"] = evaluation["reserve_floor"]
    return report


def _missing_database_report(*, scope: str, database_url_source: str) -> dict[str, Any]:
    return {
        "ok": False,
        "scope": scope,
        "read_only": True,
        "database_url_configured": False,
        "database_url_source": database_url_source,
        "reason": f"Environment variable {database_url_source!r} is not set or is empty.",
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Pilot Demo",
        "",
        f"- ok: {str(report.get('ok')).lower()}",
        f"- scope: {report.get('scope')}",
        f"- read_only: {str(report.get('read_only')).lower()}",
    ]
    if report.get("reason"):
        lines.append(f"- reason: {report['reason']}")
    if report.get("verdict"):
        lines.extend(
            [
                f"- verdict: {report['verdict']}",
                f"- evidence_count: {report.get('evidence_count', 0)}",
                "",
                "## Evidence",
                "",
            ]
        )
        for item in report.get("evidence", []) or []:
            label = item.get("label") or item.get("source") or "evidence"
            detail = item.get("detail") or item.get("url") or ""
            lines.append(f"- {label}: {detail}")
        lines.extend(
            [
                "",
                "## Routing",
                "",
            ]
        )
        for item in report.get("routing", []) or []:
            lines.append(f"- {item.get('channel', 'Unknown')}: {item.get('recommended_units', 0)} units")
        lines.extend(
            [
                "",
                "## Cash And Payback",
                "",
                f"- reserve_gap: {report.get('cash_flow', {}).get('reserve_gap', 'owner-scope only')}",
                f"- cash_safety: {report.get('cash_flow', {}).get('cash_safety', 'owner exact cash view')}",
                f"- weekly_payback: {report.get('payback_plan', {}).get('weekly_payback')}",
            ]
        )
        reserve_floor = report.get("reserve_floor")
        if report.get("scope") == "owner" and isinstance(reserve_floor, dict):
            lines.extend(
                [
                    f"- reserve_floor_configured: {str(bool(reserve_floor.get('configured'))).lower()}",
                    f"- reserve_floor_source: {reserve_floor.get('source')}",
                    f"- reserve_floor_amount: {reserve_floor.get('amount')}",
                ]
            )
        input_warnings = report.get("input_warnings", []) or []
        if input_warnings:
            lines.extend(["", "## Input Warnings", ""])
            lines.extend(f"- {warning}" for warning in input_warnings)
        lines.extend(
            [
                "",
                "## Partner Update Draft",
                "",
                str(report.get("partner_update") or ""),
            ]
        )
    return "\n".join(lines) + "\n"


def stdout_safe(text: str, *, encoding: str | None = None) -> str:
    target_encoding = encoding or getattr(sys.stdout, "encoding", None) or "utf-8"
    return text.encode(target_encoding, errors="replace").decode(target_encoding, errors="replace")


def emit(text: str) -> None:
    sys.stdout.write(stdout_safe(text))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a no-LLM read-only Degen Ops buy-decision pilot demo.")
    parser.add_argument("--scope", choices=("owner", "partner", "employee"), default="partner")
    parser.add_argument("--database-url", default="", help="Temporary database URL override. The value is never printed.")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    parser.add_argument("--scenario-json", default="", help="Optional JSON object overriding the default demo scenario.")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _load_scenario(raw: str) -> dict[str, Any]:
    if not raw:
        return dict(DEFAULT_SCENARIO)
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("--scenario-json must be a JSON object.")
    scenario = dict(DEFAULT_SCENARIO)
    scenario.update(parsed)
    return scenario


def main() -> int:
    args = parse_args()
    database_url = args.database_url
    source = "--database-url" if database_url else args.database_url_env
    if not database_url:
        database_url = os.getenv(args.database_url_env, "")
    if not database_url:
        report = _missing_database_report(scope=args.scope, database_url_source=source)
        emit(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report))
        return 1

    os.environ["DATABASE_URL"] = database_url
    os.environ["DEGEN_OPS_MCP_SCOPE"] = args.scope

    from app.ops_chat import DegenOpsChatToolRunner

    report = build_pilot_demo_report(
        scope=args.scope,
        runner=DegenOpsChatToolRunner(scope=args.scope),
        scenario=_load_scenario(args.scenario_json),
        days=args.days,
    )
    emit(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
