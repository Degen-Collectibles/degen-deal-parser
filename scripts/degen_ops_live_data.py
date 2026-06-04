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

from scripts.degen_ops_chat import sanitize


SCOPE_TOOLS = {
    "owner": [
        "get_ops_agent_manifest",
        "get_finance_snapshot",
        "get_cash_snapshot",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "get_loan_and_payback_snapshot",
        "evaluate_inventory_buy",
        "generate_partner_update",
    ],
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

SMOKE_SCENARIO = {
    "lot_name": "Read-only smoke test lot",
    "category": "readiness-smoke",
    "purchase_cost": 1.0,
    "expected_revenue": 2.0,
    "unit_count": 1,
    "minimum_cash_reserve": 0.0,
    "target_payback_weeks": 1,
}


def _tool_args(tool_name: str, *, days: int) -> dict[str, Any]:
    if tool_name in {"get_finance_snapshot", "get_channel_velocity", "get_loan_and_payback_snapshot"}:
        return {"days": days}
    if tool_name in {"evaluate_inventory_buy", "generate_partner_update"}:
        return {"scenario": dict(SMOKE_SCENARIO), "days": days}
    return {}


def build_live_data_report(
    *,
    scope: str,
    runner: Any,
    database_url_source: str,
    database_url_configured: bool = True,
    days: int = 90,
) -> dict[str, Any]:
    normalized_scope = str(scope or "").strip().lower()
    if normalized_scope not in SCOPE_TOOLS:
        raise ValueError(f"Unsupported Degen Ops scope {scope!r}.")

    checks = []
    for tool_name in SCOPE_TOOLS[normalized_scope]:
        args = _tool_args(tool_name, days=days)
        try:
            result = runner.call_tool(tool_name, args)
            if result.get("error"):
                checks.append(
                    {
                        "tool": tool_name,
                        "status": "fail",
                        "error": sanitize(result["error"]),
                    }
                )
            elif result.get("read_only") is False:
                checks.append(
                    {
                        "tool": tool_name,
                        "status": "fail",
                        "error": "Tool response did not preserve read_only=true.",
                    }
                )
            else:
                checks.append({"tool": tool_name, "status": "pass"})
        except Exception as exc:
            checks.append(
                {
                    "tool": tool_name,
                    "status": "fail",
                    "error": sanitize(exc),
                }
            )

    ok = database_url_configured and all(check["status"] == "pass" for check in checks)
    return {
        "name": "degen_ops_live_data_verifier",
        "ok": ok,
        "scope": normalized_scope,
        "database_url_configured": bool(database_url_configured),
        "database_url_source": database_url_source,
        "read_only": True,
        "days": days,
        "tools": SCOPE_TOOLS[normalized_scope],
        "checks": checks,
    }


def _missing_database_report(*, scope: str, database_url_source: str, days: int) -> dict[str, Any]:
    normalized_scope = str(scope or "").strip().lower()
    tools = SCOPE_TOOLS.get(normalized_scope, [])
    return {
        "name": "degen_ops_live_data_verifier",
        "ok": False,
        "scope": normalized_scope,
        "database_url_configured": False,
        "database_url_source": database_url_source,
        "read_only": True,
        "days": days,
        "tools": tools,
        "checks": [],
        "error": f"Environment variable {database_url_source!r} is not set or is empty.",
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Live Data Verification",
        "",
        f"- ok: {str(report['ok']).lower()}",
        f"- scope: {report['scope']}",
        f"- database_url_configured: {str(report['database_url_configured']).lower()}",
        f"- database_url_source: {report['database_url_source']}",
        "",
        "## Checks",
        "",
    ]
    if report.get("error"):
        lines.append(f"- fail: {report['error']}")
    lines.extend(
        f"- {check['status']}: {check['tool']}{' - ' + check['error'] if check.get('error') else ''}"
        for check in report.get("checks", [])
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify live Degen Ops read-only data access for one scope.")
    parser.add_argument("--scope", choices=("owner", "partner", "employee"), required=True)
    parser.add_argument("--database-url", default="", help="Temporary database URL override. The value is never printed.")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of Markdown.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_url = args.database_url
    source = "--database-url" if database_url else args.database_url_env
    if not database_url:
        database_url = os.getenv(args.database_url_env, "")
    if not database_url:
        report = _missing_database_report(scope=args.scope, database_url_source=source, days=args.days)
        print(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report), end="")
        return 1

    os.environ["DATABASE_URL"] = database_url
    os.environ["DEGEN_OPS_MCP_SCOPE"] = args.scope

    from app.ops_chat import DegenOpsChatToolRunner

    report = build_live_data_report(
        scope=args.scope,
        runner=DegenOpsChatToolRunner(scope=args.scope),
        database_url_source=source,
        database_url_configured=True,
        days=args.days,
    )
    print(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report), end="")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
