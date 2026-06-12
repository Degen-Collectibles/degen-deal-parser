from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["LOG_TO_FILE"] = "false"

from app.ops_chat import tool_schemas_for_scope
from app.ops_mcp import DEGEN_OPS_SCOPE_TOOL_NAMES, _normalize_scope
from scripts.degen_ops_team_package import build_team_package


REQUIRED_ARTIFACTS = [
    "app/ops_mcp.py",
    "app/ops_agent.py",
    "app/ops_chat.py",
    "scripts/degen_ops_mcp.py",
    "scripts/degen_ops_mcp_smoke.py",
    "scripts/degen_ops_mcp_config.py",
    "scripts/degen_ops_answer_eval.py",
    "scripts/degen_ops_prompt_coverage.py",
    "scripts/degen_ops_deploy_preflight.py",
    "scripts/degen_ops_change_manifest.py",
    "scripts/degen_ops_chat.py",
    "scripts/degen_ops_readiness.py",
    "scripts/degen_ops_completion_audit.py",
    "scripts/degen_ops_live_data.py",
    "scripts/degen_ops_local_gate.py",
    "scripts/degen_ops_approval_packet.py",
    "scripts/degen_ops_green_pilot_packet.py",
    "scripts/degen_ops_launch_checklist.py",
    "scripts/degen_ops_pilot_demo.py",
    "scripts/degen_ops_mvp_audit.py",
    "scripts/degen_ops_scope_audit.py",
    "scripts/degen_ops_rollout_gate.py",
    "scripts/degen_ops_topology_plan.py",
    "scripts/degen_ops_team_package.py",
    "docs/ops/degen-ops-agent-instructions.md",
    "docs/ops/degen-ops-answer-examples.json",
    "docs/ops/degen-ops-hermes-mcp-pilot.md",
    "docs/ops/degen-ops-team-rollout-prd.md",
    "docs/ops/degen-ops-readonly-db-role.sql",
]


def _artifact_rows(repo_root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": path,
            "exists": (repo_root / path).exists(),
        }
        for path in REQUIRED_ARTIFACTS
    ]


def _pass_check(name: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": "pass", "detail": detail}


def _fail_check(name: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": "fail", "detail": detail}


@contextmanager
def _without_env_var(name: str):
    existing = os.environ.pop(name, None)
    try:
        yield
    finally:
        if existing is not None:
            os.environ[name] = existing


def _scope_report() -> dict[str, dict[str, Any]]:
    report = {}
    for scope, tools in DEGEN_OPS_SCOPE_TOOL_NAMES.items():
        sorted_tools = sorted(tools)
        schema_tools = sorted(schema["function"]["name"] for schema in tool_schemas_for_scope(scope))
        report[scope] = {
            "tools": sorted_tools,
            "tool_count": len(sorted_tools),
            "chat_schema_matches_mcp_scope": schema_tools == sorted_tools,
        }
    return report


def _checks(repo_root: Path, scopes: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    checks: list[dict[str, str]] = []
    artifacts = _artifact_rows(repo_root)
    missing = [row["path"] for row in artifacts if not row["exists"]]
    if missing:
        checks.append(_fail_check("required_artifacts_present", f"Missing: {', '.join(missing)}"))
    else:
        checks.append(_pass_check("required_artifacts_present", "All required Ops MCP/chat/docs artifacts exist."))

    employee_tools = set(scopes["employee"]["tools"])
    sensitive_employee_tools = employee_tools & {
        "get_finance_snapshot",
        "get_cash_snapshot",
        "get_loan_and_payback_snapshot",
        "evaluate_inventory_buy",
        "generate_partner_update",
    }
    if sensitive_employee_tools:
        checks.append(
            _fail_check(
                "employee_scope_excludes_sensitive_tools",
                f"Employee scope exposes: {', '.join(sorted(sensitive_employee_tools))}",
            )
        )
    else:
        checks.append(_pass_check("employee_scope_excludes_sensitive_tools", "Employee scope has only manifest, inventory, and velocity."))

    partner_tools = set(scopes["partner"]["tools"])
    partner_owner_only_tools = partner_tools & {"get_cash_snapshot", "get_loan_and_payback_snapshot"}
    if partner_owner_only_tools:
        checks.append(
            _fail_check(
                "partner_scope_excludes_owner_only_raw_cash_and_loan_tools",
                f"Partner scope exposes: {', '.join(sorted(partner_owner_only_tools))}",
            )
        )
    elif "evaluate_inventory_buy" in partner_tools and "generate_partner_update" in partner_tools:
        checks.append(
            _pass_check(
                "partner_scope_excludes_owner_only_raw_cash_and_loan_tools",
                "Partner scope can evaluate buys but excludes raw cash and loan snapshots.",
            )
        )
    else:
        checks.append(_fail_check("partner_scope_excludes_owner_only_raw_cash_and_loan_tools", "Partner scope cannot run the buy workflow."))

    if all(scope["chat_schema_matches_mcp_scope"] for scope in scopes.values()):
        checks.append(_pass_check("chat_tool_schemas_match_mcp_scopes", "Chat tool schemas match MCP scope tool lists."))
    else:
        checks.append(_fail_check("chat_tool_schemas_match_mcp_scopes", "At least one chat schema list differs from MCP scope tools."))

    with _without_env_var("DEGEN_OPS_MCP_SCOPE"):
        missing_scope_default = _normalize_scope(None)
    if missing_scope_default == "employee":
        checks.append(_pass_check("missing_scope_defaults_to_employee", "Missing MCP scope defaults to employee tools."))
    else:
        checks.append(_fail_check("missing_scope_defaults_to_employee", "Missing MCP scope does not fail safe to employee scope."))

    template = (repo_root / "docs/ops/degen-ops-readonly-db-role.sql").read_text(encoding="utf-8")
    lowered_template = template.lower()
    if (
        "replace_with_generated_password" in lowered_template
        and "grant select on all tables" in lowered_template
        and "grant insert" not in lowered_template
        and "postgresql://" not in lowered_template
    ):
        checks.append(_pass_check("readonly_db_template_is_placeholder_select_only", "DB role template is placeholder-based and select-only."))
    else:
        checks.append(_fail_check("readonly_db_template_is_placeholder_select_only", "DB role template is missing placeholders or has unsafe grants."))

    employee_package = build_team_package(
        scope="employee",
        clients=["hermes"],
        database_url_env="DEGEN_OPS_READONLY_DATABASE_URL",
    )
    if (
        "degen_ops_employee" in employee_package
        and "degen_ops_owner" not in employee_package
        and "postgresql://" not in employee_package
        and "get_cash_snapshot" not in employee_package
    ):
        checks.append(_pass_check("employee_access_package_is_scoped_and_secret_free", "Employee package is scoped and does not print DB URLs."))
    else:
        checks.append(_fail_check("employee_access_package_is_scoped_and_secret_free", "Employee package leaked owner scope, sensitive tools, or raw DB URLs."))

    partner_package = build_team_package(
        scope="partner",
        clients=["hermes"],
        database_url_env="DEGEN_OPS_READONLY_DATABASE_URL",
    )
    if (
        "degen_ops_partner" in partner_package
        and "degen_ops_owner" not in partner_package
        and "postgresql://" not in partner_package
        and "get_cash_snapshot" not in partner_package
        and "get_loan_and_payback_snapshot" not in partner_package
        and "evaluate_inventory_buy" in partner_package
    ):
        checks.append(_pass_check("partner_access_package_is_redacted_and_secret_free", "Partner package can run buy workflow without raw cash, loan, or DB URLs."))
    else:
        checks.append(_fail_check("partner_access_package_is_redacted_and_secret_free", "Partner package leaked owner scope, owner-only tools, or raw DB URLs."))

    return checks


def build_readiness_report(*, repo_root: Path | str = REPO_ROOT) -> dict[str, Any]:
    root = Path(repo_root)
    artifacts = _artifact_rows(root)
    scopes = _scope_report()
    checks = _checks(root, scopes)
    code_ready = all(check["status"] == "pass" for check in checks)
    return {
        "name": "degen_ops_readiness",
        "code_ready": code_ready,
        "team_rollout_ready": False,
        "required_decisions": [
            "live_data_access_topology",
            "read_only_db_credential_or_green_hosted_client",
        ],
        "reason_team_rollout_not_ready": (
            "The read-only MCP/chat code is ready for local pilot checks, but non-owner live-data access "
            "still needs an approved topology and credential."
        ),
        "scopes": scopes,
        "artifacts": artifacts,
        "checks": checks,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Readiness",
        "",
        f"- code_ready: {str(report['code_ready']).lower()}",
        f"- team_rollout_ready: {str(report['team_rollout_ready']).lower()}",
        "",
        "## Required Decisions",
        "",
    ]
    lines.extend(f"- {decision}" for decision in report["required_decisions"])
    lines.extend(["", "## Checks", ""])
    lines.extend(f"- {check['status']}: {check['name']} - {check['detail']}" for check in report["checks"])
    lines.extend(["", "## Scopes", ""])
    for scope, scope_report in report["scopes"].items():
        lines.append(f"- {scope}: {scope_report['tool_count']} tools")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Static readiness audit for the Degen Ops MCP/chat rollout.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of Markdown.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_readiness_report(repo_root=args.repo_root)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(_render_markdown(report), end="")
    return 0 if report["code_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
