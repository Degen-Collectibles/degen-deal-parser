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

from app.ops_mcp import DEGEN_OPS_SCOPE_TOOL_NAMES, _normalize_scope
from scripts.degen_ops_team_package import build_team_package


FORBIDDEN_EMPLOYEE_TOOLS = {
    "get_finance_snapshot",
    "get_cash_snapshot",
    "get_loan_and_payback_snapshot",
    "evaluate_inventory_buy",
    "generate_partner_update",
}

FORBIDDEN_PARTNER_TOOLS = {
    "get_cash_snapshot",
    "get_loan_and_payback_snapshot",
}


def _scope_rows() -> dict[str, dict[str, Any]]:
    rows = {}
    for scope, tools in DEGEN_OPS_SCOPE_TOOL_NAMES.items():
        sorted_tools = sorted(tools)
        forbidden = []
        if scope == "employee":
            forbidden = sorted(set(sorted_tools) & FORBIDDEN_EMPLOYEE_TOOLS)
        elif scope == "partner":
            forbidden = sorted(set(sorted_tools) & FORBIDDEN_PARTNER_TOOLS)
        rows[scope] = {
            "status": "fail" if forbidden else "pass",
            "tool_count": len(sorted_tools),
            "tools": sorted_tools,
            "forbidden_tools_present": forbidden,
        }
    return rows


def _contains_raw_database_url(text: str) -> bool:
    lowered = text.lower()
    return "postgresql://" in lowered or "postgresql+psycopg://" in lowered


def _employee_package_check(*, database_url_env: str) -> dict[str, Any]:
    package = build_team_package(
        scope="employee",
        clients=["hermes"],
        database_url_env=database_url_env,
    )
    contains_forbidden = any(tool in package for tool in FORBIDDEN_EMPLOYEE_TOOLS)
    check = {
        "contains_database_env_reference": database_url_env in package,
        "contains_raw_database_url": _contains_raw_database_url(package),
        "contains_owner_scope": "degen_ops_owner" in package,
        "contains_partner_scope": "degen_ops_partner" in package,
        "contains_forbidden_tools": contains_forbidden,
    }
    check["status"] = "pass" if (
        check["contains_database_env_reference"]
        and not check["contains_raw_database_url"]
        and not check["contains_owner_scope"]
        and not check["contains_partner_scope"]
        and not check["contains_forbidden_tools"]
    ) else "fail"
    return check


def _partner_package_check(*, database_url_env: str) -> dict[str, Any]:
    package = build_team_package(
        scope="partner",
        clients=["hermes"],
        database_url_env=database_url_env,
    )
    contains_owner_only_tools = any(tool in package for tool in FORBIDDEN_PARTNER_TOOLS)
    check = {
        "contains_database_env_reference": database_url_env in package,
        "contains_raw_database_url": _contains_raw_database_url(package),
        "contains_owner_scope": "degen_ops_owner" in package,
        "contains_owner_only_tools": contains_owner_only_tools,
        "contains_buy_workflow": "evaluate_inventory_buy" in package and "generate_partner_update" in package,
    }
    check["status"] = "pass" if (
        check["contains_database_env_reference"]
        and not check["contains_raw_database_url"]
        and not check["contains_owner_scope"]
        and not check["contains_owner_only_tools"]
        and check["contains_buy_workflow"]
    ) else "fail"
    return check


@contextmanager
def _without_env_var(name: str):
    existing = os.environ.pop(name, None)
    try:
        yield
    finally:
        if existing is not None:
            os.environ[name] = existing


def _default_scope_check() -> dict[str, Any]:
    with _without_env_var("DEGEN_OPS_MCP_SCOPE"):
        default_scope = _normalize_scope(None)
    return {
        "status": "pass" if default_scope == "employee" else "fail",
        "default_scope": default_scope,
        "expected_scope": "employee",
    }


def build_scope_audit(*, database_url_env: str = "DEGEN_OPS_READONLY_DATABASE_URL") -> dict[str, Any]:
    scopes = _scope_rows()
    package_checks = {
        "employee": _employee_package_check(database_url_env=database_url_env),
        "partner": _partner_package_check(database_url_env=database_url_env),
    }
    default_scope_check = _default_scope_check()
    ok = all(row["status"] == "pass" for row in scopes.values()) and all(
        row["status"] == "pass" for row in package_checks.values()
    ) and default_scope_check["status"] == "pass"
    return {
        "name": "degen_ops_scope_audit",
        "ok": ok,
        "database_url_env": database_url_env,
        "scopes": scopes,
        "package_checks": package_checks,
        "default_scope_check": default_scope_check,
    }


def _render_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Scope Audit",
        "",
        f"- ok: {str(audit['ok']).lower()}",
        f"- database_url_env: {audit['database_url_env']}",
        "",
        "## Scopes",
        "",
    ]
    for scope, row in audit["scopes"].items():
        lines.append(f"- {row['status']}: {scope} ({row['tool_count']} tools)")
    lines.extend(["", "## Package Checks", ""])
    for scope, row in audit["package_checks"].items():
        lines.append(f"- {row['status']}: {scope} package")
    lines.extend(["", "## Default Scope", ""])
    default_scope = audit["default_scope_check"]
    lines.append(f"- {default_scope['status']}: missing scope defaults to {default_scope['default_scope']}")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Degen Ops MCP scope boundaries.")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit = build_scope_audit(database_url_env=args.database_url_env)
    if args.json:
        print(json.dumps(audit, indent=2, sort_keys=True))
    else:
        print(_render_markdown(audit), end="")
    return 0 if audit["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
