from __future__ import annotations

import argparse
import json
from typing import Any


AUDIENCES = ("owner", "partner", "employee")
CLIENTS = ("hermes", "codex")


def _option(
    *,
    option_id: str,
    label: str,
    recommended: bool,
    ready: bool,
    reasons: list[str],
    missing: list[str],
    verification_commands: list[str],
) -> dict[str, Any]:
    return {
        "id": option_id,
        "label": label,
        "recommended": recommended,
        "ready": ready,
        "reasons": reasons,
        "missing": missing,
        "verification_commands": verification_commands,
    }


def _scope_commands(*, audience: str, client: str, database_url_env: str) -> dict[str, list[str]]:
    live_commands = [
        (
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_mcp_smoke.py "
            f"--config {client} --scope {audience} --read-check --database-url-env {database_url_env}"
        ),
        (
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_live_data.py "
            f"--scope {audience} --database-url-env {database_url_env} --json"
        ),
    ]
    if audience in {"owner", "partner"}:
        live_commands.append(
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_pilot_demo.py "
                f"--scope {audience} --database-url-env {database_url_env} --json"
            )
        )
    return {
        "local_direct_db": live_commands,
        "green_hosted_client": [
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_readiness.py --json",
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_rollout_gate.py "
                f"--run-live --run-pilot --live-scope {audience} --pilot-scope "
                f"{'partner' if audience == 'employee' else audience} "
                f"--database-url-env {database_url_env} --json"
            ),
        ],
        "hosted_mcp_gateway": [
            "Do not run: hosted gateway needs separate auth, audit logging, deployment, and rollback approval.",
        ],
    }


def build_topology_plan(
    *,
    audience: str = "partner",
    client: str = "hermes",
    database_url_env: str = "DEGEN_OPS_READONLY_DATABASE_URL",
    direct_db_approved: bool = False,
    direct_db_verified: bool = False,
    green_session_approved: bool = False,
    green_live_read_verified: bool = False,
    gateway_approved: bool = False,
    gateway_built_verified: bool = False,
    prefer: str = "auto",
) -> dict[str, Any]:
    normalized_audience = str(audience or "").strip().lower()
    normalized_client = str(client or "").strip().lower()
    if normalized_audience not in AUDIENCES:
        raise ValueError(f"Unsupported audience {audience!r}. Use one of: {', '.join(AUDIENCES)}.")
    if normalized_client not in CLIENTS:
        raise ValueError(f"Unsupported client {client!r}. Use one of: {', '.join(CLIENTS)}.")

    commands = _scope_commands(
        audience=normalized_audience,
        client=normalized_client,
        database_url_env=database_url_env,
    )
    local_ready = direct_db_approved and direct_db_verified
    green_ready = green_session_approved and green_live_read_verified
    gateway_ready = gateway_approved and gateway_built_verified

    if prefer == "local":
        recommended_id = "local_direct_db"
    elif prefer == "green":
        recommended_id = "green_hosted_client"
    elif prefer == "gateway":
        recommended_id = "hosted_mcp_gateway"
    elif local_ready:
        recommended_id = "local_direct_db"
    elif green_ready:
        recommended_id = "green_hosted_client"
    else:
        recommended_id = "green_hosted_client"

    local_missing = []
    if not direct_db_approved:
        local_missing.append("direct_read_only_db_credential_approved")
    if not direct_db_verified:
        local_missing.append("direct_read_only_db_smoke_passed")

    green_missing = []
    if not green_session_approved:
        green_missing.append("green_hosted_session_boundaries_approved")
    if not green_live_read_verified:
        green_missing.append("green_live_read_gate_passed")

    gateway_missing = []
    if not gateway_approved:
        gateway_missing.append("hosted_gateway_security_and_deploy_plan_approved")
    if not gateway_built_verified:
        gateway_missing.append("hosted_gateway_built_and_verified")

    options = [
        _option(
            option_id="local_direct_db",
            label="Option A: local client with read-only DB URL",
            recommended=recommended_id == "local_direct_db",
            ready=local_ready,
            reasons=[
                "Best when each approved machine can safely hold a dedicated read-only DB credential.",
                "Keeps the MCP server local and simple.",
            ],
            missing=local_missing,
            verification_commands=commands["local_direct_db"],
        ),
        _option(
            option_id="green_hosted_client",
            label="Option B: Green-hosted chat client",
            recommended=recommended_id == "green_hosted_client",
            ready=green_ready,
            reasons=[
                "Recommended first team pilot path when laptop DB credentials are not approved.",
                "Keeps live database access near the existing production environment.",
            ],
            missing=green_missing,
            verification_commands=commands["green_hosted_client"],
        ),
        _option(
            option_id="hosted_mcp_gateway",
            label="Option C: hosted read-only MCP gateway",
            recommended=recommended_id == "hosted_mcp_gateway",
            ready=gateway_ready,
            reasons=[
                "Best long-term UX only after auth, audit logging, rate limits, deployment, and rollback are designed.",
                "Not appropriate for the first read-only pilot without separate approval.",
            ],
            missing=gateway_missing,
            verification_commands=commands["hosted_mcp_gateway"],
        ),
    ]
    recommended = next(option for option in options if option["recommended"])
    return {
        "name": "degen_ops_topology_plan",
        "audience": normalized_audience,
        "client": normalized_client,
        "database_url_env": database_url_env,
        "recommended_topology": recommended["id"],
        "ready_for_team_pilot": recommended["ready"],
        "required_before_pilot": recommended["missing"],
        "options": options,
        "read_only": True,
        "notes": [
            "This planner does not create credentials, edit production, restart services, or install client config.",
            "Owner scope is required for exact cash balances and owner loan/payback totals.",
            "Partner scope keeps the buy workflow but redacts raw cash and owner loan/payback details.",
        ],
    }


def _render_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Topology Plan",
        "",
        f"- audience: {plan['audience']}",
        f"- client: {plan['client']}",
        f"- recommended_topology: {plan['recommended_topology']}",
        f"- ready_for_team_pilot: {str(plan['ready_for_team_pilot']).lower()}",
        "",
        "## Required Before Pilot",
        "",
    ]
    required = plan["required_before_pilot"] or ["none"]
    lines.extend(f"- {item}" for item in required)
    lines.extend(["", "## Options", ""])
    for option in plan["options"]:
        lines.extend(
            [
                f"### {option['label']}",
                "",
                f"- recommended: {str(option['recommended']).lower()}",
                f"- ready: {str(option['ready']).lower()}",
                f"- missing: {', '.join(option['missing']) if option['missing'] else 'none'}",
                "",
                "Verification:",
                "",
            ]
        )
        lines.extend(f"- `{command}`" for command in option["verification_commands"])
        lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan the read-only Degen Ops team access topology.")
    parser.add_argument("--audience", choices=AUDIENCES, default="partner")
    parser.add_argument("--client", choices=CLIENTS, default="hermes")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    parser.add_argument("--direct-db-approved", action="store_true")
    parser.add_argument("--direct-db-verified", action="store_true")
    parser.add_argument("--green-session-approved", action="store_true")
    parser.add_argument("--green-live-read-verified", action="store_true")
    parser.add_argument("--gateway-approved", action="store_true")
    parser.add_argument("--gateway-built-verified", action="store_true")
    parser.add_argument("--prefer", choices=("auto", "local", "green", "gateway"), default="auto")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_topology_plan(
        audience=args.audience,
        client=args.client,
        database_url_env=args.database_url_env,
        direct_db_approved=args.direct_db_approved,
        direct_db_verified=args.direct_db_verified,
        green_session_approved=args.green_session_approved,
        green_live_read_verified=args.green_live_read_verified,
        gateway_approved=args.gateway_approved,
        gateway_built_verified=args.gateway_built_verified,
        prefer=args.prefer,
    )
    print(json.dumps(plan, indent=2, sort_keys=True) if args.json else _render_markdown(plan))
    return 0 if plan["ready_for_team_pilot"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
