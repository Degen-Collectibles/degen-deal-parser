from __future__ import annotations

import argparse


AUDIENCES = ("owner", "partner", "employee")
CLIENTS = ("hermes", "codex")


def _pilot_scope_for(audience: str) -> str:
    return "partner" if audience == "employee" else audience


def build_green_pilot_packet(
    *,
    audience: str = "partner",
    client: str = "hermes",
    database_url_env: str = "DEGEN_OPS_READONLY_DATABASE_URL",
    app_dir: str = "/opt/degen/app",
    python_command: str = "python",
) -> str:
    normalized_audience = str(audience or "").strip().lower()
    normalized_client = str(client or "").strip().lower()
    if normalized_audience not in AUDIENCES:
        raise ValueError(f"Unsupported audience {audience!r}. Use one of: {', '.join(AUDIENCES)}.")
    if normalized_client not in CLIENTS:
        raise ValueError(f"Unsupported client {client!r}. Use one of: {', '.join(CLIENTS)}.")

    pilot_scope = _pilot_scope_for(normalized_audience)
    approval_phrase = f"proceed with Green-hosted Degen Ops pilot for {normalized_audience} via {normalized_client}"
    app_dir = str(app_dir or "/opt/degen/app").strip() or "/opt/degen/app"
    python_command = str(python_command or "python").strip() or "python"
    live_data_command = (
        f"{python_command} scripts/degen_ops_live_data.py "
        f"--scope {normalized_audience} --database-url-env {database_url_env} --json"
    )
    pilot_demo_command = (
        f"{python_command} scripts/degen_ops_pilot_demo.py "
        f"--scope {pilot_scope} --database-url-env {database_url_env} --json"
    )
    return "\n".join(
        [
            "# Degen Ops Green-Hosted Pilot Packet",
            "",
            "This packet is the approval target for a first team pilot where the chat client runs near Green/Brev instead of placing live database credentials on laptops.",
            "",
            "## Target",
            "",
            "- topology: green_hosted_client",
            f"- audience: {normalized_audience}",
            f"- client: {normalized_client}",
            f"- mcp_scope: {normalized_audience}",
            f"- database_url_env: {database_url_env}",
            f"- app_dir: {app_dir}",
            "",
            "## Required Explicit Approval",
            "",
            f"Jeffrey must say: `{approval_phrase}`",
            "",
            "Do not treat this packet, a passing local test, or a passing static rollout gate as approval.",
            "",
            "## Hard Boundaries",
            "",
            "- Read-only inspection only",
            "- No production writes",
            "- No service restarts",
            "- No database migrations",
            "- No money movement",
            "- No inventory changes",
            "- No Shopify, TikTok, Discord, banking, payroll, customer, employee, or partner messages",
            "- No direct live database credential copied into employee or partner laptop config",
            "- No raw database URL printed into docs, tickets, screenshots, or chat",
            "",
            "## Session Boundaries",
            "",
            "- Run from an approved Green/Brev shell or approved Green-hosted client session.",
            "- Use the chosen scoped MCP config only; do not copy owner scope for partner or employee pilots.",
            "- Set `LOG_TO_FILE=false` for one-off verification commands.",
            f"- Store the live database URL only in `{database_url_env}` or the approved Green environment source.",
            "- Keep any temp scripts or shells disposable; do not edit `/opt/degen/app` from the pilot session.",
            "",
            "## Verification Commands",
            "",
            "Run these from the approved Green-hosted environment after approval:",
            "",
            "```bash",
            f"cd {app_dir}",
            "export LOG_TO_FILE=false",
            f": \"${{{database_url_env}:?Set {database_url_env} from the approved Green environment source first}}\"",
            f"{python_command} scripts/degen_ops_readiness.py --json",
            live_data_command,
            pilot_demo_command,
            (
                f"{python_command} scripts/degen_ops_rollout_gate.py "
                f"--run-live --run-pilot --live-scope {normalized_audience} "
                f"--pilot-scope {pilot_scope} --database-url-env {database_url_env} --json"
            ),
            (
                f"{python_command} scripts/degen_ops_topology_plan.py "
                f"--audience {normalized_audience} --client {normalized_client} "
                "--green-session-approved --green-live-read-verified --json"
            ),
            "```",
            "",
            "For owner or partner pilots, the `degen_ops_pilot_demo.py` command confirms the no-LLM buy workflow. For employee pilots, it runs the redacted partner buy workflow separately while `degen_ops_live_data.py --scope employee` verifies the employee MCP surface.",
            "",
            "## Success Criteria",
            "",
            "- The topology planner reports `ready_for_team_pilot: true` for `green_hosted_client`.",
            "- The rollout gate static checks pass.",
            "- The live-data verifier passes for the target audience.",
            "- Partner scope shows 6 tools and redacts raw cash/owner loan details.",
            "- Employee scope shows exactly 3 tools.",
            "- No command output contains a raw database URL.",
            "",
            "## Rollback",
            "",
            "- Close the Green-hosted chat or shell session.",
            f"- Remove or unset `{database_url_env}` from the session.",
            "- Delete disposable temp scripts created only for the pilot.",
            "- Remove scoped MCP config from the client if one was installed.",
            "- Restore the timestamped config backup if a local config was edited.",
            "- Revoke any temporary read-only credential if one was issued.",
            "",
            "## Not Included",
            "",
            "- This packet does not create a credential.",
            "- This packet does not start a service.",
            "- This packet does not edit production.",
            "- This packet does not approve a hosted MCP gateway.",
            "",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render the Degen Ops Green-hosted pilot approval packet.")
    parser.add_argument("--audience", choices=AUDIENCES, default="partner")
    parser.add_argument("--client", choices=CLIENTS, default="hermes")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    parser.add_argument("--app-dir", default="/opt/degen/app")
    parser.add_argument("--python-command", default="python")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(
        build_green_pilot_packet(
            audience=args.audience,
            client=args.client,
            database_url_env=args.database_url_env,
            app_dir=args.app_dir,
            python_command=args.python_command,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
