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
from scripts.degen_ops_readiness import build_readiness_report
from scripts.degen_ops_topology_plan import build_topology_plan


CLIENTS = ("hermes", "codex")


def _normalize_client(client: str) -> str:
    normalized = str(client or "").strip().lower()
    if normalized not in CLIENTS:
        raise ValueError(f"Unsupported client {client!r}. Use one of: {', '.join(CLIENTS)}.")
    return normalized


def _approval_phrase(*, audience: str, client: str) -> str:
    return f"proceed with Green-hosted Degen Ops pilot for {audience} via {client}"


def _green_commands(
    *,
    audience: str,
    client: str,
    database_url_env: str,
    app_dir: str,
    python_command: str,
) -> list[str]:
    pilot_scope = "partner" if audience == "employee" else audience
    return [
        f"cd {app_dir}",
        "export LOG_TO_FILE=false",
        f": \"${{{database_url_env}:?Set {database_url_env} from the approved Green environment source first}}\"",
        f"{python_command} scripts/degen_ops_readiness.py --json",
        f"{python_command} scripts/degen_ops_live_data.py --scope {audience} --database-url-env {database_url_env} --json",
        f"{python_command} scripts/degen_ops_pilot_demo.py --scope {pilot_scope} --database-url-env {database_url_env} --json",
        (
            f"{python_command} scripts/degen_ops_rollout_gate.py --run-live --run-pilot "
            f"--live-scope {audience} --pilot-scope {pilot_scope} --database-url-env {database_url_env} --json"
        ),
        (
            f"{python_command} scripts/degen_ops_topology_plan.py --audience {audience} --client {client} "
            "--green-session-approved --green-live-read-verified --json"
        ),
    ]


def build_launch_checklist(
    *,
    audience: str = "partner",
    client: str = "hermes",
    database_url_env: str = "DEGEN_OPS_READONLY_DATABASE_URL",
    app_dir: str = "/opt/degen/app",
    python_command: str = "python",
    repo_root: Path | str = REPO_ROOT,
) -> dict[str, Any]:
    normalized_audience = _normalize_scope(audience)
    normalized_client = _normalize_client(client)
    readiness = build_readiness_report(repo_root=repo_root)
    topology = build_topology_plan(
        audience=normalized_audience,
        client=normalized_client,
        database_url_env=database_url_env,
    )
    tools = sorted(DEGEN_OPS_SCOPE_TOOL_NAMES[normalized_audience])
    owner_only_tools = ["get_cash_snapshot", "get_loan_and_payback_snapshot"]
    return {
        "name": "degen_ops_launch_checklist",
        "audience": normalized_audience,
        "client": normalized_client,
        "recommended_topology": topology["recommended_topology"],
        "approval_phrase": _approval_phrase(audience=normalized_audience, client=normalized_client),
        "pre_approval_ready": bool(readiness["code_ready"] and topology["recommended_topology"] == "green_hosted_client"),
        "team_live_rollout_ready": False,
        "reason_team_rollout_not_ready": (
            "This checklist is pre-approval evidence only. Green live-data access still requires Jeffrey's "
            "explicit approval phrase, then live verification from the approved Green environment."
        ),
        "scope": {
            "tool_count": len(tools),
            "tools": tools,
            "owner_only_tools_hidden": [tool for tool in owner_only_tools if tool not in tools],
        },
        "must_not_do_before_approval": [
            "Do not create or copy live database credentials.",
            "Do not start a Green-hosted chat session.",
            "Do not edit /opt/degen/app.",
            "Do not restart production services.",
            "Do not send customer, employee, or partner messages.",
            "Do not use owner scope for partner or employee pilots.",
        ],
        "local_preapproval_commands": [
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_readiness.py --json",
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_rollout_gate.py --json",
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_green_pilot_packet.py "
                f"--audience {normalized_audience} --client {normalized_client}"
            ),
        ],
        "after_approval_green_commands": _green_commands(
            audience=normalized_audience,
            client=normalized_client,
            database_url_env=database_url_env,
            app_dir=str(app_dir or "/opt/degen/app").strip() or "/opt/degen/app",
            python_command=str(python_command or "python").strip() or "python",
        ),
        "readiness": {
            "code_ready": readiness["code_ready"],
            "checks": readiness["checks"],
        },
    }


def _render_markdown(checklist: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Launch Checklist",
        "",
        f"- audience: {checklist['audience']}",
        f"- client: {checklist['client']}",
        f"- recommended_topology: {checklist['recommended_topology']}",
        f"- pre_approval_ready: {str(checklist['pre_approval_ready']).lower()}",
        f"- team_live_rollout_ready: {str(checklist['team_live_rollout_ready']).lower()}",
        f"- approval_phrase: `{checklist['approval_phrase']}`",
        "",
        "## Scope",
        "",
        f"- tool_count: {checklist['scope']['tool_count']}",
        "",
        "Tools:",
        "",
    ]
    lines.extend(f"- {tool}" for tool in checklist["scope"]["tools"])
    lines.extend(["", "## Do Not Do Before Approval", ""])
    lines.extend(f"- {item}" for item in checklist["must_not_do_before_approval"])
    lines.extend(["", "## Local Pre-Approval Commands", "", "```powershell"])
    lines.extend(checklist["local_preapproval_commands"])
    lines.extend(["```", "", "## After-Approval Green Commands", "", "```bash"])
    lines.extend(checklist["after_approval_green_commands"])
    lines.extend(["```", ""])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a pre-approval Degen Ops Green/Hermes launch checklist.")
    parser.add_argument("--audience", choices=("owner", "partner", "employee"), default="partner")
    parser.add_argument("--client", choices=CLIENTS, default="hermes")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    parser.add_argument("--app-dir", default="/opt/degen/app")
    parser.add_argument("--python-command", default="python")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    checklist = build_launch_checklist(
        audience=args.audience,
        client=args.client,
        database_url_env=args.database_url_env,
        app_dir=args.app_dir,
        python_command=args.python_command,
    )
    if args.json:
        print(json.dumps(checklist, indent=2, sort_keys=True))
    else:
        print(_render_markdown(checklist), end="")
    return 0 if checklist["pre_approval_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
