from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["LOG_TO_FILE"] = "false"

from app.ops_mcp import DEGEN_OPS_SCOPE_TOOL_NAMES, _normalize_scope


AUDIENCES = {
    "owner": "Jeffrey / owners",
    "partner": "business partners",
    "employee": "staff / stream operators",
}

GUARDRAILS = [
    "No money movement",
    "No inventory changes",
    "No production writes",
    "No customer or partner messages without approval",
    "Draft only until a human approves any external communication",
]


def _normalize_clients(clients: Iterable[str]) -> list[str]:
    normalized = []
    for client in clients:
        value = str(client or "").strip().lower()
        if not value:
            continue
        if value not in {"hermes", "codex"}:
            raise ValueError(f"Unsupported Degen Ops client {value!r}. Use hermes or codex.")
        if value not in normalized:
            normalized.append(value)
    return normalized or ["hermes"]


def build_team_package(
    *,
    scope: str,
    clients: Iterable[str],
    database_url_env: str = "DEGEN_OPS_READONLY_DATABASE_URL",
) -> str:
    normalized_scope = _normalize_scope(scope)
    normalized_clients = _normalize_clients(clients)
    server_name = f"degen_ops_{normalized_scope}"
    tools = sorted(DEGEN_OPS_SCOPE_TOOL_NAMES[normalized_scope])
    lines = [
        f"# Degen Ops Access Package: {normalized_scope}",
        "",
        f"scope: {normalized_scope}",
        f"audience: {AUDIENCES[normalized_scope]}",
        f"server: {server_name}",
        "",
        "## Guardrails",
        "",
    ]
    lines.extend(f"- {guardrail}" for guardrail in GUARDRAILS)
    lines.extend(
        [
            "",
            "## Expected Tools",
            "",
        ]
    )
    lines.extend(f"- {tool}" for tool in tools)
    if normalized_scope == "partner":
        lines.extend(
            [
                "",
                "Partner redaction:",
                "",
                "- Raw cash balances and owner loan/payback snapshots are owner-scope only.",
                "- Buy evaluations and partner updates may report cash-safety status without exact cash balances.",
            ]
        )
    lines.extend(
        [
            "",
            "## Environment",
            "",
            f"- Store the read-only database URL in `{database_url_env}`.",
            "- Do not paste a raw database URL into chat, docs, screenshots, or tickets.",
            "- Use a dedicated read-only DB role or replica for non-owner machines.",
            "",
            "```powershell",
            f"$env:{database_url_env} = \"postgres URL from secret manager\"",
            "```",
            "",
            "## Config Snippets",
            "",
        ]
    )
    for client in normalized_clients:
        lines.extend(
            [
                f"### {client.title()}",
                "",
                "```powershell",
                (
                    ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_mcp_config.py "
                    f"--client {client} --scope {normalized_scope} "
                    f"--database-url-env {database_url_env}"
                ),
                "```",
                "",
            ]
        )
    lines.extend(
        [
            "## Preflight",
            "",
            "```powershell",
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_chat.py "
                f"--scope {normalized_scope} --preflight --read-check "
                f"--database-url-env {database_url_env}"
            ),
            "```",
            "",
            "## Chat",
            "",
            "```powershell",
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_chat.py "
                f"--scope {normalized_scope} --database-url-env {database_url_env}"
            ),
            "```",
            "",
            "One-shot prompt smoke:",
            "",
            "```powershell",
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_chat.py "
                f"--scope {normalized_scope} --prompt \"What evidence can you use here?\" "
                f"--database-url-env {database_url_env}"
            ),
            "```",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a scoped Degen Ops team access package.")
    parser.add_argument("--scope", choices=("owner", "partner", "employee"), required=True)
    parser.add_argument(
        "--client",
        action="append",
        choices=("hermes", "codex"),
        help="Client to include. Repeat for multiple clients. Defaults to hermes.",
    )
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(
        build_team_package(
            scope=args.scope,
            clients=args.client or ["hermes"],
            database_url_env=args.database_url_env,
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
