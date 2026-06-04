from __future__ import annotations

import argparse


def build_approval_packet(*, database_url_env: str = "DEGEN_OPS_READONLY_DATABASE_URL") -> str:
    return "\n".join(
        [
            "# Degen Ops Live-Data Approval Packet",
            "",
            "This packet is for choosing how partners and employees can talk to the read-only Degen Ops agent with current data.",
            "",
            "Requires explicit proceed before implementation of any live-data topology.",
            "",
            "## Hard Constraints",
            "",
            "- No production writes",
            "- No database migrations",
            "- No service restarts",
            "- No money movement",
            "- No inventory changes",
            "- No customer or partner messages without approval",
            "- Do not build Option C until auth, audit logging, rate limits, deployment, and rollback are approved.",
            "",
            "## Options",
            "",
            "### Option A: Local Clients With Read-Only DB URL",
            "",
            "Install the scoped MCP/chat config on approved machines and store the live read-only DB URL in an environment variable.",
            "",
            "Pros: simple, no new hosted service, easy to verify with the existing smoke scripts.",
            "Cons: each machine needs credential handling and network reachability.",
            "",
            "### Option B: Green-Hosted Chat Client",
            "",
            "Run the chat client from the Green/Brev environment that can already reach the production database.",
            "",
            "Pros: avoids direct DB credentials on laptops; closest to existing production network access.",
            "Cons: less ergonomic and still needs account/session boundaries.",
            "",
            "### Option C: Hosted Read-Only MCP Gateway",
            "",
            "Build a production-adjacent remote MCP service with authentication, audit logging, per-scope authorization, and revocation.",
            "",
            "Pros: best team UX and centralized revocation.",
            "Cons: new production-facing service. Do not build Option C without explicit approval.",
            "",
            "## Verification Commands",
            "",
            "Run the planner before approval, then run the verification commands after the chosen topology has a read-only credential or Green-hosted environment:",
            "",
            "```powershell",
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_topology_plan.py --audience partner --client hermes --json",
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_green_pilot_packet.py --audience partner --client hermes",
            f"$env:{database_url_env} = \"read-only DB URL from secret manager\"",
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_readiness.py --json",
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_live_data.py --scope employee "
                f"--database-url-env {database_url_env}"
            ),
            (
                ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_live_data.py --scope partner "
                f"--database-url-env {database_url_env} --json"
            ),
            "```",
            "",
            "## Success Criteria",
            "",
            "- employee scope exposes exactly 3 tools: manifest, inventory snapshot, and channel velocity",
            "- partner scope excludes raw cash and owner loan/payback tools while retaining buy evaluation and update drafting",
            "- missing or blank scope defaults to employee tools, not owner tools",
            "- partner or owner scope can answer the buy-decision workflow with evidence",
            "- live-data verifier passes for the target scope",
            "- terminal chat preflight passes before a human starts chatting",
            "- no raw database URL appears in generated packages, docs, tickets, or screenshots",
            "",
            "## Rollback",
            "",
            "- Remove scoped MCP config from the client",
            "- Remove local environment variables containing DB credentials",
            "- Revoke the read-only credential",
            "- If a Green-hosted session was used, close that session",
            "- If a future gateway is built, stop the gateway service and revoke its credential",
            "",
            "## Approval Needed",
            "",
            "- Choose Option A, Option B, or Option C",
            "- Use owner scope for any partner only if owner-level cash, loan, and payback visibility is explicitly approved",
            "- Approve the read-only credential source or Green-hosted access pattern",
            "- Say `proceed` only after reviewing the chosen target, rollback path, and verification commands",
            "",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render the Degen Ops live-data approval packet.")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(build_approval_packet(database_url_env=args.database_url_env))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
