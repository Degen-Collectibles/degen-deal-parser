from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]


def _contains(repo_root: Path, path: str, *needles: str) -> bool:
    candidate = repo_root / path
    if not candidate.exists():
        return False
    text = candidate.read_text(encoding="utf-8")
    return all(needle in text for needle in needles)


def _requirement(
    *,
    item_id: str,
    label: str,
    status: str,
    evidence: list[str],
    remaining: list[str] | None = None,
    note: str = "",
) -> dict[str, Any]:
    return {
        "id": item_id,
        "label": label,
        "status": status,
        "evidence": evidence,
        "remaining": remaining or [],
        "note": note,
    }


def build_completion_audit(*, repo_root: Path | str = REPO_ROOT) -> dict[str, Any]:
    root = Path(repo_root)
    requirements = [
        _requirement(
            item_id="chatbot_surface",
            label="No-GUI chatbot can talk to the Degen Ops harness",
            status=(
                "satisfied"
                if _contains(root, "app/ops_chat.py", "run_chat_turn", "DegenOpsChatToolRunner")
                and _contains(root, "scripts/degen_ops_chat.py", "--prompt", "--preflight")
                else "missing"
            ),
            evidence=["app/ops_chat.py", "scripts/degen_ops_chat.py"],
        ),
        _requirement(
            item_id="mcp_surface",
            label="MCP server exposes bounded Degen Ops tools for Codex/Hermes clients",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "register_degen_ops_tools", "create_mcp_server")
                and _contains(root, "scripts/degen_ops_mcp.py", "from app.ops_mcp import main")
                else "missing"
            ),
            evidence=["app/ops_mcp.py", "scripts/degen_ops_mcp.py"],
        ),
        _requirement(
            item_id="scoped_access",
            label="Owner, partner, and employee scopes are explicit and fail safe",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "DEGEN_OPS_SCOPE_TOOL_NAMES", "employee", "partner", "owner")
                and _contains(root, "tests/test_ops_mcp.py", "missing_scope_defaults_to_employee")
                else "missing"
            ),
            evidence=["app/ops_mcp.py:DEGEN_OPS_SCOPE_TOOL_NAMES", "tests/test_ops_mcp.py"],
        ),
        _requirement(
            item_id="partner_redaction",
            label="Partner scope can evaluate buys without raw cash or owner loan/payback exposure",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "_redact_partner_recommendation", "owner_scope_required_for")
                and _contains(root, "tests/test_ops_mcp.py", "partner_evaluate_redacts_owner_cash")
                else "missing"
            ),
            evidence=["app/ops_mcp.py:_redact_partner_recommendation", "tests/test_ops_mcp.py"],
        ),
        _requirement(
            item_id="read_only_guardrails",
            label="Harness stays read-only and has no money, inventory, production, or messaging writes",
            status=(
                "satisfied"
                if _contains(root, "app/ops_mcp.py", "SET TRANSACTION READ ONLY", "PRAGMA query_only = ON")
                and _contains(root, "docs/ops/degen-ops-agent-instructions.md", "You are not an autonomous actor")
                else "missing"
            ),
            evidence=["app/ops_mcp.py:_apply_session_read_only", "docs/ops/degen-ops-agent-instructions.md"],
        ),
        _requirement(
            item_id="buy_decision_workflow",
            label="Can answer buy decision, sell-through, routing, payback, and cash-risk questions with evidence",
            status=(
                "satisfied"
                if _contains(root, "app/ops_agent.py", "_build_sell_through", "_build_payback_plan", "_build_risk_flags")
                and _contains(root, "scripts/degen_ops_pilot_demo.py", "evidence", "routing", "payback_plan")
                else "missing"
            ),
            evidence=["app/ops_agent.py", "scripts/degen_ops_pilot_demo.py"],
        ),
        _requirement(
            item_id="nvidia_openai_compatible_llm",
            label="Chatbot uses the existing OpenAI-compatible provider path, including NVIDIA",
            status=(
                "satisfied"
                if _contains(root, "scripts/degen_ops_chat.py", "get_provider", "get_fast_model", "Set NVIDIA_API_KEY")
                and _contains(root, "app/ai_client.py", "AI_PROVIDER", "NVIDIA_API_KEY", "OpenAI-compatible")
                and _contains(root, "app/ops_chat.py", "chat.completions.create")
                and _contains(root, "docs/ops/degen-ops-hermes-mcp-pilot.md", "AI_PROVIDER", "NVIDIA_API_KEY")
                else "missing"
            ),
            evidence=[
                "scripts/degen_ops_chat.py",
                "app/ai_client.py",
                "app/ops_chat.py",
                "docs/ops/degen-ops-hermes-mcp-pilot.md",
            ],
        ),
        _requirement(
            item_id="hermes_codex_config_and_smoke",
            label="Hermes and Codex configs can be generated and smoke-tested by scope",
            status=(
                "satisfied"
                if _contains(root, "scripts/degen_ops_mcp_config.py", "hermes", "codex")
                and _contains(root, "scripts/degen_ops_mcp_smoke.py", "boundary_ok", "read_check")
                else "missing"
            ),
            evidence=["scripts/degen_ops_mcp_config.py", "scripts/degen_ops_mcp_smoke.py"],
        ),
        _requirement(
            item_id="green_pilot_handoff",
            label="Green-hosted partner pilot has an approval packet and launch checklist",
            status=(
                "satisfied"
                if _contains(root, "scripts/degen_ops_green_pilot_packet.py", "proceed with Green-hosted", "cd {app_dir}")
                and _contains(root, "scripts/degen_ops_launch_checklist.py", "after_approval_green_commands")
                else "missing"
            ),
            evidence=["scripts/degen_ops_green_pilot_packet.py", "scripts/degen_ops_launch_checklist.py"],
        ),
        _requirement(
            item_id="answer_quality_eval",
            label="Scoped chatbot answers can be evaluated for evidence, routing, risk, and redaction markers",
            status=(
                "satisfied"
                if _contains(root, "scripts/degen_ops_answer_eval.py", "partner_buy_decision", "forbidden_markers")
                and _contains(root, "tests/test_degen_ops_answer_eval.py", "cash balance is $")
                else "missing"
            ),
            evidence=["scripts/degen_ops_answer_eval.py", "tests/test_degen_ops_answer_eval.py"],
        ),
        _requirement(
            item_id="change_manifest",
            label="Mixed worktree can be separated into intended Degen Ops files and unrelated changes before staging",
            status=(
                "satisfied"
                if _contains(root, "scripts/degen_ops_change_manifest.py", "safe_to_stage_intended_only", "avoid broad staging")
                and _contains(root, "tests/test_degen_ops_change_manifest.py", "outputs/generated.log")
                else "missing"
            ),
            evidence=["scripts/degen_ops_change_manifest.py", "tests/test_degen_ops_change_manifest.py"],
        ),
        _requirement(
            item_id="local_gate",
            label="Local compile, focused tests, MCP smoke, rollout gate, manifest, and diff checks can run from one command",
            status=(
                "satisfied"
                if _contains(root, "scripts/degen_ops_local_gate.py", "degen_ops_local_gate", "compileall", "degen_ops_mcp_smoke.py")
                and _contains(root, "tests/test_degen_ops_local_gate.py", "dry_run")
                else "missing"
            ),
            evidence=["scripts/degen_ops_local_gate.py", "tests/test_degen_ops_local_gate.py"],
        ),
        _requirement(
            item_id="team_live_rollout",
            label="Partners and employees can use live data safely",
            status="pending_decision",
            evidence=[
                "scripts/degen_ops_live_data.py",
                "scripts/degen_ops_rollout_gate.py",
                "scripts/degen_ops_launch_checklist.py",
            ],
            remaining=[
                "Jeffrey explicit approval phrase",
                "approved Green-hosted session or read-only DB credential",
                "live-data verifier pass from the approved environment",
                "rollout gate pass with --run-live --run-pilot",
            ],
            note="This is deliberately not considered complete from local static evidence alone.",
        ),
    ]
    missing = [item for item in requirements if item["status"] == "missing"]
    pending = [item for item in requirements if item["status"] == "pending_decision"]
    return {
        "name": "degen_ops_completion_audit",
        "objective": "Degen Ops chatbot plus MCP harness with a Green/Hermes partner pilot path",
        "code_ready": not missing,
        "goal_complete": not missing and not pending,
        "team_live_rollout_ready": False,
        "requirements": requirements,
        "missing": [item["id"] for item in missing],
        "pending": [item["id"] for item in pending],
        "next_approval_phrase": "proceed with Green-hosted Degen Ops pilot for partner via hermes",
    }


def _render_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Completion Audit",
        "",
        f"- code_ready: {str(audit['code_ready']).lower()}",
        f"- goal_complete: {str(audit['goal_complete']).lower()}",
        f"- team_live_rollout_ready: {str(audit['team_live_rollout_ready']).lower()}",
        f"- next_approval_phrase: `{audit['next_approval_phrase']}`",
        "",
        "## Requirements",
        "",
    ]
    lines.extend(f"- {item['status']}: {item['label']}" for item in audit["requirements"])
    lines.extend(["", "## Pending", ""])
    lines.extend(f"- {item}" for item in audit["pending"] or ["none"])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit the full Degen Ops chatbot/MCP goal against current repo evidence.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit = build_completion_audit(repo_root=args.repo_root)
    print(json.dumps(audit, indent=2, sort_keys=True) if args.json else _render_markdown(audit), end="")
    return 0 if audit["code_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
