from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]

COMPILE_TARGETS = [
    "app",
    "scripts/degen_ops_mcp.py",
    "scripts/degen_ops_mcp_smoke.py",
    "scripts/degen_ops_mcp_config.py",
    "scripts/degen_ops_answer_eval.py",
    "scripts/degen_ops_change_manifest.py",
    "scripts/degen_ops_chat.py",
    "scripts/degen_ops_completion_audit.py",
    "scripts/degen_ops_team_package.py",
    "scripts/degen_ops_readiness.py",
    "scripts/degen_ops_live_data.py",
    "scripts/degen_ops_approval_packet.py",
    "scripts/degen_ops_green_pilot_packet.py",
    "scripts/degen_ops_launch_checklist.py",
    "scripts/degen_ops_pilot_demo.py",
    "scripts/degen_ops_mvp_audit.py",
    "scripts/degen_ops_scope_audit.py",
    "scripts/degen_ops_rollout_gate.py",
    "scripts/degen_ops_topology_plan.py",
]

TEST_TARGETS = [
    "tests/test_degen_ops_rollout_gate.py",
    "tests/test_degen_ops_scope_audit.py",
    "tests/test_degen_ops_mvp_audit.py",
    "tests/test_degen_ops_completion_audit.py",
    "tests/test_degen_ops_answer_eval.py",
    "tests/test_degen_ops_change_manifest.py",
    "tests/test_degen_ops_pilot_demo.py",
    "tests/test_degen_ops_approval_packet.py",
    "tests/test_degen_ops_green_pilot_packet.py",
    "tests/test_degen_ops_launch_checklist.py",
    "tests/test_degen_ops_live_data.py",
    "tests/test_degen_ops_readiness.py",
    "tests/test_degen_ops_topology_plan.py",
    "tests/test_degen_ops_team_package.py",
    "tests/test_degen_ops_chat.py",
    "tests/test_ops_mcp.py",
    "tests/test_ops_agent.py",
    "tests/test_degen_ops_mcp_smoke.py",
    "tests/test_degen_ops_mcp_config.py",
    "tests/test_degen_ops_docs.py",
]

DIFF_CHECK_TARGETS = [
    "app/ops_agent.py",
    "app/ops_mcp.py",
    "app/ops_chat.py",
    "requirements.txt",
    *[target for target in COMPILE_TARGETS if target.startswith("scripts/")],
    "docs/ops",
    "tests/test_ops_agent.py",
    "tests/test_ops_mcp.py",
    "tests/test_degen_ops_*.py",
]


def _run_step(name: str, command: list[str], *, repo_root: Path, timeout: int) -> dict[str, Any]:
    result = subprocess.run(
        command,
        cwd=repo_root,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    step = {
        "name": name,
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "command": command,
        "stdout_tail": result.stdout.splitlines()[-20:],
        "stderr_tail": result.stderr.splitlines()[-20:],
    }
    parsed_summary = _parse_step_summary(name, result.stdout)
    if parsed_summary:
        step["summary"] = parsed_summary
    return step


def _parse_json_stdout(stdout: str) -> dict[str, Any] | None:
    text = stdout.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _parse_step_summary(name: str, stdout: str) -> dict[str, Any] | None:
    parsed = _parse_json_stdout(stdout)
    if not parsed:
        return None
    if name == "rollout_gate":
        completion = parsed.get("completion")
        pending = completion.get("pending") if isinstance(completion, dict) else None
        return {
            "static_ok": parsed.get("static_ok"),
            "team_live_rollout_ready": parsed.get("team_live_rollout_ready"),
            "goal_complete": completion.get("goal_complete") if isinstance(completion, dict) else None,
            "pending": pending if isinstance(pending, list) else [],
        }
    if name == "change_manifest":
        return {
            "intended_file_count": parsed.get("intended_file_count"),
            "unrelated_file_count": parsed.get("unrelated_file_count"),
            "safe_to_stage_intended_only": parsed.get("safe_to_stage_intended_only"),
            "stage_command_available": parsed.get("stage_command_available"),
        }
    return None


def _python(repo_root: Path) -> str:
    candidate = repo_root / ".venv" / "Scripts" / "python.exe"
    return str(candidate) if candidate.exists() else sys.executable


def build_local_gate_plan(*, repo_root: Path | str = REPO_ROOT) -> dict[str, Any]:
    root = Path(repo_root)
    py = _python(root)
    return {
        "compile": [py, "-m", "compileall", *COMPILE_TARGETS],
        "tests": [py, "-m", "pytest", *TEST_TARGETS, "-q"],
        "mcp_smoke": [py, "scripts/degen_ops_mcp_smoke.py", "--config", "both", "--read-check"],
        "rollout_gate": [py, "scripts/degen_ops_rollout_gate.py", "--json"],
        "change_manifest": [py, "scripts/degen_ops_change_manifest.py", "--summary", "--sample-limit", "5", "--json"],
        "diff_check": ["git", "diff", "--check", "--", *DIFF_CHECK_TARGETS],
    }


def run_local_gate(
    *,
    repo_root: Path | str = REPO_ROOT,
    timeout: int = 120,
    dry_run: bool = False,
) -> dict[str, Any]:
    root = Path(repo_root)
    plan = build_local_gate_plan(repo_root=root)
    if dry_run:
        return {
            "name": "degen_ops_local_gate",
            "ok": True,
            "dry_run": True,
            "steps": [
                {"name": name, "ok": True, "returncode": None, "command": command}
                for name, command in plan.items()
            ],
            "team_live_rollout_ready": False,
        }
    steps = [
        _run_step(name, command, repo_root=root, timeout=timeout)
        for name, command in plan.items()
    ]
    summaries = {
        step["name"]: step["summary"]
        for step in steps
        if isinstance(step.get("summary"), dict)
    }
    return {
        "name": "degen_ops_local_gate",
        "ok": all(step["ok"] for step in steps),
        "dry_run": False,
        "steps": steps,
        "summaries": summaries,
        "team_live_rollout_ready": False,
        "note": "Local gate is read-only and does not approve Green, credentials, production writes, or team access.",
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Local Gate",
        "",
        f"- ok: {str(report['ok']).lower()}",
        f"- dry_run: {str(report['dry_run']).lower()}",
        f"- team_live_rollout_ready: {str(report['team_live_rollout_ready']).lower()}",
        "",
    ]
    summaries = report.get("summaries") or {}
    rollout_summary = summaries.get("rollout_gate")
    if isinstance(rollout_summary, dict):
        lines.extend(
            [
                "## Rollout",
                "",
                f"- static_ok: {str(rollout_summary.get('static_ok')).lower()}",
                f"- goal_complete: {str(rollout_summary.get('goal_complete')).lower()}",
                f"- pending: {', '.join(rollout_summary.get('pending') or []) or 'none'}",
                "",
            ]
        )
    change_summary = summaries.get("change_manifest")
    if isinstance(change_summary, dict):
        lines.extend(
            [
                "## Change Manifest",
                "",
                f"- intended_file_count: {change_summary.get('intended_file_count')}",
                f"- unrelated_file_count: {change_summary.get('unrelated_file_count')}",
                f"- safe_to_stage_intended_only: {str(change_summary.get('safe_to_stage_intended_only')).lower()}",
                "",
            ]
        )
    lines.extend(
        [
        "## Steps",
        "",
        ]
    )
    lines.extend(f"- {'pass' if step['ok'] else 'fail'}: {step['name']}" for step in report["steps"])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local read-only Degen Ops CI gate.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_local_gate(repo_root=args.repo_root, timeout=args.timeout, dry_run=args.dry_run)
    print(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report), end="")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
