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

from scripts.degen_ops_change_manifest import build_change_manifest


APPROVAL_PHRASE = "proceed with Degen Ops bot commit and Green Discord rollout"


def build_deploy_preflight(
    *,
    repo_root: Path | str = REPO_ROOT,
    changed_paths: list[str] | None = None,
) -> dict[str, Any]:
    root = Path(repo_root)
    manifest = build_change_manifest(repo_root=root, changed_paths=changed_paths)
    return {
        "name": "degen_ops_deploy_preflight",
        "read_only": True,
        "performs_changes": False,
        "requires_explicit_approval": True,
        "approval_phrase": APPROVAL_PHRASE,
        "targets": {
            "repository": "https://github.com/Degen-Collectibles/degen-deal-parser.git",
            "production_host": "Green/Brev openclaw-9902ae",
            "production_app_dir": "/opt/degen/app",
            "discord_surface": "Degen Ops Bot",
        },
        "intended_files": manifest.get("intended_files", []),
        "known_unrelated_files": manifest.get("unrelated_files", []),
        "externally_visible_changes": [
            "commit/push to origin/main or reviewed branch",
            "Green deploy of updated Degen Ops MCP/chat/bot code",
            "restart Degen Ops Discord bot service",
            "live Discord smoke replies in the configured Degen Ops channel",
        ],
        "not_in_scope": [
            "money movement",
            "inventory mutation",
            "Shopify/TikTok listing changes",
            "customer or partner messages without approval",
            "broad staging of unrelated bookkeeping/ledger/shared changes",
        ],
        "pre_deploy_checks": [
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_local_gate.py --json",
            ".\\.venv\\Scripts\\python.exe scripts\\degen_ops_change_manifest.py --summary --json",
            "git diff --check over intended Degen Ops files",
        ],
        "post_deploy_checks": [
            "Verify deployed commit on Green matches the intended commit.",
            "Run scripts/degen_ops_live_data.py against DEGEN_OPS_READONLY_DATABASE_URL for the approved scope.",
            "Run scripts/degen_ops_rollout_gate.py --run-live --run-pilot from the approved environment.",
            "Discord smoke prompts: TikTok 151 sales, price/trend lookup, weekly partner update draft, and follow-up context.",
            "Confirm audit log entries were written and no money/inventory/listing/customer-message mutations occurred.",
        ],
        "rollback_plan": [
            "Record the previous commit before deploy.",
            "If smoke fails, redeploy the previous commit and restart only the Degen Ops bot service.",
            "Re-run the pre-deploy local gate and post-deploy smoke after rollback.",
            "Preserve audit logs and failure output for diagnosis.",
        ],
        "reversibility": {
            "code_deploy": "reversible by redeploying the previous commit",
            "bot_restart": "reversible by restarting the previous deployed code",
            "schema": "OpsBotMemory table creation may be additive; do not drop data during rollback without a separate approval",
            "messages": "Discord smoke replies are externally visible and cannot be unsent without moderation action",
        },
        "notes": [
            "No production writes are performed by this preflight.",
            "This preflight does not stage, commit, push, deploy, restart, or change credentials.",
            "Use the change manifest intended_files list for explicit staging; do not use git add -A.",
        ],
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Deploy Preflight",
        "",
        f"- read_only: {str(report['read_only']).lower()}",
        f"- performs_changes: {str(report['performs_changes']).lower()}",
        f"- approval_phrase: `{report['approval_phrase']}`",
        "",
        "## Targets",
        "",
    ]
    lines.extend(f"- {key}: {value}" for key, value in report["targets"].items())
    lines.extend(["", "## Intended Files", ""])
    lines.extend(f"- {path}" for path in report["intended_files"] or ["none"])
    lines.extend(["", "## Known Unrelated Files", ""])
    lines.extend(f"- {path}" for path in report["known_unrelated_files"] or ["none"])
    lines.extend(["", "## Pre-Deploy Checks", ""])
    lines.extend(f"- `{check}`" for check in report["pre_deploy_checks"])
    lines.extend(["", "## Post-Deploy Checks", ""])
    lines.extend(f"- {check}" for check in report["post_deploy_checks"])
    lines.extend(["", "## Rollback", ""])
    lines.extend(f"- {step}" for step in report["rollback_plan"])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render the read-only Degen Ops deploy preflight.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_deploy_preflight(repo_root=args.repo_root)
    print(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
