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


REQUIRED_DECISIONS = [
    "live_data_access_topology",
    "read_only_db_credential_or_green_hosted_client",
]


def _status_block(report: dict[str, Any] | None) -> dict[str, Any]:
    if report is None:
        return {"status": "skipped", "report": None}
    return {"status": "pass" if report.get("ok") else "fail", "report": report}


def _missing_database_report(*, source: str) -> dict[str, Any]:
    return {
        "ok": False,
        "database_url_configured": False,
        "database_url_source": source,
        "read_only": True,
        "error": f"Environment variable {source!r} is not set or is empty.",
    }


def build_rollout_gate_report(
    *,
    repo_root: Path | str = REPO_ROOT,
    database_url_source: str = "",
    live_data_report: dict[str, Any] | None = None,
    pilot_demo_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from scripts.degen_ops_mvp_audit import build_mvp_audit
    from scripts.degen_ops_completion_audit import build_completion_audit
    from scripts.degen_ops_readiness import build_readiness_report
    from scripts.degen_ops_scope_audit import build_scope_audit

    root = Path(repo_root)
    readiness = build_readiness_report(repo_root=root)
    completion = build_completion_audit(repo_root=root)
    mvp = build_mvp_audit(repo_root=root)
    scope = build_scope_audit()
    static_ok = bool(
        readiness.get("code_ready")
        and completion.get("code_ready")
        and mvp.get("mvp_code_ready")
        and scope.get("ok")
    )
    live_block = _status_block(live_data_report)
    pilot_block = _status_block(pilot_demo_report)
    return {
        "name": "degen_ops_rollout_gate",
        "static_ok": static_ok,
        "team_live_rollout_ready": False,
        "database_url_source": database_url_source,
        "required_decisions": REQUIRED_DECISIONS[:],
        "reason_team_rollout_not_ready": (
            "Static code gates can pass before team rollout is approved. "
            "Live team rollout still requires an approved access topology and credential."
        ),
        "readiness": readiness,
        "completion": completion,
        "mvp": mvp,
        "scope": scope,
        "live_data": live_block,
        "pilot_demo": pilot_block,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Degen Ops Rollout Gate",
        "",
        f"- static_ok: {str(report['static_ok']).lower()}",
        f"- team_live_rollout_ready: {str(report['team_live_rollout_ready']).lower()}",
        f"- live_data: {report['live_data']['status']}",
        f"- pilot_demo: {report['pilot_demo']['status']}",
        "",
        "## Required Decisions",
        "",
    ]
    lines.extend(f"- {decision}" for decision in report["required_decisions"])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Degen Ops rollout gate.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--database-url", default="", help="Temporary database URL override. The value is never printed.")
    parser.add_argument("--database-url-env", default="DEGEN_OPS_READONLY_DATABASE_URL")
    parser.add_argument("--live-scope", choices=("owner", "partner", "employee"), default="employee")
    parser.add_argument("--pilot-scope", choices=("owner", "partner"), default="partner")
    parser.add_argument("--run-live", action="store_true", help="Run live-data verifier with the configured DB URL.")
    parser.add_argument("--run-pilot", action="store_true", help="Run no-LLM buy-decision pilot demo with the configured DB URL.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_url = args.database_url
    source = "--database-url" if database_url else args.database_url_env
    if (args.run_live or args.run_pilot) and not database_url:
        database_url = os.getenv(args.database_url_env, "")

    live_report = None
    pilot_report = None
    if args.run_live or args.run_pilot:
        if not database_url:
            missing = _missing_database_report(source=source)
            if args.run_live:
                live_report = dict(missing)
            if args.run_pilot:
                pilot_report = dict(missing)
        else:
            os.environ["DATABASE_URL"] = database_url
            from app.ops_chat import DegenOpsChatToolRunner
            from scripts.degen_ops_live_data import build_live_data_report
            from scripts.degen_ops_pilot_demo import build_pilot_demo_report

            if args.run_live:
                live_report = build_live_data_report(
                    scope=args.live_scope,
                    runner=DegenOpsChatToolRunner(scope=args.live_scope),
                    database_url_source=source,
                    database_url_configured=True,
                )
            if args.run_pilot:
                pilot_report = build_pilot_demo_report(
                    scope=args.pilot_scope,
                    runner=DegenOpsChatToolRunner(scope=args.pilot_scope),
                )

    report = build_rollout_gate_report(
        repo_root=args.repo_root,
        database_url_source=source if (args.run_live or args.run_pilot) else "",
        live_data_report=live_report,
        pilot_demo_report=pilot_report,
    )
    print(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report), end="")
    gate_ok = report["static_ok"] and report["live_data"]["status"] != "fail" and report["pilot_demo"]["status"] != "fail"
    return 0 if gate_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
