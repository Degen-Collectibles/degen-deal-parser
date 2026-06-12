from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.degen_ops_readiness import REQUIRED_ARTIFACTS


EXTRA_INTENDED_PATHS = {
    "app/models.py",
    "docs/ops/degen-ops-bot-improvement-plan.md",
    "requirements.txt",
    "scripts/degen_ops_discord_bot.py",
    "tests/test_ops_agent.py",
    "tests/test_ops_mcp.py",
}


def _normalize_path(path: str) -> str:
    return str(path or "").replace("\\", "/").strip()


def intended_path_set() -> set[str]:
    paths = {_normalize_path(path) for path in REQUIRED_ARTIFACTS}
    paths.update(EXTRA_INTENDED_PATHS)
    paths.update(_normalize_path(str(path.relative_to(REPO_ROOT))) for path in (REPO_ROOT / "tests").glob("test_degen_ops_*.py"))
    return paths


def _changed_paths_from_git(repo_root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--modified", "--others", "--exclude-standard"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        timeout=30,
        check=True,
    )
    return sorted({_normalize_path(line) for line in result.stdout.splitlines() if line.strip()})


def build_change_manifest(
    *,
    repo_root: Path | str = REPO_ROOT,
    changed_paths: list[str] | None = None,
) -> dict[str, Any]:
    root = Path(repo_root)
    intended = intended_path_set()
    changed = sorted({_normalize_path(path) for path in (changed_paths if changed_paths is not None else _changed_paths_from_git(root))})
    intended_changed = [path for path in changed if path in intended]
    unrelated_changed = [path for path in changed if path not in intended]
    generated_noise = [
        path
        for path in unrelated_changed
        if path.startswith("outputs/")
        or "/__pycache__/" in path
        or path.endswith(".pyc")
    ]
    return {
        "name": "degen_ops_change_manifest",
        "intended_file_count": len(intended_changed),
        "unrelated_file_count": len(unrelated_changed),
        "generated_noise_count": len(generated_noise),
        "safe_to_stage_intended_only": bool(intended_changed),
        "intended_files": intended_changed,
        "unrelated_files": unrelated_changed,
        "generated_noise": generated_noise,
        "stage_command": "git add -- " + " ".join(intended_changed) if intended_changed else "",
        "notes": [
            "This manifest is read-only and does not stage files.",
            "Use the intended file list for explicit staging; avoid broad staging in this mixed worktree.",
            "Review unrelated_files before any commit; preserve user or parallel-agent changes.",
        ],
    }


def summarize_manifest(manifest: dict[str, Any], *, sample_limit: int = 20) -> dict[str, Any]:
    limit = max(0, int(sample_limit))

    def sample(key: str) -> list[str]:
        return list(manifest.get(key, []))[:limit]

    return {
        "name": manifest["name"],
        "summary": True,
        "intended_file_count": manifest["intended_file_count"],
        "unrelated_file_count": manifest["unrelated_file_count"],
        "generated_noise_count": manifest["generated_noise_count"],
        "safe_to_stage_intended_only": manifest["safe_to_stage_intended_only"],
        "intended_files_sample": sample("intended_files"),
        "unrelated_files_sample": sample("unrelated_files"),
        "generated_noise_sample": sample("generated_noise"),
        "stage_command_available": bool(manifest.get("stage_command")),
        "stage_command_note": (
            "Run without --summary to print the full explicit stage command."
            if manifest.get("stage_command")
            else ""
        ),
        "notes": manifest["notes"],
    }


def _render_markdown(manifest: dict[str, Any]) -> str:
    if manifest.get("summary"):
        lines = [
            "# Degen Ops Change Manifest Summary",
            "",
            f"- intended_file_count: {manifest['intended_file_count']}",
            f"- unrelated_file_count: {manifest['unrelated_file_count']}",
            f"- generated_noise_count: {manifest['generated_noise_count']}",
            f"- safe_to_stage_intended_only: {str(manifest['safe_to_stage_intended_only']).lower()}",
            f"- stage_command_available: {str(manifest['stage_command_available']).lower()}",
            "",
            "## Intended Files Sample",
            "",
        ]
        lines.extend(f"- {path}" for path in manifest["intended_files_sample"] or ["none"])
        lines.extend(["", "## Unrelated Files Sample", ""])
        lines.extend(f"- {path}" for path in manifest["unrelated_files_sample"] or ["none"])
        lines.extend(["", "## Generated Noise Sample", ""])
        lines.extend(f"- {path}" for path in manifest["generated_noise_sample"] or ["none"])
        if manifest.get("stage_command_note"):
            lines.extend(["", "## Stage Command", "", manifest["stage_command_note"]])
        return "\n".join(lines) + "\n"

    lines = [
        "# Degen Ops Change Manifest",
        "",
        f"- intended_file_count: {manifest['intended_file_count']}",
        f"- unrelated_file_count: {manifest['unrelated_file_count']}",
        f"- generated_noise_count: {manifest['generated_noise_count']}",
        f"- safe_to_stage_intended_only: {str(manifest['safe_to_stage_intended_only']).lower()}",
        "",
        "## Intended Files",
        "",
    ]
    lines.extend(f"- {path}" for path in manifest["intended_files"] or ["none"])
    lines.extend(["", "## Unrelated Files", ""])
    lines.extend(f"- {path}" for path in manifest["unrelated_files"] or ["none"])
    if manifest["stage_command"]:
        lines.extend(["", "## Explicit Stage Command", "", f"`{manifest['stage_command']}`"])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a read-only manifest of intended Degen Ops changes.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--summary", action="store_true", help="Emit counts and path samples instead of full file lists.")
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = build_change_manifest(repo_root=args.repo_root)
    if args.summary:
        manifest = summarize_manifest(manifest, sample_limit=args.sample_limit)
    print(json.dumps(manifest, indent=2, sort_keys=True) if args.json else _render_markdown(manifest), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
