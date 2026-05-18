from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import NamedTuple, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUFLO_PACKAGE = os.environ.get("RUFLO_PACKAGE", "ruflo@3.7.0-alpha.45")
DEFAULT_NAMESPACE = "degen"


class PlannedCommand(NamedTuple):
    label: str
    command: list[str]


class MemorySeed(NamedTuple):
    key: str
    value: str
    tags: str


MEMORY_SEEDS: tuple[MemorySeed, ...] = (
    MemorySeed(
        key="parser/stitching",
        value=(
            "Degen parser is deterministic-first. Preserve image plus explicit buy/sell "
            "force-stitching up to 45s, nearest image/text pairing for back-to-back "
            "deals, and ignored child rows for stitched groups."
        ),
        tags="parser,stitching,degen",
    ),
    MemorySeed(
        key="tiktok/webhook-signature",
        value=(
            "Do not change TikTok Shop webhook verification without a captured failing "
            "webhook. Proven algorithm: HMAC-SHA256(app_secret, app_key + raw_body); "
            "signature arrives in Authorization or X-TT-Signature."
        ),
        tags="tiktok,webhook,security",
    ),
    MemorySeed(
        key="deployment/source-of-truth",
        value=(
            "For live-deal-parser, GitHub main is the deployment source of truth. "
            "Do not SSH into production just to git pull, compile, or restart after "
            "a push; verify health/logs after auto-deploy unless production-only "
            "debugging is explicitly requested."
        ),
        tags="deploy,github,production",
    ),
    MemorySeed(
        key="agent/coordination",
        value=(
            "Local Codex and OpenClaw coordinate through git and explicit file scope. "
            "Inspect dirty worktrees before edits, avoid overlapping ownership, and "
            "never revert unrelated user or agent changes."
        ),
        tags="codex,openclaw,coordination",
    ),
    MemorySeed(
        key="finance/bank-reconciliation",
        value=(
            "Bank reconciliation matching must require compatible payment rail and "
            "cash-flow direction; Apple Cash, Zelle, PayPal, cash, and card should "
            "not be fuzzy-matched across incompatible rails."
        ),
        tags="finance,reconciliation,matching",
    ),
)


def ruflo_command(*args: str) -> list[str]:
    return ["npx", "--yes", RUFLO_PACKAGE, *args]


def build_status_plan() -> list[PlannedCommand]:
    return [
        PlannedCommand("Node version", ["node", "--version"]),
        PlannedCommand("npm version", ["npm", "--version"]),
        PlannedCommand("Ruflo version", ruflo_command("--version")),
        PlannedCommand("Ruflo doctor suggestions", ruflo_command("doctor", "--fix")),
    ]


def build_init_memory_plan() -> list[PlannedCommand]:
    return [
        PlannedCommand("Initialize Ruflo project memory", ruflo_command("memory", "init")),
    ]


def build_seed_memory_plan() -> list[PlannedCommand]:
    plan: list[PlannedCommand] = []
    for seed in MEMORY_SEEDS:
        plan.append(
            PlannedCommand(
                f"Store {seed.key}",
                ruflo_command(
                    "memory",
                    "store",
                    "-k",
                    seed.key,
                    "-v",
                    seed.value,
                    "-n",
                    DEFAULT_NAMESPACE,
                    "--upsert",
                    "--tags",
                    seed.tags,
                ),
            )
        )
    return plan


def build_search_memory_plan(query: str) -> list[PlannedCommand]:
    return [
        PlannedCommand(
            "Search Degen Ruflo memory",
            ruflo_command(
                "memory",
                "search",
                "-q",
                query,
                "-n",
                DEFAULT_NAMESPACE,
                "-t",
                "keyword",
                "-l",
                "8",
            ),
        )
    ]


def build_preflight_plan(task: str) -> list[PlannedCommand]:
    return [
        PlannedCommand("Current git state", ["git", "status", "--short", "--branch"]),
        *build_search_memory_plan(task),
        PlannedCommand(
            "Route task through Ruflo hooks",
            ruflo_command("hooks", "route", "-t", task, "-K", "3"),
        ),
    ]


def build_remember_plan(key: str, value: str, tags: str) -> list[PlannedCommand]:
    command = ruflo_command(
        "memory",
        "store",
        "-k",
        key,
        "-v",
        value,
        "-n",
        DEFAULT_NAMESPACE,
        "--upsert",
    )
    if tags:
        command.extend(["--tags", tags])
    return [PlannedCommand(f"Store {key}", command)]


def build_review_diff_plan() -> list[PlannedCommand]:
    return [
        PlannedCommand(
            "Classify current git diff",
            ruflo_command("analyze", "diff", "--classify"),
        ),
        PlannedCommand(
            "Recommend reviewers for current git diff",
            ruflo_command("analyze", "diff", "--reviewers"),
        ),
    ]


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:60] or "handoff"


def default_handoff_path(task: str, *, now: dt.datetime | None = None) -> Path:
    timestamp = (now or dt.datetime.now()).strftime("%Y%m%d-%H%M%S")
    return PROJECT_ROOT / ".ruflo" / "handoffs" / f"{timestamp}-{slugify(task)}.md"


def quote_command(command: Sequence[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def resolve_command(command: Sequence[str]) -> list[str]:
    if not command:
        return []
    executable = shutil.which(command[0])
    if executable is None:
        return list(command)
    return [executable, *command[1:]]


def capture_command(command: Sequence[str], cwd: Path = PROJECT_ROOT) -> str:
    result = subprocess.run(
        resolve_command(command),
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )
    output = result.stdout
    if result.stderr:
        output += result.stderr
    if result.returncode != 0:
        output += f"\n[exit code: {result.returncode}]\n"
    return output.rstrip()


def build_openclaw_handoff_packet(task: str) -> str:
    branch = capture_command(["git", "branch", "--show-current"]) or "(detached or unknown)"
    status = capture_command(["git", "status", "--short", "--branch"]) or "(clean)"
    diff_stat = capture_command(["git", "diff", "--stat"]) or "(no unstaged diff)"

    return f"""# OpenClaw Handoff: {task}

Repo: `{PROJECT_ROOT}`
Branch: `{branch.strip()}`

## Task

{task}

## Current Git State

```text
{status}
```

## Current Diff Stat

```text
{diff_stat}
```

## Ruflo Preflight

Run `scripts/ruflo_pilot.py preflight "{task}" --apply` before editing so Ruflo can surface project memory and route hints.

## Guardrails

- Read `AGENTS.md` before changing code.
- Preserve unrelated local work; stage explicit files only.
- Keep file ownership narrow. Do not overlap another active Codex/OpenClaw task unless the user asks.
- Do not edit production files under `/opt/degen/app`.
- Do not restart production services unless the user explicitly approves it.
- Run focused tests before handing back.
- For pushes to `main`, verify ancestry/current `origin/main` and use a clean temp worktree if this checkout has unrelated local edits.
"""


def write_handoff_packet(task: str, output_path: Path | None = None) -> Path:
    path = output_path or default_handoff_path(task)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(build_openclaw_handoff_packet(task), encoding="utf-8")
    return path


def run_plan(plan: Sequence[PlannedCommand], *, apply: bool, cwd: Path = PROJECT_ROOT) -> int:
    for step in plan:
        print(f"\n# {step.label}", flush=True)
        print(quote_command(step.command), flush=True)
        if apply:
            subprocess.run(resolve_command(step.command), cwd=cwd, check=True)

    if not apply:
        print("\nDry run only. Re-run the same command with --apply to execute.")
    return 0


def run_handoff(task: str, *, apply: bool, output_path: Path | None = None) -> int:
    path = output_path or default_handoff_path(task)
    print(f"\n# OpenClaw handoff packet", flush=True)
    print(str(path), flush=True)
    if apply:
        written = write_handoff_packet(task, path)
        print(f"Wrote {written}", flush=True)
    else:
        print("\nDry run only. Re-run the same command with --apply to write the handoff packet.")
    return 0


def add_apply_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the planned Ruflo commands. Without this flag, only print them.",
    )


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Safe Ruflo pilot wrapper for the Degen live-deal-parser repo."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="Check local Ruflo prerequisites.")
    add_apply_flag(status)

    init_memory = subparsers.add_parser(
        "init-memory",
        help="Initialize Ruflo memory in local ignored runtime directories.",
    )
    add_apply_flag(init_memory)

    seed_memory = subparsers.add_parser(
        "seed-memory",
        help="Store high-value Degen project invariants into Ruflo memory.",
    )
    add_apply_flag(seed_memory)

    search_memory = subparsers.add_parser(
        "search-memory",
        help="Search the Degen Ruflo memory namespace.",
    )
    search_memory.add_argument("query", help="Keyword query to search for.")
    add_apply_flag(search_memory)

    preflight = subparsers.add_parser(
        "preflight",
        help="Run the standard Ruflo preflight for a task before editing.",
    )
    preflight.add_argument("task", help="Task description.")
    add_apply_flag(preflight)

    remember = subparsers.add_parser(
        "remember",
        help="Store a successful project lesson in Ruflo memory.",
    )
    remember.add_argument("key", help="Memory key, for example parser/payment-only-sell.")
    remember.add_argument("value", help="Lesson or pattern to remember.")
    remember.add_argument("--tags", default="degen", help="Comma-separated tags.")
    add_apply_flag(remember)

    handoff_openclaw = subparsers.add_parser(
        "handoff-openclaw",
        help="Write an ignored markdown handoff packet for OpenClaw.",
    )
    handoff_openclaw.add_argument("task", help="Task description.")
    handoff_openclaw.add_argument(
        "--out",
        type=Path,
        help="Optional handoff markdown path. Defaults to .ruflo/handoffs/...",
    )
    add_apply_flag(handoff_openclaw)

    review_diff = subparsers.add_parser(
        "review-diff",
        help="Ask Ruflo to classify risk and reviewers for the current git diff.",
    )
    add_apply_flag(review_diff)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)

    if args.command == "status":
        plan = build_status_plan()
    elif args.command == "init-memory":
        plan = build_init_memory_plan()
    elif args.command == "seed-memory":
        plan = build_seed_memory_plan()
    elif args.command == "search-memory":
        plan = build_search_memory_plan(args.query)
    elif args.command == "preflight":
        plan = build_preflight_plan(args.task)
    elif args.command == "remember":
        plan = build_remember_plan(args.key, args.value, args.tags)
    elif args.command == "handoff-openclaw":
        return run_handoff(args.task, apply=args.apply, output_path=args.out)
    elif args.command == "review-diff":
        plan = build_review_diff_plan()
    else:  # pragma: no cover - argparse prevents this path.
        parser.error(f"unknown command: {args.command}")

    return run_plan(plan, apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
