from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chat with the read-only Degen Ops Agent.")
    parser.add_argument("--scope", choices=("owner", "partner", "manager", "employee", "tiktok"), default="employee")
    parser.add_argument("--prompt", default="", help="Run one prompt and exit instead of opening interactive chat.")
    parser.add_argument("--model", default="", help="Override the repo-configured fast model.")
    parser.add_argument("--preflight", action="store_true", help="Check scope, model config, and tools without an LLM call.")
    parser.add_argument("--read-check", action="store_true", help="With --preflight, call one read-only inventory tool.")
    parser.add_argument("--database-url", default="", help="Temporarily override DATABASE_URL for this chat process.")
    parser.add_argument(
        "--database-url-env",
        default="",
        help="Read a temporary DATABASE_URL override from this environment variable.",
    )
    parser.add_argument("--temperature", type=float, default=0.2)
    return parser.parse_args()


SECRET_PATTERNS = [
    re.compile(r"(?i)(postgres(?:ql)?(?:\+psycopg)?://)([^:@\s]+):([^@\s]+)@"),
    re.compile(r"(?i)(password=)[^\s&;]+"),
    re.compile(r"(?i)(token=)[^\s&;]+"),
    re.compile(r"(?i)(api[_-]?key=)[^\s&;]+"),
]


def sanitize(text: Any) -> str:
    value = str(text)
    for pattern in SECRET_PATTERNS:
        if pattern.groups >= 3:
            value = pattern.sub(r"\1***:***@", value)
        else:
            value = pattern.sub(r"\1***", value)
    return value


def configure_environment(args: argparse.Namespace) -> int:
    database_url = args.database_url
    if args.database_url_env:
        database_url = os.getenv(args.database_url_env, "")
        if not database_url:
            print(f"Environment variable {args.database_url_env!r} is not set or is empty.", file=sys.stderr)
            return 1
    os.environ["LOG_TO_FILE"] = "false"
    os.environ["DEGEN_OPS_MCP_SCOPE"] = args.scope
    if database_url:
        os.environ["DATABASE_URL"] = database_url
    return 0


PREFLIGHT_SCENARIO = {
    "lot_name": "Preflight smoke lot",
    "category": "readiness-smoke",
    "purchase_cost": 1.0,
    "expected_revenue": 2.0,
    "unit_count": 1,
    "minimum_cash_reserve": 0.0,
    "target_payback_weeks": 1,
}


def _preflight_tool_args(tool_name: str) -> dict[str, Any]:
    if tool_name in {"get_finance_snapshot", "get_channel_velocity", "get_loan_and_payback_snapshot"}:
        return {"days": 90}
    if tool_name in {"evaluate_inventory_buy", "generate_partner_update"}:
        return {"scenario": dict(PREFLIGHT_SCENARIO), "days": 90}
    return {}


def _run_scope_read_checks(runner: Any) -> list[dict[str, Any]]:
    checks = []
    for tool_name in sorted(runner.allowed_tools):
        try:
            result = runner.call_tool(tool_name, _preflight_tool_args(tool_name))
            if result.get("error"):
                checks.append({"tool": tool_name, "status": "failed", "error": sanitize(result["error"])})
            elif result.get("read_only") is False:
                checks.append({"tool": tool_name, "status": "failed", "error": "Tool did not preserve read_only=true."})
            else:
                checks.append({"tool": tool_name, "status": "passed"})
        except Exception as exc:
            checks.append({"tool": tool_name, "status": "failed", "error": sanitize(exc)})
    return checks


def build_preflight_report(
    *,
    scope: str,
    provider: str,
    model: str,
    api_key_configured: bool,
    runner: Any,
    read_check: bool,
) -> dict[str, Any]:
    tools = sorted(runner.allowed_tools)
    report: dict[str, Any] = {
        "ok": bool(api_key_configured),
        "scope": scope,
        "provider": provider,
        "model": model,
        "api_key_configured": bool(api_key_configured),
        "tools": tools,
        "read_check": "skipped",
        "read_only": True,
    }
    if read_check:
        checks = _run_scope_read_checks(runner)
        failed = [check for check in checks if check["status"] != "passed"]
        report["read_checks"] = checks
        if failed:
            report["read_check"] = "failed"
            report["read_check_error"] = failed[0].get("error", f"{failed[0]['tool']} failed")
            report["ok"] = False
        else:
            report["read_check"] = "passed"
    return report


def main() -> int:
    args = parse_args()
    env_status = configure_environment(args)
    if env_status:
        return env_status

    from app.ai_client import get_ai_client, get_fast_model, get_provider, has_ai_key
    from app.ops_chat import DegenOpsChatToolRunner, initial_chat_messages, run_chat_turn

    model = args.model or get_fast_model()
    runner = DegenOpsChatToolRunner(scope=args.scope)

    if args.preflight:
        report = build_preflight_report(
            scope=args.scope,
            provider=get_provider(),
            model=model,
            api_key_configured=has_ai_key(),
            runner=runner,
            read_check=args.read_check,
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["ok"] else 1

    if not has_ai_key():
        print(
            f"No API key configured for AI_PROVIDER={get_provider()!r}. "
            "Set NVIDIA_API_KEY or OPENAI_API_KEY before chatting.",
            file=sys.stderr,
        )
        return 1

    client = get_ai_client()
    messages = initial_chat_messages()

    def ask(user_text: str) -> str:
        nonlocal messages
        messages.append({"role": "user", "content": user_text})
        answer, messages = run_chat_turn(
            client=client,
            model=model,
            messages=messages,
            runner=runner,
            temperature=args.temperature,
        )
        return answer

    if args.prompt:
        print(ask(args.prompt))
        return 0

    print(f"Degen Ops Agent ({args.scope} scope, model={model}). Type 'exit' to quit.")
    while True:
        try:
            user_text = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not user_text:
            continue
        if user_text.lower() in {"exit", "quit", "/exit", "/quit"}:
            return 0
        try:
            print(f"agent> {ask(user_text)}")
        except Exception as exc:
            print(f"agent error> {exc}")


if __name__ == "__main__":
    sys.exit(main())
