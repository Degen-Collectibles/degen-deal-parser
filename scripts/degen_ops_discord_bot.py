from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


MAX_DISCORD_MESSAGE_CHARS = 1900
DEFAULT_MAX_PROMPT_CHARS = 4000
DEFAULT_RATE_LIMIT_PER_MINUTE = 6
PARTNER_SCOPE = "partner"


def _csv_set(value: str) -> set[str]:
    return {item.strip() for item in str(value or "").split(",") if item.strip()}


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def sanitize_for_log(value: Any) -> str:
    text = str(value)
    text = re.sub(r"(?i)(postgres(?:ql)?(?:\+psycopg)?://)([^:@\s]+):([^@\s]+)@", r"\1***:***@", text)
    text = re.sub(r"(?i)(sk-[A-Za-z0-9_-]{4})[A-Za-z0-9_-]+", r"\1...REDACTED", text)
    text = re.sub(r"(?i)(token|secret|password|api[_-]?key)=([^\s&;]+)", r"\1=***", text)
    return text


def strip_bot_mention(content: str, bot_user_id: int | None) -> str:
    text = str(content or "").strip()
    if bot_user_id:
        text = re.sub(rf"<@!?{bot_user_id}>", "", text).strip()
    return text


def split_discord_message(text: str, *, limit: int = MAX_DISCORD_MESSAGE_CHARS) -> list[str]:
    value = str(text or "").strip()
    if not value:
        return ["I did not get an answer back."]
    chunks: list[str] = []
    remaining = value
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit)
        if split_at < max(200, limit // 3):
            split_at = remaining.rfind(" ", 0, limit)
        if split_at < max(200, limit // 3):
            split_at = limit
        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()
    if remaining:
        chunks.append(remaining)
    return chunks


@dataclass(frozen=True)
class BotConfig:
    token: str
    allowed_channel_ids: set[str]
    allowed_user_ids: set[str]
    allow_any_user_in_channel: bool
    model: str
    max_prompt_chars: int
    rate_limit_per_minute: int
    audit_log_path: Path
    dry_run: bool = False


def load_config_from_env(*, dry_run: bool = False) -> BotConfig:
    token = os.getenv("DEGEN_OPS_DISCORD_BOT_TOKEN", "").strip()
    allowed_channel_ids = _csv_set(os.getenv("DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS", ""))
    allowed_user_ids = _csv_set(os.getenv("DEGEN_OPS_DISCORD_ALLOWED_USER_IDS", ""))
    allow_any = _truthy(os.getenv("DEGEN_OPS_DISCORD_ALLOW_ANY_USER_IN_CHANNEL", "false"))
    model = os.getenv("DEGEN_OPS_DISCORD_MODEL", "").strip()
    max_prompt_chars = int(os.getenv("DEGEN_OPS_DISCORD_MAX_PROMPT_CHARS", str(DEFAULT_MAX_PROMPT_CHARS)))
    rate_limit = int(os.getenv("DEGEN_OPS_DISCORD_RATE_LIMIT_PER_MINUTE", str(DEFAULT_RATE_LIMIT_PER_MINUTE)))
    audit_path = Path(os.getenv("DEGEN_OPS_DISCORD_AUDIT_LOG", "logs/degen_ops_discord_bot.jsonl"))

    missing = []
    if not token and not dry_run:
        missing.append("DEGEN_OPS_DISCORD_BOT_TOKEN")
    if not allowed_channel_ids:
        missing.append("DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS")
    if not allowed_user_ids and not allow_any:
        missing.append("DEGEN_OPS_DISCORD_ALLOWED_USER_IDS or DEGEN_OPS_DISCORD_ALLOW_ANY_USER_IN_CHANNEL=true")
    if missing:
        raise RuntimeError("Missing required Degen Ops Discord bot config: " + ", ".join(missing))

    return BotConfig(
        token=token,
        allowed_channel_ids=allowed_channel_ids,
        allowed_user_ids=allowed_user_ids,
        allow_any_user_in_channel=allow_any,
        model=model,
        max_prompt_chars=max_prompt_chars,
        rate_limit_per_minute=rate_limit,
        audit_log_path=audit_path,
        dry_run=dry_run,
    )


def ensure_database_url_from_readonly_env() -> bool:
    if not os.getenv("DATABASE_URL") and os.getenv("DEGEN_OPS_READONLY_DATABASE_URL"):
        os.environ["DATABASE_URL"] = os.environ["DEGEN_OPS_READONLY_DATABASE_URL"]
        return True
    return False


def build_system_prompt() -> str:
    from app.ops_chat import DEGEN_OPS_CHAT_SYSTEM_PROMPT

    return (
        DEGEN_OPS_CHAT_SYSTEM_PROMPT
        + "\n\n"
        "You are answering inside the private #degen-ops-bot Discord channel for Degen partners. "
        "Use partner scope only. Be concise but evidence-backed. "
        "For buy questions, answer with: verdict, sell-through, routing, weekly payback/budget plan, risks, evidence. "
        "Refuse requests to move money, change inventory/listings, send messages, reveal raw owner cash/bank/account "
        "balances, or reveal owner loan/payback totals. End action-taking refusals by saying the bot is read-only."
    )


class PromptRateLimiter:
    def __init__(self, *, limit_per_minute: int):
        self.limit = max(1, int(limit_per_minute))
        self.events: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, user_id: str, now: float) -> bool:
        window = 60.0
        bucket = self.events[user_id]
        while bucket and now - bucket[0] > window:
            bucket.popleft()
        if len(bucket) >= self.limit:
            return False
        bucket.append(now)
        return True


def append_audit_event(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_payload = json.loads(sanitize_for_log(json.dumps(payload, default=str)))
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(clean_payload, sort_keys=True) + "\n")


def should_respond(
    *,
    author_is_bot: bool,
    channel_id: str,
    author_id: str,
    content: str,
    config: BotConfig,
) -> tuple[bool, str]:
    if author_is_bot:
        return False, "bot_author"
    if channel_id not in config.allowed_channel_ids:
        return False, "channel_not_allowed"
    if not content.strip():
        return False, "empty_message"
    if not config.allow_any_user_in_channel and author_id not in config.allowed_user_ids:
        return False, "user_not_allowed"
    if len(content) > config.max_prompt_chars:
        return False, "prompt_too_long"
    return True, "ok"


async def answer_prompt(prompt: str, *, model: str) -> str:
    from app.ai_client import get_ai_client, get_fast_model
    from app.ops_chat import DegenOpsChatToolRunner, initial_chat_messages, run_chat_turn

    runner = DegenOpsChatToolRunner(scope=PARTNER_SCOPE)
    messages = initial_chat_messages(build_system_prompt())
    messages.append({"role": "user", "content": prompt})
    answer, _history = await asyncio.to_thread(
        run_chat_turn,
        client=get_ai_client(),
        model=model or get_fast_model(),
        messages=messages,
        runner=runner,
        temperature=0.2,
        max_tool_rounds=4,
    )
    return answer


async def run_bot(config: BotConfig) -> None:
    import discord

    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)
    limiter = PromptRateLimiter(limit_per_minute=config.rate_limit_per_minute)

    @client.event
    async def on_ready() -> None:
        print(
            "[degen-ops-discord] ready "
            f"user={getattr(client.user, 'id', '')} channels={sorted(config.allowed_channel_ids)}"
        )

    @client.event
    async def on_message(message) -> None:
        bot_user_id = getattr(client.user, "id", None)
        prompt = strip_bot_mention(getattr(message, "content", ""), bot_user_id)
        channel_id = str(getattr(getattr(message, "channel", None), "id", ""))
        author = getattr(message, "author", None)
        author_id = str(getattr(author, "id", ""))
        ok, reason = should_respond(
            author_is_bot=bool(getattr(author, "bot", False)),
            channel_id=channel_id,
            author_id=author_id,
            content=prompt,
            config=config,
        )
        if not ok:
            if reason == "prompt_too_long":
                await message.reply(f"Please keep requests under {config.max_prompt_chars} characters.", mention_author=False)
            return

        now = datetime.now(timezone.utc).timestamp()
        if not limiter.allow(author_id, now):
            await message.reply("Rate limit hit. Give me a minute, then try again.", mention_author=False)
            return

        audit_base = {
            "at": datetime.now(timezone.utc).isoformat(),
            "channel_id": channel_id,
            "author_id": author_id,
            "message_id": str(getattr(message, "id", "")),
            "scope": PARTNER_SCOPE,
            "read_only": True,
        }
        append_audit_event(config.audit_log_path, {**audit_base, "event": "prompt", "prompt": prompt})

        async with message.channel.typing():
            try:
                answer = await answer_prompt(prompt, model=config.model)
            except Exception as exc:
                append_audit_event(config.audit_log_path, {**audit_base, "event": "error", "error": sanitize_for_log(exc)})
                await message.reply(
                    "I hit an internal error while answering. I did not change money, inventory, listings, or messages.",
                    mention_author=False,
                )
                return

        append_audit_event(config.audit_log_path, {**audit_base, "event": "answer", "answer": answer})
        for index, chunk in enumerate(split_discord_message(answer)):
            if index == 0:
                await message.reply(chunk, mention_author=False)
            else:
                await message.channel.send(chunk)

    await client.start(config.token)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the partner-scoped Degen Ops Discord chatbot.")
    parser.add_argument("--dry-run-config", action="store_true", help="Validate config without connecting to Discord.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    os.environ["LOG_TO_FILE"] = "false"
    os.environ["DEGEN_OPS_MCP_SCOPE"] = PARTNER_SCOPE
    ensure_database_url_from_readonly_env()
    config = load_config_from_env(dry_run=args.dry_run_config)
    if args.dry_run_config:
        print(
            json.dumps(
                {
                    "ok": True,
                    "scope": PARTNER_SCOPE,
                    "allowed_channel_count": len(config.allowed_channel_ids),
                    "allowed_user_count": len(config.allowed_user_ids),
                    "allow_any_user_in_channel": config.allow_any_user_in_channel,
                    "audit_log_path": str(config.audit_log_path),
                    "read_only": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    asyncio.run(run_bot(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
