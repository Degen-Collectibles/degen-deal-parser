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
DEFAULT_CONTEXT_HISTORY_LIMIT = 8
DEFAULT_CONTEXT_MAX_CHARS = 3500
PARTNER_SCOPE = "partner"
OWNER_SCOPE = "owner"
SCOPE_RANKS = {
    "employee": 0,
    "manager": 1,
    "partner": 2,
    "tiktok": 2,
    "owner": 3,
}


def _csv_set(value: str) -> set[str]:
    return {item.strip() for item in str(value or "").split(",") if item.strip()}


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_discord_scope(value: Any) -> str:
    scope = str(value or "").strip().lower()
    if scope not in SCOPE_RANKS:
        raise ValueError(f"Unsupported Discord scope: {value!r}")
    return scope


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


def _collapse_context_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _message_author_label(message: Any, *, bot_user_id: int | None = None) -> str:
    author = getattr(message, "author", None)
    author_id = str(getattr(author, "id", ""))
    role = "bot" if bool(getattr(author, "bot", False)) or (bot_user_id and author_id == str(bot_user_id)) else "user"
    return f"{role} {_message_author_name(message)}"


def _message_author_name(message: Any) -> str:
    author = getattr(message, "author", None)
    author_id = str(getattr(author, "id", ""))
    raw_name = (
        getattr(author, "display_name", None)
        or getattr(author, "global_name", None)
        or getattr(author, "name", None)
        or author_id
        or "unknown"
    )
    return _collapse_context_text(raw_name)


def _message_content_for_context(message: Any, *, bot_user_id: int | None = None) -> str:
    content = getattr(message, "clean_content", None) or getattr(message, "content", "")
    return strip_bot_mention(_collapse_context_text(content), bot_user_id)


def _context_line(message: Any, *, bot_user_id: int | None = None, max_message_chars: int = 700) -> str:
    content = _message_content_for_context(message, bot_user_id=bot_user_id)
    if len(content) > max_message_chars:
        content = content[: max_message_chars - 1].rstrip() + "..."
    return f"{_message_author_label(message, bot_user_id=bot_user_id)}: {content}"


def _message_key(message: Any) -> str:
    message_id = getattr(message, "id", None)
    return str(message_id) if message_id is not None else str(id(message))


def build_discord_context_text(
    *,
    current_message: Any,
    referenced_message: Any | None = None,
    recent_messages: list[Any] | None = None,
    bot_user_id: int | None = None,
    max_chars: int = DEFAULT_CONTEXT_MAX_CHARS,
) -> str:
    sections: list[str] = [
        "Discord conversation context for resolving follow-ups like 'that', 'it', 'on TikTok', or 'the previous one'."
    ]
    seen = {_message_key(current_message)}

    if referenced_message is not None:
        seen.add(_message_key(referenced_message))
        sections.append("Replied-to message:")
        sections.append(f"- {_context_line(referenced_message, bot_user_id=bot_user_id)}")

    recent_lines: list[str] = []
    for message in reversed(list(recent_messages or [])):
        key = _message_key(message)
        if key in seen:
            continue
        seen.add(key)
        content = _message_content_for_context(message, bot_user_id=bot_user_id)
        if not content:
            continue
        recent_lines.append(f"- {_context_line(message, bot_user_id=bot_user_id)}")
    if recent_lines:
        sections.append("Recent channel messages, oldest to newest:")
        sections.extend(recent_lines)

    current_content = _message_content_for_context(current_message, bot_user_id=bot_user_id)
    sections.append(f"Current user message: {_message_author_name(current_message)}: {current_content}")
    text = "\n".join(sections)
    if len(text) > max_chars:
        return text[: max_chars - 1].rstrip() + "..."
    return text


async def collect_discord_context_text(
    message: Any,
    *,
    bot_user_id: int | None = None,
    history_limit: int = DEFAULT_CONTEXT_HISTORY_LIMIT,
    max_chars: int = DEFAULT_CONTEXT_MAX_CHARS,
) -> str:
    referenced_message = None
    reference = getattr(message, "reference", None)
    if reference is not None:
        referenced_message = getattr(reference, "resolved", None)
        if referenced_message is None:
            message_id = getattr(reference, "message_id", None)
            channel = getattr(message, "channel", None)
            if message_id and hasattr(channel, "fetch_message"):
                try:
                    referenced_message = await channel.fetch_message(message_id)
                except Exception:
                    referenced_message = None

    recent_messages: list[Any] = []
    channel = getattr(message, "channel", None)
    if hasattr(channel, "history"):
        try:
            history_iter = channel.history(limit=max(1, history_limit), before=message)
            async for prior_message in history_iter:
                recent_messages.append(prior_message)
        except Exception:
            recent_messages = []

    return build_discord_context_text(
        current_message=message,
        referenced_message=referenced_message,
        recent_messages=recent_messages,
        bot_user_id=bot_user_id,
        max_chars=max_chars,
    )


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
class DiscordScopeConfig:
    user_scopes: dict[str, str] | None = None
    channel_scopes: dict[str, str] | None = None

    def has_mappings(self) -> bool:
        return bool(self.user_scopes or self.channel_scopes)


@dataclass(frozen=True)
class BotConfig:
    token: str
    allowed_channel_ids: set[str]
    allowed_user_ids: set[str]
    owner_user_ids: set[str]
    allow_any_user_in_channel: bool
    scope_config: DiscordScopeConfig
    db_auth_enabled: bool
    legacy_allowlist_fallback: bool
    allow_dms: bool
    model: str
    max_prompt_chars: int
    rate_limit_per_minute: int
    audit_log_path: Path
    config_env_path: Path | None = None
    dry_run: bool = False


@dataclass(frozen=True)
class PartnerSetupCommand:
    partner_name: str
    partner_user_id: str
    channel_slug: str


@dataclass(frozen=True)
class PartnerSetupPlan:
    guild_id: str
    requester_user_id: str
    partner_name: str
    partner_user_id: str
    category_name: str
    channel_name: str
    confirmation_phrase: str


def load_config_from_env(*, dry_run: bool = False) -> BotConfig:
    token = os.getenv("DEGEN_OPS_DISCORD_BOT_TOKEN", "").strip()
    allowed_channel_ids = _csv_set(os.getenv("DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS", ""))
    allowed_user_ids = _csv_set(os.getenv("DEGEN_OPS_DISCORD_ALLOWED_USER_IDS", ""))
    owner_user_ids = _csv_set(os.getenv("DEGEN_OPS_DISCORD_OWNER_USER_IDS", ""))
    allow_any = _truthy(os.getenv("DEGEN_OPS_DISCORD_ALLOW_ANY_USER_IN_CHANNEL", "false"))
    db_auth_enabled = _truthy(os.getenv("DEGEN_OPS_DISCORD_DB_AUTH_ENABLED", "false"))
    legacy_allowlist_fallback = _truthy(os.getenv("DEGEN_OPS_DISCORD_LEGACY_ALLOWLIST_FALLBACK", "true"))
    allow_dms = _truthy(os.getenv("DEGEN_OPS_DISCORD_ALLOW_DMS", "false"))
    model = os.getenv("DEGEN_OPS_DISCORD_MODEL", "").strip()
    max_prompt_chars = int(os.getenv("DEGEN_OPS_DISCORD_MAX_PROMPT_CHARS", str(DEFAULT_MAX_PROMPT_CHARS)))
    rate_limit = int(os.getenv("DEGEN_OPS_DISCORD_RATE_LIMIT_PER_MINUTE", str(DEFAULT_RATE_LIMIT_PER_MINUTE)))
    audit_path = Path(os.getenv("DEGEN_OPS_DISCORD_AUDIT_LOG", "logs/degen_ops_discord_bot.jsonl"))
    config_env_path_value = os.getenv("DEGEN_OPS_DISCORD_CONFIG_ENV_FILE", "").strip()
    config_env_path = Path(config_env_path_value) if config_env_path_value else None
    scope_config = load_scope_config_from_env()

    missing = []
    if not token and not dry_run:
        missing.append("DEGEN_OPS_DISCORD_BOT_TOKEN")
    if not db_auth_enabled and not allowed_channel_ids and not scope_config.has_mappings():
        missing.append("DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS")
    if not db_auth_enabled and not allowed_user_ids and not allow_any and not scope_config.has_mappings():
        missing.append("DEGEN_OPS_DISCORD_ALLOWED_USER_IDS or DEGEN_OPS_DISCORD_ALLOW_ANY_USER_IN_CHANNEL=true")
    if missing:
        raise RuntimeError("Missing required Degen Ops Discord bot config: " + ", ".join(missing))

    return BotConfig(
        token=token,
        allowed_channel_ids=allowed_channel_ids,
        allowed_user_ids=allowed_user_ids,
        owner_user_ids=owner_user_ids,
        allow_any_user_in_channel=allow_any,
        scope_config=scope_config,
        db_auth_enabled=db_auth_enabled,
        legacy_allowlist_fallback=legacy_allowlist_fallback,
        allow_dms=allow_dms,
        model=model,
        max_prompt_chars=max_prompt_chars,
        rate_limit_per_minute=rate_limit,
        audit_log_path=audit_path,
        config_env_path=config_env_path,
        dry_run=dry_run,
    )


def ensure_database_url_from_readonly_env() -> bool:
    if not os.getenv("DATABASE_URL") and os.getenv("DEGEN_OPS_READONLY_DATABASE_URL"):
        os.environ["DATABASE_URL"] = os.environ["DEGEN_OPS_READONLY_DATABASE_URL"]
        return True
    return False


def parse_scope_config(value: str) -> DiscordScopeConfig:
    raw = str(value or "").strip()
    if not raw:
        return DiscordScopeConfig(user_scopes={}, channel_scopes={})
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError("Discord scope config must be a JSON object.")

    users = loaded.get("users", {})
    channels = loaded.get("channels", {})
    if not isinstance(users, dict) or not isinstance(channels, dict):
        raise ValueError("Discord scope config requires object fields: users, channels.")
    return DiscordScopeConfig(
        user_scopes={str(key): _normalize_discord_scope(value) for key, value in users.items()},
        channel_scopes={str(key): _normalize_discord_scope(value) for key, value in channels.items()},
    )


def load_scope_config_from_env() -> DiscordScopeConfig:
    file_path = os.getenv("DEGEN_OPS_DISCORD_SCOPE_MAP_FILE", "").strip()
    if file_path:
        return parse_scope_config(Path(file_path).read_text(encoding="utf-8"))
    return parse_scope_config(os.getenv("DEGEN_OPS_DISCORD_ROLE_MAP", ""))


def resolve_discord_scope(
    author_id: str,
    channel_id: str,
    scope_config: DiscordScopeConfig,
) -> tuple[str | None, str]:
    user_scopes = scope_config.user_scopes or {}
    channel_scopes = scope_config.channel_scopes or {}
    user_scope = user_scopes.get(str(author_id))
    if not user_scope:
        return None, "user_not_mapped"
    channel_scope = channel_scopes.get(str(channel_id))
    if not channel_scope:
        return None, "channel_not_mapped"
    user_rank = SCOPE_RANKS[user_scope]
    channel_rank = SCOPE_RANKS[channel_scope]
    effective = user_scope if user_rank <= channel_rank else channel_scope
    return effective, "ok"


def determine_message_scope(
    author_id: str,
    channel_id: str,
    config: BotConfig,
    *,
    db_scope: tuple[str | None, str] | None = None,
) -> tuple[str | None, str]:
    if config.db_auth_enabled and db_scope is not None:
        scope, reason = db_scope
        if scope:
            return scope, reason
        if not config.legacy_allowlist_fallback:
            return None, reason
    if config.scope_config.has_mappings():
        return resolve_discord_scope(author_id, channel_id, config.scope_config)
    if channel_id in config.allowed_channel_ids and (
        config.allow_any_user_in_channel or author_id in config.allowed_user_ids
    ):
        return PARTNER_SCOPE, "legacy_partner"
    return None, "not_authorized"


def _slugify_channel_name(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug[:80] or "partner"


def parse_partner_setup_command(content: str) -> PartnerSetupCommand | None:
    text = str(content or "").strip()
    match = re.fullmatch(
        r"(?is)setup\s+partner\s+(.+?)\s+(?:user[_\s-]?id|discord[_\s-]?id)\s*=\s*<?@?!?(\d{15,25})>?",
        text,
    )
    if not match:
        return None
    partner_name = re.sub(r"\s+", " ", match.group(1)).strip(" .#")
    partner_user_id = match.group(2)
    if not partner_name:
        return None
    return PartnerSetupCommand(
        partner_name=partner_name,
        partner_user_id=partner_user_id,
        channel_slug=_slugify_channel_name(partner_name),
    )


def build_partner_setup_plan(
    *,
    author_id: str,
    command: PartnerSetupCommand | None,
    config: BotConfig,
    guild_id: str,
    requester_user_id: str,
) -> tuple[bool, str, PartnerSetupPlan | None]:
    if command is None:
        return False, "not_setup_command", None
    if author_id not in config.owner_user_ids:
        return False, "owner_only", None
    if not guild_id:
        return False, "guild_required", None
    confirmation_name = re.sub(r"[^A-Za-z0-9]+", " ", command.partner_name).strip().upper()
    if not confirmation_name:
        confirmation_name = command.channel_slug.upper()
    plan = PartnerSetupPlan(
        guild_id=str(guild_id),
        requester_user_id=str(requester_user_id),
        partner_name=command.partner_name,
        partner_user_id=command.partner_user_id,
        category_name="Degen Ops Partners",
        channel_name=f"degen-ops-{command.channel_slug}",
        confirmation_phrase=f"CONFIRM SETUP {confirmation_name}",
    )
    return True, "ok", plan


def format_partner_setup_draft(plan: PartnerSetupPlan | None) -> str:
    if plan is None:
        return "I could not build a partner setup plan."
    return (
        "I can set up this private partner workspace.\n\n"
        f"Category: {plan.category_name}\n"
        f"Channel: #{plan.channel_name}\n"
        f"Partner user ID: {plan.partner_user_id}\n"
        f"Owner/admin user ID: {plan.requester_user_id}\n"
        "Permissions: private to the partner, owner/admin, and Degen Ops Bot.\n"
        "Business-data mode: partner-safe read-only answers only.\n\n"
        f"Reply `{plan.confirmation_phrase}` to create it."
    )


def matches_partner_setup_confirmation(content: str, plan: PartnerSetupPlan | None) -> bool:
    return bool(plan and str(content or "").strip() == plan.confirmation_phrase)


def _merge_csv_values(existing_value: str, new_values: set[str]) -> str:
    values = [item.strip() for item in str(existing_value or "").split(",") if item.strip()]
    seen = set(values)
    for value in sorted(new_values):
        if value not in seen:
            values.append(value)
            seen.add(value)
    return ",".join(values)


def _replace_or_append_env_line(lines: list[str], key: str, value: str) -> list[str]:
    prefix = f"{key}="
    replaced = False
    output: list[str] = []
    for line in lines:
        if line.startswith(prefix):
            output.append(prefix + value)
            replaced = True
        else:
            output.append(line)
    if not replaced:
        output.append(prefix + value)
    return output


def update_env_allowlist(path: Path, *, channel_ids: set[str], user_ids: set[str]) -> None:
    existing_text = path.read_text(encoding="utf-8") if path.exists() else ""
    lines = existing_text.splitlines()
    current_channels = ""
    current_users = ""
    for line in lines:
        if line.startswith("DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS="):
            current_channels = line.split("=", 1)[1]
        elif line.startswith("DEGEN_OPS_DISCORD_ALLOWED_USER_IDS="):
            current_users = line.split("=", 1)[1]

    lines = _replace_or_append_env_line(
        lines,
        "DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS",
        _merge_csv_values(current_channels, channel_ids),
    )
    lines = _replace_or_append_env_line(
        lines,
        "DEGEN_OPS_DISCORD_ALLOWED_USER_IDS",
        _merge_csv_values(current_users, user_ids),
    )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


async def apply_partner_setup(guild: Any, bot_member: Any, plan: PartnerSetupPlan, config: BotConfig) -> Any:
    import discord

    category = discord.utils.get(getattr(guild, "categories", []), name=plan.category_name)
    reason = f"Degen Ops partner setup requested by {plan.requester_user_id}"
    if category is None:
        category = await guild.create_category(plan.category_name, reason=reason)

    existing_channel = discord.utils.get(getattr(guild, "text_channels", []), name=plan.channel_name)

    partner_member = guild.get_member(int(plan.partner_user_id))
    if partner_member is None:
        partner_member = await guild.fetch_member(int(plan.partner_user_id))
    requester_member = guild.get_member(int(plan.requester_user_id))
    if requester_member is None:
        requester_member = await guild.fetch_member(int(plan.requester_user_id))

    allow = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        partner_member: allow,
        requester_member: allow,
    }
    if bot_member is not None:
        overwrites[bot_member] = allow

    if existing_channel is None:
        channel = await category.create_text_channel(plan.channel_name, overwrites=overwrites, reason=reason)
    else:
        channel = existing_channel
        await channel.edit(category=category, overwrites=overwrites, reason=reason)

    config.allowed_channel_ids.add(str(channel.id))
    config.allowed_user_ids.add(plan.partner_user_id)
    if config.config_env_path is not None:
        update_env_allowlist(
            config.config_env_path,
            channel_ids={str(channel.id)},
            user_ids={plan.partner_user_id},
        )
    return channel


def build_system_prompt(scope: str = PARTNER_SCOPE) -> str:
    from app.ops_chat import DEGEN_OPS_CHAT_SYSTEM_PROMPT

    normalized_scope = _normalize_discord_scope(scope)
    audience = "Degen owners" if normalized_scope == OWNER_SCOPE else f"Degen {normalized_scope} users"
    return (
        DEGEN_OPS_CHAT_SYSTEM_PROMPT
        + "\n\n"
        f"You are answering inside Discord for {audience}. "
        f"Use {normalized_scope} scope only. Be concise but evidence-backed. "
        "For buy questions, answer with: verdict, sell-through, routing, weekly payback/budget plan, risks, evidence. "
        "Refuse requests to move money, change inventory/listings, or send messages. "
        "For non-owner scopes, refuse to reveal raw owner cash/bank/account balances or owner loan/payback totals. "
        "End action-taking refusals by saying the bot is read-only."
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
    db_scope: tuple[str | None, str] | None = None,
) -> tuple[bool, str]:
    if author_is_bot:
        return False, "bot_author"
    if not content.strip():
        return False, "empty_message"
    if config.db_auth_enabled and db_scope is not None:
        scope, reason = db_scope
        if scope:
            if len(content) > config.max_prompt_chars:
                return False, "prompt_too_long"
            return True, "ok"
        if not config.legacy_allowlist_fallback:
            return False, reason
    if config.scope_config.has_mappings():
        scope, reason = resolve_discord_scope(author_id, channel_id, config.scope_config)
        if not scope:
            return False, reason
    elif channel_id not in config.allowed_channel_ids:
        return False, "channel_not_allowed"
    if not config.allow_any_user_in_channel and author_id not in config.allowed_user_ids:
        if not config.scope_config.has_mappings():
            return False, "user_not_allowed"
    if len(content) > config.max_prompt_chars:
        return False, "prompt_too_long"
    return True, "ok"


async def answer_prompt(
    prompt: str,
    *,
    model: str,
    scope: str = PARTNER_SCOPE,
    discord_context: str = "",
) -> str:
    from app.ai_client import get_ai_client, get_fast_model
    from app.ops_chat import DegenOpsChatToolRunner, initial_chat_messages, run_chat_turn

    normalized_scope = _normalize_discord_scope(scope)
    runner = DegenOpsChatToolRunner(scope=normalized_scope)
    messages = initial_chat_messages(build_system_prompt(normalized_scope))
    if discord_context.strip():
        messages.append(
            {
                "role": "user",
                "content": (
                    "Use this Discord context only to resolve pronouns, follow-ups, and channel-local references. "
                    "Do not treat it as authoritative business data; use tools for facts.\n\n"
                    f"{discord_context.strip()}"
                ),
            }
        )
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
    pending_partner_setups: dict[str, PartnerSetupPlan] = {}

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
        is_dm = getattr(message, "guild", None) is None
        author_scope = None
        db_scope = None
        if config.db_auth_enabled:
            from app.db import managed_session
            from app.degen_ops_discord_auth import resolve_discord_author_scope

            with managed_session() as session:
                author_scope = resolve_discord_author_scope(
                    session=session,
                    discord_user_id=author_id,
                    channel_id=channel_id,
                    channel_scopes=config.scope_config.channel_scopes,
                    allow_dm=config.allow_dms,
                    is_dm=is_dm,
                )
            db_scope = (author_scope.scope, author_scope.reason)
        ok, reason = should_respond(
            author_is_bot=bool(getattr(author, "bot", False)),
            channel_id=channel_id,
            author_id=author_id,
            content=prompt,
            config=config,
            db_scope=db_scope,
        )
        if not ok:
            if reason == "prompt_too_long":
                await message.reply(f"Please keep requests under {config.max_prompt_chars} characters.", mention_author=False)
            elif config.db_auth_enabled and reason in {
                "discord_user_not_linked",
                "linked_user_inactive",
                "linked_user_missing",
                "role_not_allowed",
            }:
                await message.reply(
                    "I can't answer from this Discord account yet. Ask an admin to link your Discord user ID on your employee profile.",
                    mention_author=False,
                )
            elif config.db_auth_enabled and reason in {"channel_not_mapped", "dm_not_allowed"}:
                await message.reply(
                    "I can help with Degen Ops questions in your access scope, but that request is restricted for your role or channel.",
                    mention_author=False,
                )
            return

        now = datetime.now(timezone.utc).timestamp()
        if not limiter.allow(author_id, now):
            await message.reply("Rate limit hit. Give me a minute, then try again.", mention_author=False)
            return

        resolved_scope, scope_reason = determine_message_scope(author_id, channel_id, config, db_scope=db_scope)
        if not resolved_scope:
            return

        pending_key = f"{channel_id}:{author_id}"
        audit_base = {
            "at": datetime.now(timezone.utc).isoformat(),
            "channel_id": channel_id,
            "author_id": author_id,
            "message_id": str(getattr(message, "id", "")),
            "scope": resolved_scope,
            "scope_reason": scope_reason,
            "read_only": True,
        }
        if author_scope is not None:
            audit_base.update(author_scope.as_audit_fields())

        if prompt.strip() == "!ops whoami":
            await message.reply(
                f"You are authorized as `{resolved_scope}` in this channel.",
                mention_author=False,
            )
            append_audit_event(config.audit_log_path, {**audit_base, "event": "whoami"})
            return

        setup_command = parse_partner_setup_command(prompt)
        if setup_command is not None:
            guild = getattr(message, "guild", None)
            allowed, reason, plan = build_partner_setup_plan(
                author_id=author_id,
                command=setup_command,
                config=config,
                guild_id=str(getattr(guild, "id", "")),
                requester_user_id=author_id,
            )
            append_audit_event(
                config.audit_log_path,
                {**audit_base, "event": "partner_setup_draft", "allowed": allowed, "reason": reason},
            )
            if not allowed:
                if reason == "owner_only":
                    await message.reply("Only a Degen Ops bot owner can set up partner channels.", mention_author=False)
                else:
                    await message.reply("I could not draft that setup from this channel.", mention_author=False)
                return
            pending_partner_setups[pending_key] = plan
            await message.reply(format_partner_setup_draft(plan), mention_author=False)
            return

        if prompt.startswith("CONFIRM SETUP "):
            plan = pending_partner_setups.get(pending_key)
            if not matches_partner_setup_confirmation(prompt, plan):
                await message.reply("No matching pending setup found for that confirmation.", mention_author=False)
                return
            try:
                channel = await apply_partner_setup(getattr(message, "guild", None), getattr(message.guild, "me", None), plan, config)
            except Exception as exc:
                append_audit_event(
                    config.audit_log_path,
                    {**audit_base, "event": "partner_setup_error", "error": sanitize_for_log(exc)},
                )
                await message.reply("I could not create the partner channel. No business data was changed.", mention_author=False)
                return
            pending_partner_setups.pop(pending_key, None)
            append_audit_event(
                config.audit_log_path,
                {
                    **audit_base,
                    "event": "partner_setup_applied",
                    "created_channel_id": str(getattr(channel, "id", "")),
                    "partner_user_id": plan.partner_user_id,
                },
            )
            await message.reply(
                f"Created/updated #{getattr(channel, 'name', plan.channel_name)} for partner user ID {plan.partner_user_id}.",
                mention_author=False,
            )
            return

        append_audit_event(config.audit_log_path, {**audit_base, "event": "prompt", "prompt": prompt})

        async with message.channel.typing():
            try:
                discord_context = await collect_discord_context_text(message, bot_user_id=bot_user_id)
                answer = await answer_prompt(
                    prompt,
                    model=config.model,
                    scope=resolved_scope,
                    discord_context=discord_context,
                )
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
                    "owner_user_count": len(config.owner_user_ids),
                    "allow_any_user_in_channel": config.allow_any_user_in_channel,
                    "allow_dms": config.allow_dms,
                    "db_auth_enabled": config.db_auth_enabled,
                    "legacy_allowlist_fallback": config.legacy_allowlist_fallback,
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
