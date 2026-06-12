import os
from pathlib import Path

from scripts.degen_ops_discord_bot import (
    BotConfig,
    DiscordScopeConfig,
    PromptRateLimiter,
    build_system_prompt,
    determine_message_scope,
    build_partner_setup_plan,
    ensure_database_url_from_readonly_env,
    format_partner_setup_draft,
    load_config_from_env,
    matches_partner_setup_confirmation,
    parse_partner_setup_command,
    parse_scope_config,
    resolve_discord_scope,
    sanitize_for_log,
    should_respond,
    split_discord_message,
    strip_bot_mention,
    update_env_allowlist,
)


def _config(**overrides):
    base = {
        "token": "token",
        "allowed_channel_ids": {"123"},
        "allowed_user_ids": {"42"},
        "owner_user_ids": {"42"},
        "allow_any_user_in_channel": False,
        "scope_config": DiscordScopeConfig(),
        "model": "aws/anthropic/claude-haiku-4-5-v1",
        "max_prompt_chars": 100,
        "rate_limit_per_minute": 2,
        "audit_log_path": Path("logs/test.jsonl"),
        "config_env_path": None,
        "dry_run": True,
    }
    base.update(overrides)
    return BotConfig(**base)


def test_parse_scope_config_from_json_maps_users_and_channels():
    config = parse_scope_config(
        '{"users":{"42":"owner","99":"partner"},"channels":{"111":"owner","222":"partner"}}'
    )

    assert config.user_scopes == {"42": "owner", "99": "partner"}
    assert config.channel_scopes == {"111": "owner", "222": "partner"}


def test_scope_resolution_uses_channel_maximum_and_denies_unknowns():
    scope_config = DiscordScopeConfig(
        user_scopes={"42": "owner", "99": "partner"},
        channel_scopes={"111": "owner", "222": "partner"},
    )

    assert resolve_discord_scope("42", "111", scope_config) == ("owner", "ok")
    assert resolve_discord_scope("42", "222", scope_config) == ("partner", "ok")
    assert resolve_discord_scope("99", "111", scope_config) == ("partner", "ok")
    assert resolve_discord_scope("99", "333", scope_config) == (None, "channel_not_mapped")
    assert resolve_discord_scope("77", "111", scope_config) == (None, "user_not_mapped")


def test_should_respond_accepts_scoped_user_without_legacy_allowed_user_list():
    config = _config(
        allowed_channel_ids=set(),
        allowed_user_ids=set(),
        scope_config=DiscordScopeConfig(
            user_scopes={"42": "owner"},
            channel_scopes={"111": "owner"},
        ),
    )

    ok, reason = should_respond(
        author_is_bot=False,
        channel_id="111",
        author_id="42",
        content="!ops whoami",
        config=config,
    )

    assert ok is True
    assert reason == "ok"


def test_should_respond_denies_unknown_scoped_channel():
    config = _config(
        allowed_channel_ids=set(),
        allowed_user_ids=set(),
        scope_config=DiscordScopeConfig(
            user_scopes={"42": "owner"},
            channel_scopes={"111": "owner"},
        ),
    )

    ok, reason = should_respond(
        author_is_bot=False,
        channel_id="999",
        author_id="42",
        content="hello",
        config=config,
    )

    assert ok is False
    assert reason == "channel_not_mapped"


def test_build_system_prompt_mentions_resolved_scope():
    owner_prompt = build_system_prompt("owner")
    partner_prompt = build_system_prompt("partner")

    assert "Use owner scope only." in owner_prompt
    assert "Use partner scope only." in partner_prompt
    assert "private #degen-ops-bot Discord channel" not in owner_prompt


def test_determine_message_scope_defaults_legacy_allowed_channels_to_partner():
    assert determine_message_scope("42", "123", _config()) == ("partner", "legacy_partner")


def test_determine_message_scope_uses_explicit_map_when_configured():
    config = _config(
        scope_config=DiscordScopeConfig(
            user_scopes={"42": "owner"},
            channel_scopes={"111": "owner"},
        )
    )

    assert determine_message_scope("42", "111", config) == ("owner", "ok")


def test_parse_partner_setup_command_extracts_name_and_user_id():
    command = parse_partner_setup_command("setup partner Andrew user_id=206237952412483584")

    assert command is not None
    assert command.partner_name == "Andrew"
    assert command.partner_user_id == "206237952412483584"
    assert command.channel_slug == "andrew"


def test_partner_setup_draft_requires_owner_and_confirmation():
    config = _config(owner_user_ids={"42"})
    command = parse_partner_setup_command("setup partner Andrew user_id=206237952412483584")

    allowed, reason, plan = build_partner_setup_plan(
        author_id="42",
        command=command,
        config=config,
        guild_id="999",
        requester_user_id="42",
    )

    assert allowed is True
    assert reason == "ok"
    assert plan is not None
    assert plan.confirmation_phrase == "CONFIRM SETUP ANDREW"
    assert plan.category_name == "Degen Ops Partners"
    assert plan.channel_name == "degen-ops-andrew"


def test_partner_setup_rejects_non_owner():
    config = _config(owner_user_ids={"42"})
    command = parse_partner_setup_command("setup partner Andrew user_id=206237952412483584")

    allowed, reason, plan = build_partner_setup_plan(
        author_id="99",
        command=command,
        config=config,
        guild_id="999",
        requester_user_id="99",
    )

    assert allowed is False
    assert reason == "owner_only"
    assert plan is None


def test_partner_setup_draft_lists_private_channel_actions():
    command = parse_partner_setup_command("setup partner Andrew user_id=206237952412483584")
    _allowed, _reason, plan = build_partner_setup_plan(
        author_id="42",
        command=command,
        config=_config(owner_user_ids={"42"}),
        guild_id="999",
        requester_user_id="42",
    )

    draft = format_partner_setup_draft(plan)

    assert "#degen-ops-andrew" in draft
    assert "Degen Ops Partners" in draft
    assert "206237952412483584" in draft
    assert "CONFIRM SETUP ANDREW" in draft


def test_partner_setup_confirmation_must_match_exact_plan_phrase():
    command = parse_partner_setup_command("setup partner Andrew user_id=206237952412483584")
    _allowed, _reason, plan = build_partner_setup_plan(
        author_id="42",
        command=command,
        config=_config(owner_user_ids={"42"}),
        guild_id="999",
        requester_user_id="42",
    )

    assert matches_partner_setup_confirmation("CONFIRM SETUP ANDREW", plan) is True
    assert matches_partner_setup_confirmation("confirm setup andrew", plan) is False
    assert matches_partner_setup_confirmation("CONFIRM SETUP BRIAN", plan) is False


def test_update_env_allowlist_updates_only_allowed_channel_and_user_lines(tmp_path):
    env_path = tmp_path / "bot.env"
    env_path.write_text(
        "\n".join(
            [
                "DEGEN_OPS_DISCORD_BOT_TOKEN=do-not-touch",
                "DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS=111,222",
                "DEGEN_OPS_DISCORD_ALLOWED_USER_IDS=42",
                "OTHER=value",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    update_env_allowlist(env_path, channel_ids={"222", "333"}, user_ids={"42", "99"})

    text = env_path.read_text(encoding="utf-8")
    assert "DEGEN_OPS_DISCORD_BOT_TOKEN=do-not-touch" in text
    assert "DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS=111,222,333" in text
    assert "DEGEN_OPS_DISCORD_ALLOWED_USER_IDS=42,99" in text
    assert "OTHER=value" in text


def test_should_respond_only_in_allowed_channel_for_allowed_user():
    ok, reason = should_respond(
        author_is_bot=False,
        channel_id="123",
        author_id="42",
        content="Should we buy this lot?",
        config=_config(),
    )

    assert ok is True
    assert reason == "ok"


def test_should_reject_unapproved_channel_user_bot_and_long_prompt():
    for kwargs, expected in [
        ({"author_is_bot": True}, "bot_author"),
        ({"channel_id": "999"}, "channel_not_allowed"),
        ({"author_id": "99"}, "user_not_allowed"),
        ({"content": ""}, "empty_message"),
        ({"content": "x" * 101}, "prompt_too_long"),
    ]:
        values = {
            "author_is_bot": False,
            "channel_id": "123",
            "author_id": "42",
            "content": "hello",
            "config": _config(),
        }
        values.update(kwargs)
        ok, reason = should_respond(**values)
        assert ok is False
        assert reason == expected


def test_allow_any_user_in_private_channel_is_explicit():
    ok, reason = should_respond(
        author_is_bot=False,
        channel_id="123",
        author_id="99",
        content="hello",
        config=_config(allowed_user_ids=set(), allow_any_user_in_channel=True),
    )

    assert ok is True
    assert reason == "ok"


def test_strip_bot_mention_removes_direct_mentions():
    assert strip_bot_mention("<@12345> should we buy this?", 12345) == "should we buy this?"
    assert strip_bot_mention("<@!12345> should we buy this?", 12345) == "should we buy this?"


def test_split_discord_message_keeps_chunks_under_limit():
    chunks = split_discord_message(("word " * 1000).strip(), limit=100)

    assert len(chunks) > 1
    assert all(len(chunk) <= 100 for chunk in chunks)


def test_rate_limiter_limits_per_user():
    limiter = PromptRateLimiter(limit_per_minute=2)

    assert limiter.allow("42", 1000.0) is True
    assert limiter.allow("42", 1001.0) is True
    assert limiter.allow("42", 1002.0) is False
    assert limiter.allow("99", 1002.0) is True
    assert limiter.allow("42", 1062.0) is True


def test_sanitize_for_log_redacts_common_secret_shapes():
    text = sanitize_for_log(
        "postgresql+psycopg://user:pass@host/db token=tokval api_key=keyval sk-abcdef123456"
    )

    assert "pass" not in text
    assert "tokval" not in text
    assert "keyval" not in text
    assert "sk-abcd" in text
    assert "123456" not in text


def test_load_config_dry_run_allows_missing_token(monkeypatch):
    monkeypatch.setenv("DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS", "123")
    monkeypatch.setenv("DEGEN_OPS_DISCORD_ALLOWED_USER_IDS", "42")
    monkeypatch.delenv("DEGEN_OPS_DISCORD_BOT_TOKEN", raising=False)

    config = load_config_from_env(dry_run=True)

    assert config.dry_run is True
    assert config.allowed_channel_ids == {"123"}
    assert config.allowed_user_ids == {"42"}


def test_load_config_accepts_scope_map_without_legacy_allowlists(monkeypatch):
    monkeypatch.setenv(
        "DEGEN_OPS_DISCORD_ROLE_MAP",
        '{"users":{"42":"owner"},"channels":{"111":"owner"}}',
    )
    monkeypatch.delenv("DEGEN_OPS_DISCORD_ALLOWED_CHANNEL_IDS", raising=False)
    monkeypatch.delenv("DEGEN_OPS_DISCORD_ALLOWED_USER_IDS", raising=False)
    monkeypatch.delenv("DEGEN_OPS_DISCORD_BOT_TOKEN", raising=False)

    config = load_config_from_env(dry_run=True)

    assert config.scope_config.user_scopes == {"42": "owner"}
    assert config.scope_config.channel_scopes == {"111": "owner"}


def test_database_url_falls_back_to_readonly_env(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DEGEN_OPS_READONLY_DATABASE_URL", "postgresql://readonly/db")

    assert ensure_database_url_from_readonly_env() is True
    assert os.environ["DATABASE_URL"] == "postgresql://readonly/db"


def test_database_url_fallback_does_not_override_existing(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://existing/db")
    monkeypatch.setenv("DEGEN_OPS_READONLY_DATABASE_URL", "postgresql://readonly/db")

    assert ensure_database_url_from_readonly_env() is False
    assert os.environ["DATABASE_URL"] == "postgresql://existing/db"
