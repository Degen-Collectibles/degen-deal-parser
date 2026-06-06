from pathlib import Path

from scripts.degen_ops_discord_bot import (
    BotConfig,
    PromptRateLimiter,
    sanitize_for_log,
    should_respond,
    split_discord_message,
    strip_bot_mention,
)


def _config(**overrides):
    base = {
        "token": "token",
        "allowed_channel_ids": {"123"},
        "allowed_user_ids": {"42"},
        "allow_any_user_in_channel": False,
        "model": "aws/anthropic/claude-haiku-4-5-v1",
        "max_prompt_chars": 100,
        "rate_limit_per_minute": 2,
        "audit_log_path": Path("logs/test.jsonl"),
        "dry_run": True,
    }
    base.update(overrides)
    return BotConfig(**base)


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
