from __future__ import annotations

from types import SimpleNamespace

from app.team.email import EmailSendResult


class _FakeResponse:
    status_code = 200
    text = "{}"

    def json(self):
        return {"id": "discord-message-1"}


def _settings(**overrides):
    base = {
        "public_base_url": "https://ops.degencollectibles.com",
        "team_request_alert_email_to": "degencollectiblesllc@gmail.com",
        "team_request_alert_email_enabled": True,
        "team_supply_discord_enabled": True,
        "team_supply_discord_channel_id": "1373938191593246801",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_supply_request_alert_sends_email_and_discord_with_urgency_and_name(monkeypatch):
    from app.team import request_alerts

    emails = []
    discord_posts = []

    def fake_send_email(**kwargs):
        emails.append(kwargs)
        return EmailSendResult(provider="smtp", status="sent")

    def fake_post(url, *, headers, json, timeout):
        discord_posts.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        return _FakeResponse()

    monkeypatch.setenv("DEGEN_OPS_DISCORD_BOT_TOKEN", "bot-token")
    monkeypatch.setattr(request_alerts, "send_email", fake_send_email)
    monkeypatch.setattr(request_alerts.httpx, "post", fake_post)

    result = request_alerts.send_supply_request_alert(
        request_id=123,
        employee_name="Alice Nguyen",
        employee_username="alice",
        title="Top loaders",
        description="Need 3 cases before Friday",
        urgency="high",
        settings=_settings(),
    )

    assert result.email.status == "sent"
    assert result.discord.status == "sent"
    assert emails[0]["to_email"] == "degencollectiblesllc@gmail.com"
    assert "high" in emails[0]["subject"].lower()
    assert "Alice Nguyen" in emails[0]["subject"]
    assert "Top loaders" in emails[0]["subject"]
    assert "Need 3 cases before Friday" in emails[0]["body"]
    assert discord_posts[0]["url"].endswith("/channels/1373938191593246801/messages")
    assert discord_posts[0]["headers"]["Authorization"] == "Bot bot-token"
    assert "HIGH" in discord_posts[0]["json"]["content"]
    assert "Alice Nguyen" in discord_posts[0]["json"]["content"]
    assert "Top loaders" in discord_posts[0]["json"]["content"]


def test_timeoff_request_alert_sends_email_without_discord(monkeypatch):
    from app.team import request_alerts

    emails = []

    def fake_send_email(**kwargs):
        emails.append(kwargs)
        return EmailSendResult(provider="smtp", status="sent")

    def fail_discord(*args, **kwargs):
        raise AssertionError("time-off requests must not post to Discord")

    monkeypatch.setattr(request_alerts, "send_email", fake_send_email)
    monkeypatch.setattr(request_alerts.httpx, "post", fail_discord)

    result = request_alerts.send_timeoff_request_alert(
        request_id=55,
        employee_name="Ben Carter",
        employee_username="ben",
        start_date="2026-06-20",
        end_date="2026-06-22",
        reason="Family trip",
        settings=_settings(team_supply_discord_enabled=True),
    )

    assert result.email.status == "sent"
    assert result.discord.status == "not_requested"
    assert emails[0]["to_email"] == "degencollectiblesllc@gmail.com"
    assert "Ben Carter" in emails[0]["subject"]
    assert "2026-06-20" in emails[0]["subject"]
    assert "Family trip" in emails[0]["body"]
