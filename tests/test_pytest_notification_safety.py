"""Guards that pytest cannot inherit live notification credentials."""
from __future__ import annotations

import os

from app.config import get_settings


def test_pytest_forces_external_notifications_to_dry_run() -> None:
    get_settings.cache_clear()
    settings = get_settings()

    assert settings.sms_provider == "dry_run"
    assert settings.password_reset_email_provider == "dry_run"
    assert settings.sms_twilio_account_sid == ""
    assert settings.sms_twilio_auth_token == ""
    assert settings.sms_twilio_messaging_service_sid == ""
    assert settings.sms_from_number == ""
    assert settings.discord_bot_token == ""
    assert settings.team_supply_discord_bot_token == ""

    assert os.environ.get("DEGEN_OPS_DISCORD_BOT_TOKEN", "") == ""
    assert os.environ.get("TELEGRAM_BOT_TOKEN", "") == ""
    assert os.environ.get("TELEGRAM_ALERT_CHAT_ID", "") == ""
    assert os.environ.get("TELEGRAM_ALERT_TOPIC_ID", "") == ""
