"""External alerts for employee portal requests."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Optional

import httpx

from ..config import Settings, get_settings
from .email import EmailSendResult, send_email


DISCORD_API_BASE = "https://discord.com/api/v10"
DEFAULT_ALERT_EMAIL = "degencollectiblesllc@gmail.com"
DEFAULT_SUPPLY_CHANNEL_ID = "1373938191593246801"


@dataclass(frozen=True)
class DiscordSendResult:
    provider: str = "discord"
    status: str = "not_requested"
    message_id: str = ""
    error: str = ""

    @property
    def success(self) -> bool:
        return not self.error


@dataclass(frozen=True)
class RequestAlertResult:
    email: EmailSendResult
    discord: DiscordSendResult


def _clean(value: object, *, limit: int = 700) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _display_employee(name: str, username: str) -> str:
    clean_name = _clean(name, limit=120)
    clean_username = _clean(username, limit=80)
    if clean_name and clean_username and clean_name != clean_username:
        return f"{clean_name} (@{clean_username})"
    return clean_name or clean_username or "Unknown employee"


def _absolute_url(path: str, settings: Settings) -> str:
    cleaned = (path or "/team/").strip() or "/team/"
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        return cleaned
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    base = (getattr(settings, "public_base_url", "") or "https://ops.degencollectibles.com").rstrip("/")
    return f"{base}{cleaned}"


def _email_disabled_result() -> EmailSendResult:
    return EmailSendResult(provider="team_request_alert", status="not_requested", dry_run=True)


def _send_alert_email(
    *,
    subject: str,
    body: str,
    settings: Settings,
) -> EmailSendResult:
    if not bool(getattr(settings, "team_request_alert_email_enabled", True)):
        return _email_disabled_result()
    recipient = (
        getattr(settings, "team_request_alert_email_to", "")
        or os.getenv("TEAM_REQUEST_ALERT_EMAIL_TO", "")
        or DEFAULT_ALERT_EMAIL
    ).strip()
    if not recipient:
        return _email_disabled_result()
    try:
        result = send_email(
            to_email=recipient,
            subject=subject,
            body=body,
            settings=settings,
        )
    except Exception as exc:  # pragma: no cover - defensive against provider bugs
        result = EmailSendResult(
            provider="team_request_alert",
            status="send_error",
            error=str(exc)[:240],
        )
    if result.error:
        print(f"[team_request_alerts] email alert failed: {result.status} {result.error[:160]}")
    return result


def _discord_token(settings: Settings) -> str:
    candidates = (
        getattr(settings, "team_supply_discord_bot_token", ""),
        os.getenv("TEAM_SUPPLY_DISCORD_BOT_TOKEN", ""),
        os.getenv("DEGEN_OPS_DISCORD_BOT_TOKEN", ""),
        getattr(settings, "discord_bot_token", ""),
        os.getenv("DISCORD_BOT_TOKEN", ""),
    )
    for candidate in candidates:
        token = str(candidate or "").strip()
        if token:
            return token
    return ""


def _send_discord_message(
    *,
    content: str,
    settings: Settings,
) -> DiscordSendResult:
    if not bool(getattr(settings, "team_supply_discord_enabled", True)):
        return DiscordSendResult(status="not_requested")
    channel_id = (
        getattr(settings, "team_supply_discord_channel_id", "")
        or os.getenv("TEAM_SUPPLY_DISCORD_CHANNEL_ID", "")
        or DEFAULT_SUPPLY_CHANNEL_ID
    ).strip()
    if not channel_id:
        return DiscordSendResult(status="not_configured", error="Discord channel ID is not configured.")
    token = _discord_token(settings)
    if not token:
        return DiscordSendResult(status="not_configured", error="Discord bot token is not configured.")

    try:
        response = httpx.post(
            f"{DISCORD_API_BASE}/channels/{channel_id}/messages",
            headers={
                "Authorization": f"Bot {token}",
                "Content-Type": "application/json",
                "User-Agent": "DegenTeamAlerts/1.0",
            },
            json={"content": _clean(content, limit=1900)},
            timeout=float(getattr(settings, "team_request_alert_timeout_seconds", 10.0) or 10.0),
        )
    except Exception as exc:
        result = DiscordSendResult(status="send_error", error=str(exc)[:240])
        print(f"[team_request_alerts] Discord alert failed: {result.status} {result.error[:160]}")
        return result

    if response.status_code < 200 or response.status_code >= 300:
        result = DiscordSendResult(
            status=f"http_{response.status_code}",
            error=(response.text or "")[:240],
        )
        print(f"[team_request_alerts] Discord alert failed: {result.status} {result.error[:160]}")
        return result

    message_id = ""
    try:
        payload = response.json()
        message_id = str(payload.get("id") or "")
    except ValueError:
        message_id = ""
    return DiscordSendResult(status="sent", message_id=message_id)


def send_supply_request_alert(
    *,
    request_id: Optional[int],
    employee_name: str = "",
    employee_username: str = "",
    title: str = "",
    description: str = "",
    urgency: str = "normal",
    settings: Optional[Settings] = None,
) -> RequestAlertResult:
    settings = settings or get_settings()
    employee = _display_employee(employee_name, employee_username)
    urgency_text = _clean(urgency or "normal", limit=40).lower() or "normal"
    title_text = _clean(title or "Untitled request", limit=180)
    description_text = _clean(description, limit=1200)
    queue_url = _absolute_url("/team/admin/supply", settings)
    request_line = f"Request ID: {request_id}" if request_id is not None else "Request ID: pending"
    subject = f"[Degen] Supply request: {urgency_text} - {employee} - {title_text}"
    body = "\n".join(
        part
        for part in (
            "New supply request",
            "",
            f"Employee: {employee}",
            f"Urgency: {urgency_text}",
            f"Item: {title_text}",
            f"Description: {description_text}" if description_text else "",
            request_line,
            f"Open queue: {queue_url}",
        )
        if part != ""
    )
    discord_content = "\n".join(
        part
        for part in (
            "**New Supply Request**",
            f"Employee: {employee}",
            f"Urgency: {urgency_text.upper()}",
            f"Item: {title_text}",
            f"Description: {description_text}" if description_text else "",
            request_line,
            f"Queue: {queue_url}",
        )
        if part != ""
    )
    email_result = _send_alert_email(subject=subject, body=body, settings=settings)
    discord_result = _send_discord_message(content=discord_content, settings=settings)
    return RequestAlertResult(email=email_result, discord=discord_result)


def send_supply_ordered_alert(
    *,
    request_id: Optional[int],
    title: str = "",
    settings: Optional[Settings] = None,
) -> RequestAlertResult:
    settings = settings or get_settings()
    title_text = _clean(title or "supplies", limit=180)
    queue_url = _absolute_url("/team/admin/supply", settings)
    request_line = f"Request ID: {request_id}" if request_id is not None else "Request ID: pending"
    discord_content = "\n".join(
        part
        for part in (
            "**Supply Ordered**",
            f"Someone requested {title_text}, and we ordered it.",
            request_line,
            f"Queue: {queue_url}",
        )
        if part != ""
    )
    discord_result = _send_discord_message(content=discord_content, settings=settings)
    return RequestAlertResult(
        email=_email_disabled_result(),
        discord=discord_result,
    )


def send_timeoff_request_alert(
    *,
    request_id: Optional[int],
    employee_name: str = "",
    employee_username: str = "",
    start_date: date | str,
    end_date: date | str,
    reason: str = "",
    settings: Optional[Settings] = None,
) -> RequestAlertResult:
    settings = settings or get_settings()
    employee = _display_employee(employee_name, employee_username)
    start_text = _clean(start_date, limit=40)
    end_text = _clean(end_date, limit=40)
    reason_text = _clean(reason, limit=1200)
    queue_url = _absolute_url("/team/admin/timeoff", settings)
    request_line = f"Request ID: {request_id}" if request_id is not None else "Request ID: pending"
    subject = f"[Degen] Time-off request: {employee} - {start_text} to {end_text}"
    body = "\n".join(
        part
        for part in (
            "New time-off request pending approval",
            "",
            f"Employee: {employee}",
            f"Dates: {start_text} to {end_text}",
            f"Reason: {reason_text}" if reason_text else "",
            request_line,
            f"Open queue: {queue_url}",
        )
        if part != ""
    )
    email_result = _send_alert_email(subject=subject, body=body, settings=settings)
    return RequestAlertResult(
        email=email_result,
        discord=DiscordSendResult(status="not_requested"),
    )


def send_password_reset_manager_request_alert(
    *,
    request_id: Optional[int],
    employee_name: str = "",
    employee_username: str = "",
    reason: str = "",
    settings: Optional[Settings] = None,
) -> RequestAlertResult:
    settings = settings or get_settings()
    employee = _display_employee(employee_name, employee_username)
    reason_text = _clean(reason or "manual_reset_needed", limit=240)
    queue_url = _absolute_url("/team/admin/password-reset-requests", settings)
    request_line = f"Request ID: {request_id}" if request_id is not None else "Request ID: pending"
    subject = f"[Degen] Password reset help needed - {employee}"
    body = "\n".join(
        part
        for part in (
            "Password reset help needed",
            "",
            f"Employee: {employee}",
            f"Reason: {reason_text}",
            request_line,
            f"Open queue: {queue_url}",
        )
        if part != ""
    )
    email_result = _send_alert_email(subject=subject, body=body, settings=settings)
    return RequestAlertResult(
        email=email_result,
        discord=DiscordSendResult(status="not_requested"),
    )


def send_help_request_alert(
    *,
    request_id: Optional[int],
    employee_name: str = "",
    employee_username: str = "",
    message: str = "",
    page_path: str = "",
    settings: Optional[Settings] = None,
) -> RequestAlertResult:
    settings = settings or get_settings()
    employee = _display_employee(employee_name, employee_username)
    message_text = _clean(message, limit=2000)
    page_text = _clean(page_path or "/team/help", limit=240) or "/team/help"
    page_url = _absolute_url(page_text, settings)
    request_line = f"Request ID: {request_id}" if request_id is not None else "Request ID: pending"
    subject = f"[Degen] Help request - {employee}"
    body = "\n".join(
        part
        for part in (
            "New employee help request",
            "",
            f"Employee: {employee}",
            f"Page: {page_text}",
            f"Message: {message_text}" if message_text else "",
            request_line,
            f"Open page: {page_url}",
        )
        if part != ""
    )
    email_result = _send_alert_email(subject=subject, body=body, settings=settings)
    return RequestAlertResult(
        email=email_result,
        discord=DiscordSendResult(status="not_requested"),
    )
