"""Small email sending adapter for employee portal password reset links."""
from __future__ import annotations

import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formataddr
from typing import Optional

from ..config import Settings, get_settings
from .fingerprints import keyed_fingerprint


@dataclass(frozen=True)
class EmailSendResult:
    provider: str
    status: str
    message_id: str = ""
    dry_run: bool = False
    error: str = ""

    @property
    def success(self) -> bool:
        return not self.error


def mask_email_address(email_address: str) -> str:
    raw = (email_address or "").strip().lower()
    if "@" not in raw:
        return "***"
    local, domain = raw.split("@", 1)
    if not local or not domain:
        return "***"
    return f"{local[:1]}***@{domain}"


def email_address_fingerprint(email_address: str) -> str:
    normalized = (email_address or "").strip().lower()
    return keyed_fingerprint(
        normalized,
        namespace="email",
        length=16,
        settings=get_settings(),
    )


def send_email(
    *,
    to_email: str,
    subject: str,
    body: str,
    settings: Optional[Settings] = None,
) -> EmailSendResult:
    settings = settings or get_settings()
    provider = (getattr(settings, "password_reset_email_provider", "dry_run") or "dry_run").strip().lower()
    if provider in {"", "dryrun", "dry_run", "log", "console"}:
        return EmailSendResult(provider="dry_run", status="dry_run", dry_run=True)
    if provider in {"disabled", "off", "none"}:
        return EmailSendResult(
            provider=provider,
            status="disabled",
            error="PASSWORD_RESET_EMAIL_PROVIDER is disabled.",
        )
    if provider != "smtp":
        return EmailSendResult(
            provider=provider,
            status="unsupported_provider",
            error=f"Unsupported PASSWORD_RESET_EMAIL_PROVIDER: {provider}",
        )

    host = (getattr(settings, "password_reset_smtp_host", "") or "").strip()
    port = int(getattr(settings, "password_reset_smtp_port", 587) or 587)
    username = (getattr(settings, "password_reset_smtp_username", "") or "").strip()
    password = getattr(settings, "password_reset_smtp_password", "") or ""
    from_email = (getattr(settings, "password_reset_email_from", "") or "").strip() or username
    from_name = (getattr(settings, "password_reset_email_from_name", "") or "").strip()
    timeout = float(getattr(settings, "password_reset_email_timeout_seconds", 10.0) or 10.0)
    use_starttls = bool(getattr(settings, "password_reset_smtp_starttls", True))
    use_ssl = bool(getattr(settings, "password_reset_smtp_ssl", False))

    if not host or not from_email:
        return EmailSendResult(
            provider="smtp",
            status="not_configured",
            error="PASSWORD_RESET_SMTP_HOST and PASSWORD_RESET_EMAIL_FROM are required.",
        )
    to_clean = (to_email or "").strip()
    if "@" not in to_clean or to_clean.startswith("@") or to_clean.endswith("@"):
        return EmailSendResult(
            provider="smtp",
            status="invalid_recipient",
            error="Recipient email is invalid.",
        )

    message = EmailMessage()
    message["To"] = to_clean
    message["From"] = formataddr((from_name, from_email)) if from_name else from_email
    message["Subject"] = subject
    message.set_content(body)

    try:
        smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        with smtp_cls(host, port, timeout=timeout) as client:
            if use_starttls and not use_ssl:
                client.starttls()
            if username or password:
                client.login(username, password)
            client.send_message(message)
    except (OSError, smtplib.SMTPException) as exc:
        return EmailSendResult(
            provider="smtp",
            status="send_error",
            error=str(exc)[:240],
        )
    return EmailSendResult(provider="smtp", status="sent")
