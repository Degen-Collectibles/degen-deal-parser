"""Post-commit SMS delivery. Uncertain attempts are never automatically resent."""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import timedelta
from urllib.parse import urlsplit

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from ..config import get_settings
from ..models import SmsOutbox, SmsDispatchLease, EmployeeProfile, utcnow
from .pii import decrypt_pii, PIIDecryptError
from .sms import send_sms, normalize_sms_phone, sms_phone_fingerprint, mask_sms_phone
from .sms_consent import consent_context

log = logging.getLogger(__name__)
MAX_ATTEMPTS = 3
MAX_PER_HOUR = 10
CALLBACK_PATH = "/webhooks/twilio/status/"


def callback_base(settings) -> str:
    base = settings.sms_callback_base_url.rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path:
        return ""
    return base


def enqueue(session: Session, notification, phone: str) -> SmsOutbox:
    # Flush binds the intent to the notification; both roll back together.
    session.flush()
    state = consent_context(session, notification.target_user_id)
    now = utcnow()
    row = SmsOutbox(notification_id=notification.id, user_id=notification.target_user_id,
                    phone_binding=state["phone_binding"], phone_fingerprint=sms_phone_fingerprint(phone),
                    phone_label=mask_sms_phone(phone), expires_at=now + timedelta(hours=1))
    session.add(row)
    return row


def ready(settings) -> bool:
    return bool(settings.sms_operational_alerts_enabled and settings.sms_dispatcher_enabled
                and settings.sms_webhooks_enabled and settings.sms_provider == "twilio"
                and settings.sms_twilio_account_sid and settings.sms_twilio_auth_token
                and (settings.sms_from_number or settings.sms_twilio_messaging_service_sid)
                and callback_base(settings))


def dispatch_one(engine) -> bool:
    settings = get_settings()
    if not ready(settings):
        return False
    owner = uuid.uuid4().hex
    now = utcnow()
    # A short DB lease serializes dispatchers across processes on both engines.
    with Session(engine) as session:
        if session.get(SmsDispatchLease, 1) is None:
            session.add(SmsDispatchLease())
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
        claimed = session.exec(update(SmsDispatchLease).where(
            SmsDispatchLease.id == 1, SmsDispatchLease.until <= now
        ).values(owner=owner, until=now + timedelta(minutes=2)))
        session.commit()
        if claimed.rowcount != 1:
            return False
    try:
        return _dispatch_claimed(engine, settings)
    finally:
        with Session(engine) as session:
            session.exec(update(SmsDispatchLease).where(SmsDispatchLease.id == 1,
                SmsDispatchLease.owner == owner).values(until=utcnow()))
            session.commit()


def _dispatch_claimed(engine, settings) -> bool:
    now = utcnow()
    with Session(engine) as session:
        # A process may die after Twilio accepted but before the SID was stored.
        session.exec(update(SmsOutbox).where(SmsOutbox.status == "dispatching",
            SmsOutbox.updated_at < now - timedelta(minutes=3)).values(
                status="unknown", error_code="interrupted_attempt", updated_at=now))
        row = session.exec(select(SmsOutbox).where(SmsOutbox.status.in_(("queued", "retry")),
            SmsOutbox.next_attempt_at <= now).order_by(SmsOutbox.id).limit(1)).first()
        if row is None:
            session.commit()
            return False
        state = consent_context(session, row.user_id)
        if row.expires_at.replace(tzinfo=now.tzinfo) <= now:
            row.status, row.error_code = "expired", "stale_alert"
        elif not state["opted_in"] or state["phone_binding"] != row.phone_binding:
            row.status, row.error_code = "cancelled", "consent_or_phone_changed"
        else:
            # Counts accepted/uncertain attempts as well as in-flight ones.
            recent = session.exec(select(SmsOutbox).where(
                SmsOutbox.phone_fingerprint == row.phone_fingerprint,
                SmsOutbox.attempts > 0, SmsOutbox.updated_at > now - timedelta(hours=1))).all()
            if len(recent) >= MAX_PER_HOUR:
                row.next_attempt_at = now + timedelta(minutes=5)
                row.error_code = "rate_limited"
                session.add(row)
                session.commit()
                return False
            row.status = "dispatching"
            row.attempts += 1
            row.attempt_token = uuid.uuid4().hex
        row.updated_at = now
        session.add(row)
        session.commit()
        row_id, token = row.id, row.attempt_token
        if row.status != "dispatching":
            return True
    # Crucially: no connection to the originating business transaction.
    with Session(engine) as session:
        row = session.get(SmsOutbox, row_id)
        state = consent_context(session, row.user_id)
        if not ready(get_settings()) or not state["opted_in"] or state["phone_binding"] != row.phone_binding:
            row.status, row.error_code = "cancelled", "delivery_gate_closed"
            session.add(row)
            session.commit()
            return True
        profile = session.get(EmployeeProfile, row.user_id)
        try:
            phone = normalize_sms_phone(decrypt_pii(profile.phone_enc) or "")
        except (PIIDecryptError, ValueError):
            phone = None
        if not phone:
            row.status, row.error_code = "cancelled", "phone_unreadable"
            session.add(row)
            session.commit()
            return True
    message = ("Degen Collectibles: A team operations update is available.\n"
               f"{settings.public_base_url.rstrip('/')}/team/notifications\n"
               "Reply STOP to unsubscribe or HELP for help.")
    try:
        result = send_sms(to_phone=phone, body=message, settings=settings,
            status_callback=f"{callback_base(settings)}{CALLBACK_PATH}{token}")
    except Exception:
        # Do not log raw transport details/PII and never retry an unknown outcome.
        log.error("SMS dispatch failed; outcome requires reconciliation for outbox %s", row_id)
        from .sms import SmsSendResult
        result = SmsSendResult("twilio", "unknown", error="unexpected_transport_failure")
    with Session(engine) as session:
        row = session.get(SmsOutbox, row_id)
        if row.attempt_token != token or row.status not in {"dispatching", "unknown"}:
            return True  # A signed callback may already have finalized it.
        values = {"updated_at": utcnow()}
        if result.success and result.message_id and not result.dry_run:
            values.update(provider_sid=result.message_id,
                status=result.status if result.status in {"sent", "delivered", "failed", "undelivered", "canceled"} else "accepted", error_code="")
        elif result.status in {"not_connected", "http_429"}:
            values.update(status="retry" if row.attempts < MAX_ATTEMPTS else "failed", error_code=result.status,
                next_attempt_at=utcnow() + timedelta(seconds=30 * 2 ** (row.attempts - 1)))
        elif result.status.startswith("http_4") or result.status in {"disabled", "not_configured", "unsupported_provider"}:
            values.update(status="failed", error_code=result.error)
        else:
            values.update(status="unknown", error_code="provider_outcome_unknown")
        # Compare-and-set also covers a callback arriving after this SELECT.
        session.exec(update(SmsOutbox).where(SmsOutbox.id == row_id,
            SmsOutbox.attempt_token == token, SmsOutbox.status.in_(("dispatching", "unknown"))).values(**values))
        if result.error == "21610":
            from .sms_suppression import suppress
            suppress(session, row.phone_fingerprint, True)
        session.commit()
    return True


async def sms_dispatch_loop(stop_event):
    from ..db import engine
    while not stop_event.is_set():
        try:
            await asyncio.to_thread(dispatch_one, engine)
        except Exception:
            log.exception("SMS queue processing failed")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
