"""Signed Twilio callbacks and a read-only admin delivery log."""
from __future__ import annotations

import base64
import hashlib
import hmac
import re
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from ..config import get_settings
from ..db import get_session
from ..models import SmsOutbox, SmsSuppression, SmsInboundEvent, utcnow
from ..shared import templates
from ..team.sms import normalize_sms_phone, sms_phone_fingerprint
from ..team.sms_outbox import callback_base, ready
from ..team.sms_suppression import suppress
from .team_admin import _admin_gate

router = APIRouter()
SID = re.compile(r"SM[0-9a-fA-F]{32}\Z")
STOP_WORDS = {"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT", "REVOKE", "OPTOUT"}
START_WORDS = {"START", "YES", "UNSTOP"}


async def verified_form(request: Request) -> dict[str, str]:
    settings = get_settings()
    if not settings.sms_webhooks_enabled or not settings.sms_twilio_auth_token or not callback_base(settings):
        raise HTTPException(503, "SMS callbacks are disabled")
    if request.url.query or request.headers.get("content-type", "").split(";")[0] != "application/x-www-form-urlencoded":
        raise HTTPException(400, "Expected form callback without query parameters")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 16384:
            raise HTTPException(413, "Callback too large")
    try:
        fields = parse_qsl(body.decode("utf-8"), keep_blank_values=True, max_num_fields=100, errors="strict")
    except (ValueError, UnicodeError):
        raise HTTPException(400, "Invalid form encoding")
    params = dict(fields)
    if len(params) != len(fields):
        raise HTTPException(400, "Ambiguous form fields")
    # Pin the public origin; never trust Host or forwarded headers for signing.
    signed = callback_base(settings) + request.url.path
    signed += "".join(key + params[key] for key in sorted(params))
    digest = base64.b64encode(hmac.new(settings.sms_twilio_auth_token.encode(), signed.encode(), hashlib.sha1).digest()).decode()
    if not hmac.compare_digest(digest, request.headers.get("x-twilio-signature", "")):
        raise HTTPException(403, "Invalid provider signature")
    if params.get("AccountSid") != settings.sms_twilio_account_sid:
        raise HTTPException(403, "Wrong provider account")
    return params


@router.post("/webhooks/twilio/status/{attempt_token}")
async def delivery_status(attempt_token: str, request: Request, session: Session = Depends(get_session)):
    data = await verified_form(request)
    row = session.exec(select(SmsOutbox).where(SmsOutbox.attempt_token == attempt_token)).first()
    sid = data.get("MessageSid", "")
    phone = normalize_sms_phone(data.get("To", ""))
    if row is None or not SID.fullmatch(sid) or not phone or sms_phone_fingerprint(phone) != row.phone_fingerprint:
        raise HTTPException(400, "Unknown delivery")
    if row.provider_sid and row.provider_sid != sid:
        raise HTTPException(409, "Message identifier mismatch")
    status = data.get("MessageStatus", "")
    predecessors = {
        "accepted": {"dispatching", "unknown"},
        "queued": {"dispatching", "unknown"},
        "sending": {"dispatching", "unknown", "accepted"},
        "sent": {"dispatching", "unknown", "accepted", "sending"},
        "delivered": {"dispatching", "unknown", "accepted", "sending", "sent"},
        "undelivered": {"dispatching", "unknown", "accepted", "sending", "sent"},
        "failed": {"dispatching", "unknown", "accepted", "sending", "sent"},
        "canceled": {"dispatching", "unknown", "accepted", "sending"},
    }
    if status not in predecessors:
        raise HTTPException(400, "Unsupported delivery status")
    code = data.get("ErrorCode", "")
    code = code if code.isdigit() and len(code) <= 10 else ""
    if code == "21610" and row.error_code != code:
        suppress(session, row.phone_fingerprint, True)
    session.exec(update(SmsOutbox).where(SmsOutbox.id == row.id,
        SmsOutbox.attempt_token == attempt_token, SmsOutbox.status.in_(predecessors[status]),
        SmsOutbox.provider_sid.in_(("", sid))).values(
        provider_sid=sid, status="accepted" if status == "queued" else status,
        error_code=code, updated_at=utcnow()))
    session.commit()
    return Response(status_code=204)


@router.post("/webhooks/twilio/inbound")
async def inbound(request: Request, session: Session = Depends(get_session)):
    data = await verified_form(request)
    settings = get_settings()
    destination_ok = bool(settings.sms_from_number and data.get("To") == settings.sms_from_number)
    service_ok = bool(settings.sms_twilio_messaging_service_sid and data.get("MessagingServiceSid") == settings.sms_twilio_messaging_service_sid)
    phone = normalize_sms_phone(data.get("From", ""))
    sid = data.get("MessageSid", "")
    if not (destination_ok or service_ok) or not phone or not SID.fullmatch(sid):
        raise HTTPException(400, "Unknown sender or destination")
    body = data.get("Body", "").strip().upper()
    event = data.get("OptOutType", "").upper()
    if event not in {"STOP", "START", "HELP"}:
        event = "STOP" if body in STOP_WORDS else "START" if body in START_WORDS else "HELP" if body in {"HELP", "INFO"} else "OTHER"
    fingerprint = sms_phone_fingerprint(phone)
    session.add(SmsInboundEvent(message_sid=sid, phone_fingerprint=fingerprint, event=event))
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        return Response("<Response/>", media_type="application/xml")
    if event == "STOP":
        suppress(session, fingerprint, True)
    elif event == "START":
        suppress(session, fingerprint, False)
    session.commit()
    # Advanced Opt-Out already replies; avoid sending a duplicate response.
    text = ""
    if ready(settings) and not data.get("OptOutType"):
        if event == "HELP":
            text = "Degen Collectibles support: info@degencollectibles.com. Reply STOP to unsubscribe."
        elif event == "START":
            text = "Degen Collectibles: To subscribe, use Optional text alerts in your Degen Team profile. Reply HELP for help or STOP to unsubscribe."
    return Response(f"<Response><Message>{text}</Message></Response>" if text else "<Response/>", media_type="application/xml")


@router.get("/team/admin/sms")
def sms_log(request: Request, before: int = 0, session: Session = Depends(get_session)):
    denial, user = _admin_gate(request, session, "admin.permissions.view")
    if denial:
        return denial
    query = select(SmsOutbox).order_by(SmsOutbox.id.desc()).limit(100)
    if before > 0:
        query = query.where(SmsOutbox.id < before)
    rows = session.exec(query).all()
    return templates.TemplateResponse(request, "team/admin/sms.html", {
        "request": request, "current_user": user, "rows": rows, "title": "Text alerts",
        "delivery_ready": ready(get_settings()), "next_before": rows[-1].id if len(rows) == 100 else 0,
    })
