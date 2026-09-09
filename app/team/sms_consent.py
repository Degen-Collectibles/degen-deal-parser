"""Voluntary, phone-bound consent for the owners/managers SMS pilot.

Use the existing append-only audit trail; never infer consent from a phone
number, onboarding, a role grant, or the employee's general terms acceptance.
"""
from __future__ import annotations

import hashlib
import json

from sqlmodel import Session, select

from ..models import AuditLog, EmployeeProfile, User
from .pii import PIIDecryptError, decrypt_pii
from .sms import mask_sms_phone, normalize_sms_phone

CONSENT_ACTION = "sms.consent"
CONSENT_VERSION = "2026-09-08"
CONSENT_TEXT = (
    "Send me Degen Collectibles operational text alerts about schedules, shifts, "
    "and team operations. Optional; app access does not require consent. "
    "Message frequency varies. Message and data rates may apply. "
    "Reply STOP to unsubscribe or HELP for help."
)
PILOT_ROLES = frozenset({"admin", "manager"})


def consent_context(session: Session, user_id: int) -> dict:
    user = session.get(User, user_id)
    profile = session.get(EmployeeProfile, user_id)
    eligible = bool(user and user.is_active and user.role in PILOT_ROLES)
    phone = None
    if profile and profile.phone_enc:
        try:
            phone = normalize_sms_phone(decrypt_pii(profile.phone_enc) or "")
        except (PIIDecryptError, ValueError):
            pass
    # Binding to the encrypted record also invalidates consent when a phone is
    # changed and later changed back, including through an admin profile edit.
    binding = hashlib.sha256(profile.phone_enc).hexdigest() if phone else ""
    latest = session.exec(
        select(AuditLog).where(
            AuditLog.target_user_id == user_id,
            AuditLog.action == CONSENT_ACTION,
        ).order_by(AuditLog.id.desc()).limit(1)
    ).first()
    try:
        evidence = json.loads(latest.details_json) if latest else {}
    except (TypeError, ValueError):
        evidence = {}
    if not isinstance(evidence, dict):
        evidence = {}
    opted_in = bool(
        eligible and binding and evidence.get("opted_in") is True
        and evidence.get("version") == CONSENT_VERSION
        and evidence.get("phone_binding") == binding
        and latest.actor_user_id == user_id
    )
    return {
        "eligible": eligible, "opted_in": opted_in,
        "phone_ready": bool(phone), "phone_label": mask_sms_phone(phone) if phone else "",
        "phone_binding": binding, "version": CONSENT_VERSION, "text": CONSENT_TEXT,
    }


def record_consent(
    session: Session, *, user_id: int, opted_in: bool,
    version: str = "", phone_binding: str = "", ip_address: str | None = None,
) -> None:
    state = consent_context(session, user_id)
    if opted_in:
        if not state["eligible"]:
            raise ValueError("Text alerts are currently limited to active owners and managers.")
        if not state["phone_ready"]:
            raise ValueError("Save a valid phone number in Your info before opting in.")
        if version != CONSENT_VERSION or phone_binding != state["phone_binding"]:
            raise ValueError("Your phone or the consent terms changed. Refresh this page and try again.")
    # Withdrawals always append evidence, even after a role or phone change.
    session.add(AuditLog(
        actor_user_id=user_id, target_user_id=user_id,
        action=CONSENT_ACTION, resource_key="page.profile", ip_address=ip_address,
        details_json=json.dumps({
            "opted_in": opted_in, "version": CONSENT_VERSION,
            "text": CONSENT_TEXT if opted_in else "Unsubscribe from operational SMS",
            "phone_binding": state["phone_binding"] if opted_in else "",
            "phone_label": state["phone_label"], "source": "profile_sms_form",
        }),
    ))


def consent_allows_sms(session: Session, user_id: int, phone: str) -> bool:
    state = consent_context(session, user_id)
    if not state["opted_in"]:
        return False
    profile = session.get(EmployeeProfile, user_id)
    try:
        saved = normalize_sms_phone(decrypt_pii(profile.phone_enc) or "")
    except (PIIDecryptError, ValueError):
        return False
    return bool(saved and normalize_sms_phone(phone) == saved)
