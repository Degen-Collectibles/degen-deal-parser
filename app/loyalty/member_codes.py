"""Opaque member codes for a customer's rewards card. Disabled by default.

A code is derived from a server secret, the shop, the Shopify customer and a
generation number, so it can be reproduced when a wallet card is re-rendered
while the database holds only its hash. It never contains the customer ID,
a phone number or any other contact detail.
"""
import base64
import hashlib
import hmac
import re

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from .models import LoyaltyMemberCode as MemberCode, now_utc
from .pos_auth import POSAccess

PREFIX = 'DGR1'
PATTERN = re.compile(r'DGR1[A-Z2-7]{26}')
CUSTOMER = re.compile(r'[1-9][0-9]{0,15}')


class MemberCodeError(ValueError):
    pass


def gates(settings):
    """Reasons scanning is unavailable; empty when it may run."""
    if not getattr(settings, 'loyalty_member_scan_enabled', False):
        return ['member_scan_disabled']
    reasons = []
    if not 32 <= len(settings.loyalty_member_code_secret.get_secret_value().encode()) <= 512:
        reasons.append('member_code_secret_unset')
    try:
        POSAccess.from_settings(settings)
    except ValueError:
        reasons.append('pos_read_unavailable')
    return reasons


def _secret(settings):
    if gates(settings):
        raise MemberCodeError('member_scan_unavailable')
    return settings.loyalty_member_code_secret.get_secret_value().encode()


def _customer(customer):
    customer = str(customer)
    if not CUSTOMER.fullmatch(customer) or int(customer) > 9007199254740991:
        raise MemberCodeError('invalid_customer')
    return f'gid://shopify/Customer/{customer}'


def _derive(secret, shop, customer_gid, generation):
    digest = hmac.digest(secret, f'degen-member-code-v1|{shop}|{customer_gid}|{generation}'.encode(), 'sha256')
    return PREFIX + base64.b32encode(digest[:16]).decode().rstrip('=')


def _hash(code):
    return hashlib.sha256(code.encode()).hexdigest()


def _current(session, shop, customer_gid):
    """Newest generation for the customer, whether or not it is revoked."""
    return session.exec(select(MemberCode).where(MemberCode.shop == shop, MemberCode.customer_id == customer_gid)
                        .order_by(MemberCode.generation.desc()).limit(1)).first()


def issue(engine, settings, shop, customer):
    """Return the customer's current code, creating one if none is active."""
    secret = _secret(settings)
    gid = _customer(customer)
    for _ in range(3):
        with Session(engine) as session:
            row = _current(session, shop, gid)
            if row and row.revoked_at is None:
                return _derive(secret, shop, gid, row.generation)
            generation = row.generation + 1 if row else 1
            code = _derive(secret, shop, gid, generation)
            session.add(MemberCode(shop=shop, customer_id=gid, generation=generation, code_hash=_hash(code)))
            try:
                session.commit()
                return code
            except IntegrityError:
                # A concurrent issue took this generation; read it back.
                session.rollback()
    raise MemberCodeError('member_code_busy')


def revoke(engine, shop, customer):
    """Revoke the customer's active code. Returns True when one was revoked."""
    gid = _customer(customer)
    with Session(engine) as session:
        row = _current(session, shop, gid)
        if not row or row.revoked_at is not None:
            return False
        row.revoked_at = now_utc()
        session.add(row)
        session.commit()
        return True


def resolve(engine, settings, shop, code):
    """Numeric Shopify customer ID for an active code, else None."""
    secret = _secret(settings)
    if not isinstance(code, str) or not PATTERN.fullmatch(code):
        raise MemberCodeError('invalid_code')
    with Session(engine) as session:
        row = session.exec(select(MemberCode).where(MemberCode.shop == shop, MemberCode.code_hash == _hash(code))).first()
        if not row or row.revoked_at is not None:
            return None
        # A stored hash alone is not enough: the code must also be the one this
        # server derives, and no newer generation may have replaced it.
        if not hmac.compare_digest(_derive(secret, shop, row.customer_id, row.generation), code):
            return None
        if _current(session, shop, row.customer_id).id != row.id:
            return None
        return row.customer_id.rsplit('/', 1)[1]
