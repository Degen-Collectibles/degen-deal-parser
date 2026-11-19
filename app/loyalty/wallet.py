"""Rewards-card sign-up links issued at the register. Disabled by default.

The cashier opens a customer; the register shows a QR code holding a
single-use link that expires in ten minutes. The customer scans it with their
own phone, so no sign-in or contact detail is involved. The link token is
random, stored only as a hash, and bound to the customer on the cashier's
screen.
"""
from datetime import timedelta
import hashlib
import io
import re
import secrets

from sqlalchemy import delete, update
from sqlmodel import Session, select

from . import apple_pass, member_codes
from .models import LoyaltyWalletCard as Card, LoyaltyWalletEnrollment as Enrollment, now_utc
from .queries import ledger_snapshot

LIFETIME = timedelta(minutes=10)
KEEP = timedelta(days=1)
TOKEN = re.compile(r'[A-Za-z0-9_-]{43}')
BASE = re.compile(r'https://[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:[0-9]{1,5})?')
PLATFORMS = ('apple', 'google')


class WalletError(ValueError):
    pass


def gates(settings):
    """Reasons card sign-up is unavailable; empty when it may run."""
    if not getattr(settings, 'loyalty_wallet_enrollment_enabled', False):
        return ['wallet_enrollment_disabled']
    reasons = list(member_codes.gates(settings))
    if not BASE.fullmatch(getattr(settings, 'loyalty_card_base_url', '') or ''):
        reasons.append('card_base_url_unset')
    return reasons


def platforms(settings):
    """Wallet platforms that can issue a card with the current settings."""
    return [] if apple_pass.gates(settings) else ['apple']


def _hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def _require(settings):
    if gates(settings):
        raise WalletError('wallet_enrollment_unavailable')


def link(settings, token):
    return f'{settings.loyalty_card_base_url}/rewards/card/{token}'


def create(engine, settings, shop, customer, session_hash, *, now=None):
    """New link for the customer; any earlier unused link of theirs stops working."""
    _require(settings)
    gid = member_codes._customer(customer)
    now = now or now_utc()
    token = secrets.token_urlsafe(32)
    with Session(engine) as session:
        session.exec(delete(Enrollment).where(Enrollment.expires_at < now - KEEP))
        session.exec(update(Enrollment).where(Enrollment.shop == shop, Enrollment.customer_id == gid,
                                              Enrollment.used_at.is_(None), Enrollment.expires_at > now)
                     .values(used_at=now, platform='replaced'))
        session.add(Enrollment(shop=shop, customer_id=gid, token_hash=_hash(token), session_hash=session_hash,
                               created_at=now, expires_at=now + LIFETIME))
        session.commit()
    return token, now + LIFETIME


def _live(token, shop, now):
    return (Enrollment.token_hash == _hash(token), Enrollment.shop == shop,
            Enrollment.used_at.is_(None), Enrollment.expires_at > now)


def valid(engine, settings, token, *, now=None):
    """True while the link can still be used. Reveals nothing about the customer."""
    _require(settings)
    if not isinstance(token, str) or not TOKEN.fullmatch(token):
        return False
    with Session(engine) as session:
        return session.exec(select(Enrollment.id).where(*_live(token, settings.loyalty_shop_domain, now or now_utc()))).first() is not None


def consume(engine, settings, token, platform, *, now=None):
    """Use the link once. Returns the numeric Shopify customer ID, else None."""
    _require(settings)
    if platform not in PLATFORMS:
        raise WalletError('invalid_platform')
    if not isinstance(token, str) or not TOKEN.fullmatch(token):
        return None
    now = now or now_utc()
    with Session(engine) as session:
        # One statement decides the winner when two phones use the same link.
        result = session.exec(update(Enrollment).where(*_live(token, settings.loyalty_shop_domain, now))
                              .values(used_at=now, platform=platform))
        if result.rowcount != 1:
            session.rollback()
            return None
        customer = session.exec(select(Enrollment.customer_id).where(Enrollment.token_hash == _hash(token))).one()
        session.commit()
        return customer.rsplit('/', 1)[1]


def issue(engine, settings, token, platform, *, now=None):
    """Use the link and record a new card. Returns its details, else None.

    The balance is read before the link is used, so a failed read leaves the
    link working. The card's serial is random and says nothing about the customer.
    """
    _require(settings)
    if platform not in platforms(settings):
        raise WalletError('platform_unavailable')
    if not isinstance(token, str) or not TOKEN.fullmatch(token):
        return None
    shop = settings.loyalty_shop_domain
    with Session(engine) as session:
        gid = session.exec(select(Enrollment.customer_id).where(*_live(token, shop, now or now_utc()))).first()
    if gid is None:
        return None
    snapshot = ledger_snapshot(engine, shop, gid.rsplit('/', 1)[1], limit=1)
    if snapshot is None:
        raise WalletError('wallet_enrollment_unavailable')
    points = int(snapshot[0]['balance_points'])
    customer = consume(engine, settings, token, platform, now=now)
    if customer is None:
        return None
    code = member_codes.issue(engine, settings, shop, customer)
    serial = secrets.token_hex(16)
    with Session(engine) as session:
        session.add(Card(shop=shop, customer_id=gid, platform=platform, serial=serial))
        session.commit()
    return {'serial': serial, 'code': code, 'points': points}


def qr_png(text):
    """PNG of a QR code for the text, sized for a register screen."""
    import qrcode
    code = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=10, border=4)
    code.add_data(text)
    code.make(fit=True)
    output = io.BytesIO()
    code.make_image(fill_color='black', back_color='white').save(output, format='PNG')
    return output.getvalue()
