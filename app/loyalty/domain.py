"""Pure, exact, versioned earning rules over minimized canonical evidence.

Money is integer USD cents. Rounding happens once on cumulative NET per order.
No fallback to floats, current subtotals, contact matching or receipt timestamps.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

RULE_VERSION = 'pos-usd-whole-v1'
MAX_CENTS = 1_000_000_000_000


class Review(ValueError):
    """A fixed reason code safe to log/display; never include provider payloads."""


def whole(value, *, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_CENTS:
        raise Review('invalid_integer_evidence')
    return value


def cents(value):
    if not isinstance(value, (str, Decimal, int)) or isinstance(value, bool):
        raise Review('invalid_money')
    try:
        amount = Decimal(value)
        scaled = amount * 100
        if not amount.is_finite() or not 0 <= scaled <= MAX_CENTS or scaled != scaled.to_integral_value():
            raise Review('invalid_money')
        return int(scaled)
    except (InvalidOperation, ValueError, OverflowError):
        raise Review('invalid_money') from None


def gid(resource, value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise Review('invalid_identifier')
    text = str(value)
    prefix = f'gid://shopify/{resource}/'
    if text.startswith(prefix):
        text = text[len(prefix):]
    if not re.fullmatch(r'[0-9]{1,20}', text) or int(text) <= 0 or int(text) > 2**64-1:
        raise Review('invalid_identifier')
    return prefix + str(int(text))


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(parsed, datetime) or parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError):
        raise Review('invalid_timestamp') from None


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class Policy:
    shop_domain: str
    shop_id: str
    locations: frozenset[str]
    launch_at: datetime

    def digest(self):
        return fingerprint([self.shop_domain, self.shop_id, sorted(self.locations), timestamp(self.launch_at).isoformat(), RULE_VERSION])


@dataclass(frozen=True)
class Entitlement:
    state: str
    reason: str
    net_cents: int = 0
    points: int = 0
    basis_hash: str = ''


def calculate(order, policy, attachment):
    """Return entitlement or raise Review. Unsupported cases never imply zero credit."""
    try:
        return _calculate(order, policy, attachment)
    except (KeyError, TypeError, IndexError, AttributeError):
        raise Review('incomplete_evidence') from None


def _calculate(o, policy, attachment):
    if o['shop_id'] != policy.shop_id:
        raise Review('shop_mismatch')
    gid('Order', o['id'])
    if not isinstance(o['source'], str) or not o['source']:
        raise Review('missing_channel')
    if o['source'] != 'pos':
        return Entitlement('excluded', 'channel_excluded')
    for key in ('test', 'edited', 'returns', 'exchanges', 'taxes_included'):
        if type(o[key]) is not bool:
            raise Review('incomplete_evidence')
    if o['test']:
        return Entitlement('excluded', 'test_order')
    location = gid('Location', o['location_id'])
    if location not in policy.locations:
        return Entitlement('excluded', 'location_excluded')
    created, processed, updated = [timestamp(o[k]) for k in ('created_at','processed_at','updated_at')]
    if min(created, processed) < timestamp(policy.launch_at):
        return Entitlement('excluded', 'prelaunch')
    if updated < created:
        raise Review('timestamp_conflict')
    if o['edited'] or o['exchanges']:
        raise Review('edit_or_exchange_review')
    if o['currency'] != 'USD':
        raise Review('unsupported_currency')
    total = whole(o['total_cents'])
    received = whole(o['received_cents'])
    payments = o['payments']
    if not isinstance(payments, list) or not payments:
        raise Review('payment_unverified')
    seen = set(); paid = 0; paid_rounding = 0; paid_times = []
    for p in payments:
        pid = gid('OrderTransaction', p['id'])
        if pid in seen:
            raise Review('duplicate_transaction')
        seen.add(pid)
        if p['kind'] in ('SALE', 'CAPTURE'):
            if p['status'] in ('FAILURE', 'ERROR'):
                continue  # A terminal failed attempt is not a payment.
            if p['status'] != 'SUCCESS':
                raise Review('payment_unverified')
            paid += whole(p['cents'])
            paid_rounding += whole(p['rounding_cents'], minimum=-MAX_CENTS)
            paid_times.append(timestamp(p['processed_at']))
        elif p['kind'] not in ('AUTHORIZATION', 'VOID', 'REFUND'):
            raise Review('payment_unverified')
    rounding = whole(o['payment_rounding_cents'], minimum=-MAX_CENTS)
    if (not paid_times or paid_rounding != rounding or paid != received
        or received not in (total, total + rounding)
        or whole(o['outstanding_cents']) != 0
        or o['financial_status'] not in ('PAID', 'PARTIALLY_REFUNDED', 'REFUNDED')):
        raise Review('payment_unverified')
    if min(paid_times) < policy.launch_at:
        return Entitlement('excluded', 'prelaunch')
    paid_at = max(paid_times)
    if paid_at < created or updated < paid_at:
        raise Review('timestamp_conflict')
    if o['customer_id'] is None:
        return Entitlement('pending', 'customer_unattached')
    customer = gid('Customer', o['customer_id'])
    if not attachment or attachment.get('kind') not in ('canonical_observation', 'verified_attachment'):
        raise Review('attachment_timing_unverified')
    if gid('Customer', attachment['customer_id']) != customer:
        raise Review('identity_reassignment')
    attached_at = timestamp(attachment['at'])
    if not created <= attached_at <= paid_at + timedelta(days=7):
        raise Review('attachment_outside_window')
    if not isinstance(o['lines'], list) or not o['lines']:
        raise Review('missing_lines')
    lines = {}; basis = []; eligible = 0; all_line_gross = 0
    for line in o['lines']:
        lid = gid('LineItem', line['id'])
        if lid in lines:
            raise Review('duplicate_line')
        whole(line['quantity'], minimum=1)
        original = whole(line['original_cents'])
        if not isinstance(line['discounts_cents'], list):
            raise Review('discounts_incomplete')
        discounts = sum(whole(v) for v in line['discounts_cents'])
        tax = whole(line['tax_cents'])
        net = original - discounts - (tax if o['taxes_included'] else 0)
        if net < 0:
            raise Review('invalid_line_basis')
        all_line_gross += net + tax
        if type(line['gift_card']) is not bool or type(line['merchandise']) is not bool:
            raise Review('line_classification_unverified')
        if not line['gift_card'] and not line['merchandise']:
            # No catalog-name guessing for custom services, tips or fees.
            raise Review('line_classification_unverified')
        qualifying = not line['gift_card']
        if qualifying:
            eligible += net
        lines[lid] = {'net':net, 'refunded':0, 'quantity':line['quantity'], 'refunded_quantity':0, 'qualifying':qualifying}
        basis.append({**line, 'discounts_cents':sorted(line['discounts_cents'])})
    if all_line_gross > total:
        raise Review('line_total_mismatch')
    refund_ids = set(); refund_line_ids = set(); refund_tx_ids = set()
    for refund in o['refunds']:
        rid = gid('Refund', refund['id'])
        if rid in refund_ids:
            raise Review('duplicate_refund')
        refund_ids.add(rid)
        if refund['adjustments'] or refund['duties']:
            raise Review('refund_allocation_unverified')
        txs = refund['transactions']
        if not txs:
            raise Review('refund_payment_unverified')
        cash = 0; refund_rounding = 0
        for tx in txs:
            txid = gid('OrderTransaction', tx['id'])
            if txid in refund_tx_ids or tx['kind'] != 'REFUND' or tx['status'] != 'SUCCESS':
                raise Review('refund_payment_unverified')
            refund_tx_ids.add(txid)
            cash += whole(tx['cents'])
            refund_rounding += whole(tx['rounding_cents'], minimum=-MAX_CENTS)
        parts = refund['lines']
        subtotal = sum(whole(p['subtotal_cents']) for p in parts)
        tax = sum(whole(p['tax_cents']) for p in parts)
        shipping = whole(refund['shipping_cents']) + whole(refund['shipping_tax_cents'])
        # 2026-04 calls subtotalSet "subtotal" without a universal inclusive-tax
        # promise. Accept only an exact reimbursement identity. No approximation.
        interpretations=[]
        if cash in (subtotal + tax + shipping, subtotal + tax + shipping + refund_rounding):
            interpretations.append(False)
        if o['taxes_included'] and tax and cash in (subtotal + shipping, subtotal + shipping + refund_rounding):
            interpretations.append(True)
        if len(interpretations)!=1:
            raise Review('refund_allocation_unverified')
        included=interpretations[0]
        for part in parts:
            rlid = gid('RefundLineItem', part['id'])
            lid = gid('LineItem', part['line_id'])
            if rlid in refund_line_ids or lid not in lines:
                raise Review('refund_line_unverified')
            refund_line_ids.add(rlid)
            item = lines[lid]
            amount = whole(part['subtotal_cents']) - (whole(part['tax_cents']) if included else 0)
            item['refunded_quantity'] += whole(part['quantity'])
            item['refunded'] += amount
            if amount < 0 or item['refunded'] > item['net'] or item['refunded_quantity'] > item['quantity']:
                raise Review('refund_exceeds_original')
            if item['qualifying']:
                eligible -= amount
    if sum(whole(t['rounding_cents'],minimum=-MAX_CENTS) for r in o['refunds'] for t in r['transactions']) != whole(o['refund_rounding_cents'],minimum=-MAX_CENTS):
        raise Review('refund_rounding_mismatch')
    basis_hash = fingerprint({'lines':sorted(basis,key=lambda x:x['id']), 'taxes_included':o['taxes_included'],
        'total_cents':total, 'created_at':created.isoformat(), 'processed_at':processed.isoformat(),
        'location_id':location, 'source':o['source'], 'currency':o['currency']})
    return Entitlement('eligible', 'calculated', eligible, (eligible+50)//100, basis_hash)
