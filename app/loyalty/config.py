"""Policy/activation gates. Unset business decisions are never synthesized."""
import re
from .domain import Policy, Review, gid, timestamp


def shop_domain(settings):
    value=settings.loyalty_shop_domain
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*\.myshopify\.com', value or ''):
        raise Review('loyalty_shop_unset')
    if value != settings.shopify_store_domain:
        raise Review('loyalty_transport_shop_mismatch')
    return value


def policy_from_settings(settings):
    domain=shop_domain(settings)
    if not settings.loyalty_location_ids or not settings.loyalty_launch_at:
        raise Review('loyalty_policy_unset')
    launch=timestamp(settings.loyalty_launch_at)
    if not settings.loyalty_launch_at.endswith(('Z','+00:00')):
        raise Review('loyalty_launch_must_be_utc')
    locations=frozenset(gid('Location',s.strip()) for s in settings.loyalty_location_ids.split(','))
    return Policy(domain,gid('Shop',settings.loyalty_shop_id),locations,launch)


def receiving_gates(settings):
    reasons=[]
    if not settings.loyalty_receiving_enabled: reasons.append('receiving_disabled')
    try: shop_domain(settings); gid('Shop',settings.loyalty_shop_id)
    except Review as exc: reasons.append(str(exc))
    if not settings.shopify_webhook_secret: reasons.append('webhook_secret_unset')
    if not settings.loyalty_receipt_retention_days: reasons.append('receipt_retention_unset')
    return reasons


def processing_gates(settings):
    reasons=[]
    if not settings.loyalty_processing_enabled: reasons.append('processing_disabled')
    try: policy_from_settings(settings)
    except Review as exc: reasons.append(str(exc))
    if not settings.loyalty_read_access_verified: reasons.append('read_access_unverified')
    if not settings.loyalty_evidence_retention_days: reasons.append('evidence_retention_unset')
    if settings.loyalty_budget_cents is None: reasons.append('spending_budget_unset')
    return reasons


def posting_gates(settings):
    reasons=processing_gates(settings)
    if not settings.loyalty_posting_enabled: reasons.append('posting_disabled')
    if not settings.loyalty_exception_owner.strip(): reasons.append('exception_owner_unset')
    if not settings.loyalty_ledger_retention_days: reasons.append('ledger_retention_unset')
    if not settings.loyalty_backup_retention_days: reasons.append('backup_retention_unset')
    return reasons
