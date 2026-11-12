"""Read-only deployment readiness for the features configured on this web process."""
from .schema import schema_ready
from .config import shop_domain, receiving_gates, processing_gates, posting_gates, policy_from_settings
from .pos_auth import POSAccess
from .redemption import Redemptions
from .reward_policy import redemption_gates
from .service import Store


def ready(engine, settings, state):
    flags = ('loyalty_pos_read_enabled', 'loyalty_receiving_enabled',
             'loyalty_processing_enabled', 'loyalty_posting_enabled', 'loyalty_redemption_enabled')
    if not any(getattr(settings, flag, False) for flag in flags):
        return True
    try:
        if not schema_ready(engine): return False
        if settings.loyalty_pos_read_enabled:
            POSAccess.from_settings(settings); shop_domain(settings)
        if settings.loyalty_receiving_enabled and receiving_gates(settings): return False
        if settings.loyalty_processing_enabled:
            if processing_gates(settings): return False
            task = getattr(state, 'loyalty_task', None)
            if task is None or task.done(): return False
        if settings.loyalty_posting_enabled and posting_gates(settings): return False
        if settings.loyalty_processing_enabled or settings.loyalty_posting_enabled:
            rewards = Redemptions(Store(engine), policy_getter=lambda: policy_from_settings(settings))
            if not rewards.issuance_ready(settings.loyalty_shop_domain): return False
        if settings.loyalty_redemption_enabled and redemption_gates(settings): return False
        recovery = not redemption_gates(settings, issuing=False)
        if settings.loyalty_processing_enabled or recovery:
            from ..inventory.shopify import resolve_shopify_access_token
            if not resolve_shopify_access_token(settings): return False
        if recovery:
            task = getattr(state, 'loyalty_redemption_task', None)
            if task is None or task.done(): return False
        return True
    except Exception:
        # No configuration, customer data, or provider errors on public health.
        return False
