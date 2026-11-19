"""Versioned, integer-only checkout reward terms. No environment writes."""
from dataclasses import dataclass
from .domain import gid, whole


# Approved terms as (points, discount cents, minimum merchandise cents). A discount
# is never more than 20% of the minimum. Changing these needs owner sign-off.
APPROVED_TERMS = ((250, 500, 2500), (500, 1000, 5000), (1000, 2500, 12500))


@dataclass(frozen=True)
class Reward:
    points: int = 500
    discount_cents: int = 1000
    minimum_cents: int = 5000
    version: str = 'fixed-usd-physical-custom-v5'

    def __post_init__(self):
        whole(self.points, minimum=1)
        whole(self.discount_cents, minimum=1)
        whole(self.minimum_cents, minimum=1)
        object.__setattr__(self, 'version', f'fixed-usd-physical-custom-v5-p{self.points}-d{self.discount_cents}-m{self.minimum_cents}')
        if self.discount_cents * 5 > self.minimum_cents:
            raise ValueError('reward_exceeds_twenty_percent')

    @classmethod
    def approved(cls):
        """Every reward a cashier may issue, smallest first."""
        return tuple(cls(*terms) for terms in APPROVED_TERMS)

    @classmethod
    def for_points(cls, points):
        # bool is an int in Python; a reward is chosen by an exact whole point cost.
        if type(points) is int:
            for reward in cls.approved():
                if reward.points == points:
                    return reward
        raise ValueError('reward_terms_not_approved')

    def public(self):
        return dict(points=self.points, discount_cents=self.discount_cents,
                    minimum_cents=self.minimum_cents, version=self.version)


def redemption_gates(settings, *, issuing=True):
    # Cashier writes are deliberately independent of the existing all-staff READ grant.
    if issuing and not getattr(settings, 'loyalty_redemption_enabled', False):
        return ['redemption_disabled']
    reasons = []
    if issuing:
        from .config import posting_gates
        reasons.extend(posting_gates(settings))
    if not getattr(settings, 'loyalty_redemption_all_staff_enabled', False):
        reasons.append('redemption_staff_policy_unset')
    if not getattr(settings, 'loyalty_redemption_provider_verified', False):
        reasons.append('discount_provider_unverified')
    try:
        from .config import shop_domain
        shop_domain(settings)
        gid('Shop', settings.loyalty_shop_id)
        locations=[v.strip() for v in settings.loyalty_location_ids.split(',') if v.strip()]
        if not locations:raise ValueError()
        for location in locations:gid('Location',location)
        from .pos_auth import POSAccess
        POSAccess.from_settings(settings)
        Reward.approved()
    except (AttributeError, ValueError):
        reasons.append('reward_policy_unset')
    return reasons


def variant_ids(values, *, allow_empty=False):
    """Bounded catalog IDs only. No client prices or classification claims."""
    if not isinstance(values, list) or not (0 if allow_empty else 1) <= len(values) <= 100:
        raise ValueError('invalid_variants')
    if any(not isinstance(v, str) or not v.startswith('gid://shopify/ProductVariant/') for v in values):
        raise ValueError('invalid_variants')
    result = sorted(gid('ProductVariant', v) for v in values)
    if len(set(result)) != len(result):
        raise ValueError('invalid_variants')
    return result
