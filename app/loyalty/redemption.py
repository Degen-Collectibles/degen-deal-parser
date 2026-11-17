"""Reservations and order-backed redemption entries in the existing immutable ledger.

Network evidence is obtained by the server-side provider, never supplied by POS.
An expired code is NOT proof of an abandoned sale (offline orders can arrive late).
Uncertain cancellations stay held for reviewed reconciliation.
"""
from datetime import timedelta
import asyncio
import json
import re
from uuid import uuid4
from sqlalchemy import func, or_, and_
from sqlmodel import Session, select
from .domain import gid, fingerprint
from .models import LoyaltyAccount as Account, LoyaltyEntitlement as Entitlement
from .models import LoyaltyLedger as Ledger, LoyaltyRedemption as Redemption
from .service import dump, naive, insert_missing, canonical_evidence
from .reward_policy import Reward

HOLD_STATES = ('reserved', 'ready', 'cancelling', 'review')
REWARD_REASONS = ('reward_redeemed', 'reward_restored')


def blocking_review():
    # A posted debit has already consumed this reservation. Review remains
    # visible in Ops, but must not become a second hold on future purchases.
    # Account identity holds and disputed earning entitlements remain separate.
    return and_(Redemption.state.in_(HOLD_STATES), Redemption.debit_ledger_id.is_(None))


class RedemptionError(ValueError):
    pass


def available(s, account):
    balance = int(s.exec(select(func.coalesce(func.sum(Ledger.delta), 0)).where(
        Ledger.shop == account.shop, Ledger.account_id == account.id)).one())
    held = int(s.exec(select(func.coalesce(func.sum(Redemption.points), 0)).where(
        Redemption.shop == account.shop, Redemption.account_id == account.id,
        Redemption.state.in_(HOLD_STATES), Redemption.debit_ledger_id.is_(None))).one())
    return balance, held


def disputed_earnings(s, account):
    return int(s.exec(select(func.coalesce(func.sum(Entitlement.posted_points), 0)).where(
        Entitlement.account_id == account.id, Entitlement.shop == account.shop,
        Entitlement.status == 'review', Entitlement.posted_points > 0)).one())


def receipt_evidence_current(s, row, evidence):
    if evidence is None:
        return row.order_id is None
    from .models import LoyaltyInbox
    refunds = {r['id'] for r in evidence.get('refunds', [])}
    updated = naive(evidence['updated_at']) if evidence.get('updated_at') else None
    receipts = s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop == row.shop,
        LoyaltyInbox.order_id == row.order_id))
    for receipt in receipts:
        event = json.loads(receipt.evidence_json)
        if event.get('refund_id') and event['refund_id'] not in refunds: return False
        if event.get('updated_at') and (updated is None or updated < naive(event['updated_at'])): return False
    return True


class Redemptions:
    def __init__(self, store, *, posting_allowed=lambda: True, policy_getter=None):
        self.store = store
        self.posting_allowed = posting_allowed
        self.policy_getter = policy_getter

    def require_ready(self, shop, session=None):
        if not self.posting_allowed():
            raise RedemptionError('posting_paused')
        if self.policy_getter is None:
            return
        def check(s):
            from .models import LoyaltyReconciliation
            policy = self.policy_getter()
            # Serialize with earning-policy updates before taking an account lock.
            insert_missing(s, LoyaltyReconciliation, dict(shop=shop), ['shop'])
            program = s.exec(select(LoyaltyReconciliation).where(
                LoyaltyReconciliation.shop == shop).with_for_update()).one()
            if policy.shop_domain != shop or (program.policy_hash and program.policy_hash != policy.digest()):
                raise RedemptionError('program_policy_changed')
            # Initial pin only; an existing digest is never silently migrated.
            if not program.policy_hash:
                program.policy_hash = policy.digest(); s.add(program)
        if session is not None:
            check(session)
        else:
            self.store.transaction(check)

    def issuance_ready(self, shop):
        # Read-only status: never initialize or change program policy here.
        try:
            if not self.posting_allowed(): return False
            if self.policy_getter is None: return True
            from .models import LoyaltyReconciliation
            policy = self.policy_getter()
            with Session(self.store.engine) as s:
                program = s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.shop == shop)).first()
                return policy.shop_domain == shop and (not program or not program.policy_hash or program.policy_hash == policy.digest())
        except (ValueError, AttributeError):
            return False

    def observe(self, shop, id, evidence):
        """Persist server-verified identity before eligibility or ledger posting.

        This records the actual observation time, never a fabricated attachment
        time. First discovery outside the attachment window still needs review.
        """
        order_id = gid('Order', evidence['order_id'])
        def operation(s):
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).one()
            if evidence['customer_id'] != row.customer_id or evidence['code'] != row.code:
                raise RedemptionError('order_identity_mismatch')
            if row.order_id and row.order_id != order_id:
                raise RedemptionError('order_conflict')
            row.order_id = order_id
            if 'canonical_order' in evidence:
                canonical = canonical_evidence(evidence['canonical_order'])
                if canonical['id'] != order_id or canonical['customer_id'] != row.customer_id or canonical['test']:
                    raise RedemptionError('order_identity_mismatch')
                previous = json.loads(row.observed_order_json)
                if previous:
                    updated, old_updated = naive(canonical['updated_at']), naive(previous['updated_at'])
                    if updated < old_updated: raise RedemptionError('stale_order_snapshot')
                    if updated == old_updated and previous != canonical: raise RedemptionError('same_timestamp_conflict')
                row.observed_order_json = dump(canonical)
                if row.manual_decision_hash and row.manual_decision_hash != fingerprint(canonical) and not row.restore_ledger_id:
                    row.state = 'review'; row.reason = 'manual_reward_evidence_changed'
            if row.first_observed_at is None: row.first_observed_at = naive()
            s.add(row)
            return row.first_observed_at
        return self.store.transaction(operation)

    def overview(self, shop, customer):
        customer = gid('Customer', customer)
        with Session(self.store.engine) as s:
            account = s.exec(select(Account).where(Account.shop == shop, Account.customer_id == customer)).first()
            if not account:
                return dict(balance_points=0, reserved_points=0, available_points=0, needs_review=False)
            balance, held = available(s, account)
            disputed = disputed_earnings(s, account)
            review=s.exec(select(Redemption.id).where(Redemption.account_id==account.id,
                Redemption.state=='review', blocking_review()).limit(1)).first() is not None
            return dict(balance_points=balance, reserved_points=held,
                        available_points=max(0, balance-held-disputed), needs_review=account.identity_hold or balance < 0 or review or disputed > 0)

    def reserve(self, shop, customer, session_hash, request_key, reward, *, now=None, eligibility=None):
        customer = gid('Customer', customer)
        if not re.fullmatch(r'[A-Za-z0-9_-]{16,80}', request_key or ''):
            raise RedemptionError('invalid_request_key')
        if not re.fullmatch(r'[a-f0-9]{64}', session_hash or ''):
            raise RedemptionError('invalid_session')
        if eligibility is not None:
            from .reward_policy import variant_ids
            custom = eligibility.get('custom_sales') is True
            if (set(eligibility) != ({'requested', 'eligible', 'custom_sales'} if custom else {'requested', 'eligible'})
                or variant_ids(eligibility['requested'], allow_empty=custom) != eligibility['requested']
                or variant_ids(eligibility['eligible'], allow_empty=custom) != eligibility['eligible']
                or (custom and eligibility['requested'] != eligibility['eligible'])
                or not set(eligibility['eligible']) <= set(eligibility['requested'])):
                raise RedemptionError('invalid_variants')
        current = naive(now).replace(microsecond=0)
        def operation(s):
            self.require_ready(shop, s)
            account = s.exec(select(Account).where(Account.shop == shop, Account.customer_id == customer).with_for_update()).first()
            if not account or account.identity_hold:
                raise RedemptionError('customer_unavailable')
            previous = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.request_key == request_key)).first()
            if previous:
                if (previous.customer_id != customer or previous.session_hash != session_hash
                    or (eligibility is not None and previous.eligibility_json != dump(eligibility))):
                    raise RedemptionError('request_key_conflict')
                return previous
            # One outstanding checkout per customer, including uncertain provider calls.
            active = s.exec(select(Redemption).where(Redemption.account_id == account.id,
                blocking_review())).first()
            if active:
                raise RedemptionError('redemption_already_pending')
            balance, held = available(s, account)
            if balance-held-disputed_earnings(s, account) < reward.points:
                raise RedemptionError('insufficient_points')
            row = Redemption(shop=shop, account_id=account.id, customer_id=customer,
                session_hash=session_hash, request_key=request_key, code='DEGEN-'+uuid4().hex.upper(),
                points=reward.points, discount_cents=reward.discount_cents,
                minimum_cents=reward.minimum_cents, rule_version=reward.version,
                eligibility_json=dump(eligibility or {}),
                created_at=current, expires_at=current+timedelta(minutes=30))
            s.add(row); s.flush()
            return row
        return self.store.transaction(operation)

    def get(self, shop, id, customer=None, session_hash=None):
        with Session(self.store.engine) as s:
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id)).first()
            if not row or (customer is not None and row.customer_id != gid('Customer', customer)) or (
                    session_hash is not None and row.session_hash != session_hash):
                raise RedemptionError('redemption_not_found')
            return row

    def owned_pending(self, shop, customer, session_hash):
        with Session(self.store.engine) as s:
            row=s.exec(select(Redemption).where(Redemption.shop==shop,
                Redemption.customer_id==gid('Customer',customer),Redemption.session_hash==session_hash,
                blocking_review()).order_by(Redemption.id.desc()).limit(1)).first()
            return dict(redemption_id=row.id,state=row.state,request_key=row.request_key,can_cancel=row.debit_ledger_id is None) if row else None

    def transition(self, shop, id, state, *, provider_id=None, reason='', now=None):
        allowed = {'ready': ('reserved', 'ready'), 'cancelling': ('reserved', 'ready', 'cancelling', 'review'),
                   'review': HOLD_STATES + ('committed',)}
        if state not in allowed:
            raise RedemptionError('invalid_transition')
        def operation(s):
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).one()
            if row.state not in allowed[state]:
                return row
            if state == 'cancelling' and row.debit_ledger_id:
                raise RedemptionError('reward_already_spent')
            if state == 'ready' and (not provider_id or naive(now) >= row.expires_at):
                raise RedemptionError('reward_expired')
            if provider_id:
                gid('DiscountCodeNode', provider_id)
                if row.provider_id and row.provider_id != provider_id:
                    raise RedemptionError('provider_identity_changed')
                row.provider_id = provider_id
            row.state = state; row.reason = reason; row.checked_at = naive(now)
            s.add(row)
            return row
        return self.store.transaction(operation)

    def settle(self, shop, id, evidence, *, now=None):
        """Trusted provider snapshot. The API has no endpoint accepting this evidence."""
        if not self.posting_allowed():
            raise RedemptionError('posting_paused')
        current = naive(now)
        order_id = gid('Order', evidence['order_id'])
        def operation(s):
            if self.policy_getter is not None:
                from .models import LoyaltyReconciliation
                policy = self.policy_getter()
                program = s.exec(select(LoyaltyReconciliation).where(
                    LoyaltyReconciliation.shop == shop).with_for_update()).first()
                if policy.shop_domain != shop or (program and program.policy_hash and program.policy_hash != policy.digest()):
                    raise RedemptionError('program_policy_changed')
            # Same ordering as the earning worker: entitlement -> account -> reservation.
            insert_missing(s, Entitlement, dict(shop=shop, order_id=order_id), ['shop','order_id'])
            order = s.exec(select(Entitlement).where(Entitlement.shop == shop, Entitlement.order_id == order_id).with_for_update()).one()
            if self.policy_getter is not None and order.policy_hash and order.policy_hash != policy.digest():
                raise RedemptionError('program_policy_changed')
            if order.lease_token and order.lease_until and order.lease_until > current:
                raise RedemptionError('order_busy')
            row0 = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id)).one()
            account = s.exec(select(Account).where(Account.id == row0.account_id, Account.shop == shop).with_for_update()).one()
            row = s.exec(select(Redemption).where(Redemption.id == id).with_for_update()).one()
            if row.manual_decision_hash and row.state != 'restored':
                if row.manual_decision_hash != fingerprint(json.loads(row.observed_order_json)):
                    raise RedemptionError('manual_reward_evidence_changed')
                return row
            if row.state == 'restored':
                if row.order_id != order_id: raise RedemptionError('order_conflict')
                return row
            released = row.state == 'cancelled' or row.review_cause == 'late_sale_after_reviewed_release' or row.reason == 'late_sale_after_reviewed_release'
            if row.order_id and row.order_id != order_id:
                raise RedemptionError('order_conflict')
            updated=naive(evidence['updated_at']) if evidence.get('updated_at') else current
            if row.order_updated_at and updated < row.order_updated_at:
                raise RedemptionError('stale_order_snapshot')
            if evidence['customer_id'] != row.customer_id or evidence['code'] != row.code:
                raise RedemptionError('order_identity_mismatch')
            if evidence['discount_cents'] != row.discount_cents or evidence['eligible_cents'] < row.minimum_cents:
                raise RedemptionError('order_discount_mismatch')
            if evidence['currency'] != 'USD' or evidence['stacked'] or evidence['test'] or not evidence['paid']:
                raise RedemptionError('order_not_eligible')
            if account.identity_hold or (order.account_id and order.account_id != account.id):
                raise RedemptionError('identity_review')
            order.account_id = account.id; order.customer_id = row.customer_id
            clean = {k:evidence[k] for k in ('order_id','customer_id','code','discount_cents','eligible_cents',
                                            'currency','stacked','test','paid','full_refund','partial_refund')}
            clean['reward'] = dict(points=row.points, version=row.rule_version)
            clean['resulting_points_semantics'] = 'purchase_earning_entitlement_only'
            clean.update({k:evidence[k] for k in ('basis_hash','refunds') if k in evidence})
            previous = json.loads(row.evidence_json)
            if previous:
                if row.order_updated_at == updated and previous != clean:
                    raise RedemptionError('same_timestamp_conflict')
                if previous.get('basis_hash') != clean.get('basis_hash'):
                    raise RedemptionError('original_basis_changed')
                refunds = {r['id']:r for r in clean.get('refunds', [])}
                if any(refunds.get(r['id']) != r for r in previous.get('refunds', [])):
                    raise RedemptionError('refund_history_changed')
                if previous['partial_refund'] and not (clean['partial_refund'] or clean['full_refund']):
                    raise RedemptionError('refund_history_changed')
            if order.basis_hash and clean.get('basis_hash') != order.basis_hash:
                raise RedemptionError('original_basis_changed')
            canonical = canonical_evidence(evidence['canonical_order']) if 'canonical_order' in evidence else None
            if canonical is not None:
                if order.source_updated_at and updated < order.source_updated_at:
                    raise RedemptionError('stale_order_snapshot')
                if order.source_updated_at == updated and order.evidence_hash and order.evidence_hash != fingerprint(canonical):
                    raise RedemptionError('same_timestamp_conflict')
                old_order = json.loads(order.evidence_json)
                incoming = {r['id']:r for r in canonical['refunds']}
                if any(incoming.get(r['id']) != r for r in old_order.get('refunds', [])):
                    raise RedemptionError('refund_history_changed')
                if row.debit_ledger_id and not previous.get('basis_hash') and not order.basis_hash:
                    raise RedemptionError('original_basis_changed')
            if evidence['full_refund'] and (order.manual_hold or order.identity_hold or order.status == 'review'):
                raise RedemptionError('earning_refund_review')
            if evidence['full_refund'] and order.posted_points:
                if not clean.get('basis_hash'):
                    raise RedemptionError('earning_refund_review')
                # Revoke purchase earnings under the same account/entitlement lock
                # before restoring the reward, irrespective of worker ordering.
                self.store.post(s, order, 0, 'refund', current,
                    business_key=f'redemption:{row.id}:earning_refund', proof=clean)
            if canonical is not None and evidence['full_refund']:
                # Fence older earning snapshots, including a delayed pre-refund
                # delivery after the reward worker revoked the purchase points.
                order.evidence_json = dump(canonical); order.evidence_hash = fingerprint(canonical)
                order.source_updated_at = updated; order.basis_hash = clean['basis_hash']
            def post(delta, reason):
                order.revision += 1
                entry = Ledger(shop=shop, account_id=account.id, entitlement_id=order.id,
                    revision=order.revision, business_key=f'redemption:{row.id}:{reason}', delta=delta,
                    resulting_points=order.posted_points, reason=reason, rule_version=row.rule_version,
                    evidence_hash=fingerprint(clean), evidence_json=dump(clean), created_at=current)
                s.add(entry); s.flush()
                return entry.id
            if not row.debit_ledger_id:
                row.debit_ledger_id = post(-row.points, 'reward_redeemed')
            row.order_id = order_id; row.state = 'committed'; row.reason = ''
            row.order_updated_at = updated
            row.evidence_json = dump(clean); row.failure_count = 0
            if row.first_observed_at is None:
                row.first_observed_at = naive(evidence['observed_at']) if evidence.get('observed_at') else current
            if evidence['full_refund'] and not row.restore_ledger_id:
                row.restore_ledger_id = post(row.points, 'reward_restored'); row.state = 'restored'
            elif evidence['partial_refund']:
                row.state = 'review'; row.reason = 'partial_refund_review'
            elif released:
                row.state = 'review'; row.reason = 'late_sale_after_reviewed_release'
            if row.state == 'review':
                row.review_cause = row.reason
                if row.resolved_evidence_hash == fingerprint(clean):
                    row.state = 'committed'; row.reason = 'administrator_retained_debit'
            row.checked_at = current
            s.add(order); s.add(row)
            return row
        return self.store.transaction(operation)

    def failed(self, shop, id, exc, generation=None):
        # Never persist arbitrary exception strings or provider response bodies.
        safe = {'manual_reward_evidence_changed', 'program_policy_changed', 'posting_paused', 'order_busy', 'order_changed_retry', 'stale_order_snapshot',
                'same_timestamp_conflict', 'original_basis_changed', 'refund_history_changed',
                'earning_refund_review', 'order_payment_or_refund_review', 'order_conflict',
                'order_identity_mismatch', 'order_discount_mismatch', 'order_not_eligible',
                'identity_review', 'gift_card_discount_review', 'non_merchandise_review',
                'multiple_reward_orders_review', 'reward_discount_conflict',
                'reward_eligibility_changed', 'reward_eligibility_unverified', 'custom_sale_excluded_items',
                'order_line_scope_unverified', 'ineligible_discount_review',
                'reward_pricing_unsupported'}
        reason = str(exc) if isinstance(exc, RedemptionError) and str(exc) in safe else 'provider_retry'
        transient = reason in ('program_policy_changed', 'posting_paused', 'provider_retry', 'order_busy', 'order_changed_retry', 'stale_order_snapshot')
        def operation(s):
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).one()
            if row.state == 'restored': return
            if not transient and row.manual_decision_hash and row.manual_decision_hash == fingerprint(json.loads(row.observed_order_json)):
                # The explicit decision covers this qualification failure, but a
                # concurrent or unreflected receipt must remain pending.
                if generation == row.work_generation and receipt_evidence_current(s, row, json.loads(row.observed_order_json)):
                    row.acknowledged_generation = generation
                    row.failure_count = 0
                    s.add(row)
                return
            row.failure_count += 1; row.checked_at = naive()
            if row.work_generation == row.acknowledged_generation:
                row.work_generation += 1
            if row.state != 'cancelled' and reason not in ('posting_paused', 'program_policy_changed') and (not transient or (row.state != 'committed' and row.failure_count >= 8)):
                row.state = 'review'
            # A failed retry cannot erase a previously established review reason.
            if not row.reason or row.reason == 'provider_retry': row.reason = reason
            s.add(row)
        self.store.transaction(operation)

    def pending(self, shop, limit=10, *, history_seconds=21600, read_all_orders=False):
        if not 1 <= limit <= 100: raise ValueError('invalid_limit')
        if not 30 <= history_seconds <= 21600: raise ValueError('invalid_history_interval')
        current = naive()
        with Session(self.store.engine) as s:
            # Keep capacity for both active checkouts and later refunds. A large
            # historical ledger cannot put new reservations at the queue's tail.
            active = Redemption.state.in_(('reserved','ready','cancelling'))
            due_active = or_(Redemption.checked_at.is_(None), Redemption.checked_at < current-timedelta(seconds=30))
            due_history = or_(Redemption.checked_at.is_(None), Redemption.checked_at < current-timedelta(seconds=history_seconds),
                and_(Redemption.work_generation > Redemption.acknowledged_generation, due_active))
            quota = max(1, limit-limit//5)
            query = select(Redemption).where(Redemption.shop == shop)
            ordering = (Redemption.checked_at.asc().nullsfirst(), Redemption.id)
            rows = list(s.exec(query.where(active, due_active).order_by(*ordering).limit(quota)).all())
            if len(rows) < limit:
                rows += list(s.exec(query.where(~active, Redemption.state != 'restored', due_history,
                    or_(read_all_orders, and_(Redemption.debit_ledger_id.is_(None), Redemption.state != 'cancelled'),
                        Redemption.created_at >= current-timedelta(days=60), Redemption.checked_at.is_(None),
                        Redemption.work_generation > Redemption.acknowledged_generation))
                    .order_by(*ordering).limit(limit-len(rows))).all())
            return rows

    def succeeded(self, shop, id, generation=None, evidence=None):
        def operation(s):
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).one()
            row.failure_count = 0
            # A new receipt during the read cannot be consumed by this attempt.
            # Also wait for the provider to reflect every pending refund/update.
            caught_up = receipt_evidence_current(s, row, evidence)
            if generation is not None and row.work_generation == generation and caught_up:
                row.acknowledged_generation = generation
            if row.state != 'review' and row.reason in ('provider_retry', 'posting_paused'):
                row.reason = ''
            s.add(row)
        self.store.transaction(operation)

    def checked(self, shop, id):
        def operation(s):
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).one()
            row.checked_at = naive()
            s.add(row)
        self.store.transaction(operation)

    def release_cancelled(self, shop, id):
        """Cashier cancelled at the register before any sale.

        Only called after the provider confirmed the code is deactivated, has
        never been used, and no order carries it. A sale that still surfaces
        later is debited by settle() and sent to review, so the worst case is
        one reward's value pending review, never a silent loss.
        """
        def operation(s):
            row0 = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id)).first()
            if row0 is None: raise RedemptionError('redemption_not_found')
            s.exec(select(Account).where(Account.id == row0.account_id).with_for_update()).one()
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).one()
            if row.state == 'cancelled': return row
            if row.state != 'cancelling' or row.debit_ledger_id or row.order_id:
                raise RedemptionError('reservation_not_releasable')
            row.state = 'cancelled'; row.reason = 'cashier_cancelled_unused'; row.checked_at = naive()
            s.add(row)
            return row
        return self.store.transaction(operation)

    def release_reviewed(self, shop, id, reference, authorize):
        """Explicit administrator attestation; never called by POS or a timer."""
        from .ops import key
        from ..models import AuditLog
        key(reference)
        def operation(s):
            actor=authorize(s,'admin.loyalty.correct')
            row0=s.exec(select(Redemption).where(Redemption.shop==shop,Redemption.id==id)).first()
            if row0 is None: raise RedemptionError('redemption_not_found')
            s.exec(select(Account).where(Account.id==row0.account_id).with_for_update()).one()
            row=s.exec(select(Redemption).where(Redemption.shop==shop,Redemption.id==id).with_for_update()).one()
            if row.state=='cancelled': return row.id
            if (row.state!='review' or row.reason!='unused_code_requires_review' or
                row.expires_at>naive() or row.debit_ledger_id or row.order_id):
                raise RedemptionError('reservation_not_releasable')
            row.state='cancelled';row.reason='administrator_verified_unused';s.add(row)
            s.add(AuditLog(actor_user_id=actor.id,action='loyalty.reward_release',
                resource_key='admin.loyalty.correct',details_json=dump(dict(shop=shop,
                redemption_id=row.id,points=row.points,reference=reference,
                attestation='all_registers_synced_and_no_sale_used_reward'))))
            return row.id
        return self.store.transaction(operation)

    def resolve_reviewed(self, shop, id, reference, authorize, *, expected_hash):
        """Acknowledge reviewed evidence while retaining the existing debit."""
        from .ops import key
        from ..models import AuditLog
        key(reference)
        def operation(s):
            actor = authorize(s, 'admin.loyalty.correct')
            row0 = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id)).first()
            if row0 is None: raise RedemptionError('redemption_not_found')
            s.exec(select(Account).where(Account.id == row0.account_id).with_for_update()).one()
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).one()
            if row.manual_decision_hash: raise RedemptionError('reward_not_resolvable')
            if expected_hash != fingerprint(json.loads(row.evidence_json)):
                raise RedemptionError('stale_order_snapshot')
            if row.state == 'committed' and row.reason == 'administrator_retained_debit': return row.id
            if row.state != 'review' or not row.debit_ledger_id or not row.order_id or row.restore_ledger_id:
                raise RedemptionError('reward_not_resolvable')
            previous_reason = row.reason
            row.review_cause = row.review_cause or previous_reason
            row.resolved_evidence_hash = fingerprint(json.loads(row.evidence_json))
            row.state = 'committed'; row.reason = 'administrator_retained_debit'
            s.add(row)
            s.add(AuditLog(actor_user_id=actor.id, action='loyalty.reward_resolve',
                resource_key='admin.loyalty.correct', details_json=dump(dict(
                    shop=shop, redemption_id=id, reference=reference, previous_reason=previous_reason,
                    debit_ledger_id=row.debit_ledger_id, attestation='sale_and_refunds_reviewed_keep_reward_debit'))))
            return row.id
        return self.store.transaction(operation)

    def adjudicate_reviewed(self, shop, id, expected_hash, decision, reference, authorize):
        """Explicit admin exception. Never treats an ineligible sale as eligible.

        A restore also requires purchase earnings to have been reconciled to zero;
        this action cannot conceal an outstanding earning/refund discrepancy.
        """
        from .ops import key
        from ..models import AuditLog
        from .domain import full_refund
        key(reference)
        if decision not in ('debit', 'restore'): raise RedemptionError('invalid_decision')
        def operation(s):
            actor = authorize(s, 'admin.loyalty.correct')
            self.require_ready(shop, s)
            initial = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id)).first()
            if initial is None or not initial.order_id: raise RedemptionError('reward_not_resolvable')
            insert_missing(s, Entitlement, dict(shop=shop, order_id=initial.order_id), ['shop','order_id'])
            order = s.exec(select(Entitlement).where(Entitlement.shop == shop, Entitlement.order_id == initial.order_id).with_for_update()).one()
            account = s.exec(select(Account).where(Account.id == initial.account_id, Account.shop == shop).with_for_update()).one()
            row = s.exec(select(Redemption).where(Redemption.id == id).with_for_update()).one()
            canonical = json.loads(row.observed_order_json)
            if not canonical or fingerprint(canonical) != expected_hash:
                raise RedemptionError('stale_order_snapshot')
            if (canonical['id'] != row.order_id or canonical['customer_id'] != row.customer_id
                or canonical['test'] or canonical['currency'] != 'USD' or account.identity_hold
                or order.identity_hold or (order.account_id and order.account_id != account.id)):
                raise RedemptionError('identity_review')
            if self.policy_getter is not None:
                policy = self.policy_getter()
                if canonical['shop_id'] != policy.shop_id or (order.policy_hash and order.policy_hash != policy.digest()):
                    raise RedemptionError('program_policy_changed')
            if order.lease_token and order.lease_until and order.lease_until > naive():
                raise RedemptionError('order_busy')
            if row.manual_decision_hash == expected_hash and ((decision == 'debit' and row.debit_ledger_id and not row.restore_ledger_id)
                                                            or (decision == 'restore' and row.restore_ledger_id)):
                return row.id
            if row.state != 'review' or row.restore_ledger_id: raise RedemptionError('reward_not_resolvable')
            if sum(sum(line['discounts_cents']) for line in canonical['lines']) != row.discount_cents:
                raise RedemptionError('order_discount_mismatch')
            if canonical['financial_status'] not in ('PAID','PARTIALLY_REFUNDED','REFUNDED') or canonical['outstanding_cents'] != 0:
                raise RedemptionError('order_payment_or_refund_review')
            payments = [p for p in canonical['payments'] if p['kind'] in ('SALE','CAPTURE') and p['status'] == 'SUCCESS']
            if (not payments or len({p['id'] for p in payments}) != len(payments)
                or any(p['cents'] < 0 for p in payments)
                or sum(p['cents'] for p in payments) != canonical['received_cents']
                or canonical['received_cents'] not in (canonical['total_cents'], canonical['total_cents']+canonical['payment_rounding_cents'])):
                raise RedemptionError('order_payment_or_refund_review')
            if decision == 'restore':
                if not full_refund(canonical) or order.posted_points != 0:
                    raise RedemptionError('earning_refund_review')
            elif canonical['financial_status'] == 'REFUNDED':
                raise RedemptionError('order_payment_or_refund_review')
            order.account_id = account.id; order.customer_id = row.customer_id
            proof = dict(administrator_exception=True, order_id=row.order_id,
                observed_evidence_hash=expected_hash, canonical_order=canonical, decision=decision, reference=reference,
                points=row.points, rule_version=row.rule_version)
            def post(delta, reason):
                order.revision += 1
                entry = Ledger(shop=shop, account_id=account.id, entitlement_id=order.id,
                    revision=order.revision, business_key=f'redemption:{row.id}:{reason}', delta=delta,
                    resulting_points=order.posted_points, reason=reason, rule_version=row.rule_version,
                    evidence_hash=fingerprint(proof), evidence_json=dump(proof), created_at=naive())
                s.add(entry); s.flush(); return entry.id
            if not row.debit_ledger_id: row.debit_ledger_id = post(-row.points, 'reward_redeemed')
            if decision == 'restore': row.restore_ledger_id = post(row.points, 'reward_restored')
            # Keep exception earning under explicit review, including delayed
            # pre-refund snapshots arriving after an administrator restores points.
            if decision == 'restore':
                if not order.manual_hold: order.reason = 'reward_exception_manual_hold'
                order.manual_hold = True; order.status = 'review'
            row.state = 'restored' if row.restore_ledger_id else 'committed'
            row.reason = 'administrator_exception_'+decision
            row.manual_decision_hash = expected_hash
            row.evidence_json = dump(proof)
            row.order_updated_at = naive(canonical['updated_at'])
            s.add(order); s.add(row)
            s.add(AuditLog(actor_user_id=actor.id, action='loyalty.reward_adjudicate',
                resource_key='admin.loyalty.correct', details_json=dump(dict(
                    shop=shop, redemption_id=id, decision=decision, reference=reference,
                    evidence_hash=expected_hash, debit_ledger_id=row.debit_ledger_id,
                    restore_ledger_id=row.restore_ledger_id))))
            return row.id
        return self.store.transaction(operation)

    def retry_reviewed(self, shop, id, reference, authorize):
        from .ops import key
        from ..models import AuditLog
        key(reference)
        def operation(s):
            actor = authorize(s, 'admin.loyalty.reconcile')
            row = s.exec(select(Redemption).where(Redemption.shop == shop, Redemption.id == id).with_for_update()).first()
            if row is None: raise RedemptionError('redemption_not_found')
            if row.state != 'review': raise RedemptionError('reward_not_in_review')
            row.checked_at = None
            row.work_generation += 1
            if row.debit_ledger_id is None and row.order_id is None:
                # An administrator requested recovery of an unresolved hold.
                # Check for a sale first, then deactivate; never release here.
                row.state = 'cancelling'
            s.add(row)
            s.add(AuditLog(actor_user_id=actor.id, action='loyalty.reward_retry',
                resource_key='admin.loyalty.reconcile', details_json=dump(dict(
                    shop=shop, redemption_id=id, reference=reference))))
            return row.id
        return self.store.transaction(operation)


async def prepare(redemptions, provider, row):
    """Creation is recoverable by unique code after a crash/uncertain response."""
    if naive() >= row.expires_at: raise RedemptionError('reward_expired')
    if row.state not in ('reserved','ready'): raise RedemptionError('redemption_not_ready')
    async def issuance_check():
        await asyncio.to_thread(redemptions.require_ready, row.shop)
    provider.issuance_check = issuance_check
    await issuance_check()
    provider_id = await provider.ensure_discount(row)
    await issuance_check()
    return await asyncio.to_thread(redemptions.transition, row.shop, row.id, 'ready', provider_id=provider_id)


async def reconcile(redemptions, provider, row, *, allow_prepare=True, cashier_cancel=False):
    try:
        result = await _reconcile(redemptions, provider, row, allow_prepare=allow_prepare, cashier_cancel=cashier_cancel)
        proof = json.loads(result.observed_order_json if result.manual_decision_hash else result.evidence_json) if result and result.evidence_json != '{}' else None
        if proof is not None and result.order_updated_at:
            proof['updated_at'] = result.order_updated_at.isoformat()+'Z'
        await asyncio.to_thread(redemptions.succeeded, row.shop, row.id, row.work_generation, proof)
        return result
    except Exception as exc:
        await asyncio.to_thread(redemptions.failed, row.shop, row.id, exc, row.work_generation)
        raise


async def _reconcile(redemptions, provider, row, *, allow_prepare=True, cashier_cancel=False):
    async def observe(evidence):
        return await asyncio.to_thread(redemptions.observe, row.shop, row.id, evidence)
    provider.observation = observe
    evidence = await provider.order_evidence(row)
    if evidence is not None:
        return await asyncio.to_thread(redemptions.settle, row.shop, row.id, evidence)
    if row.order_id and row.state != 'restored':
        raise RedemptionError('order_changed_retry')
    if row.state in ('cancelled','committed','restored'): return row
    if row.state == 'review' and row.reason != 'unused_code_requires_review': return row
    if row.state == 'cancelling' or naive() >= row.expires_at or (not allow_prepare and row.state in ('reserved','ready')):
        # Never release based on usageCount/search absence: Shopify is asynchronous.
        await provider.deactivate(row)
        # A cashier's own cancel may release the hold, but only on positive
        # confirmation: the code is deactivated with zero uses, and a second
        # order search after deactivation still finds nothing. Expiry, admin
        # retries and the background worker never take this path.
        confirm = getattr(provider, 'confirmed_unused', None)
        if cashier_cancel and row.state == 'cancelling' and confirm is not None and await confirm(row):
            evidence = await provider.order_evidence(row)
            if evidence is not None:
                return await asyncio.to_thread(redemptions.settle, row.shop, row.id, evidence)
            return await asyncio.to_thread(redemptions.release_cancelled, row.shop, row.id)
        return await asyncio.to_thread(redemptions.transition, row.shop, row.id, 'review', reason='unused_code_requires_review')
    if row.state == 'reserved': return await prepare(redemptions, provider, row)
    return row
