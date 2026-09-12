"""Short, retryable transactions; per-order fenced leases include first insert.

SQLite serializes writers with BEGIN IMMEDIATE. PostgreSQL uses row locks and
ON CONFLICT for absent rows. Network calls NEVER run inside these transactions.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import random
import time
from uuid import uuid4
from sqlalchemy import func, or_, text, update
from sqlalchemy.exc import OperationalError, DBAPIError
from sqlmodel import Session, select
from .domain import Review, RULE_VERSION, calculate, fingerprint, gid, timestamp, whole
from .models import LoyaltyAccount, LoyaltyEntitlement, LoyaltyInbox, LoyaltyLedger, LoyaltyReconciliation
from .schema import schema_ready

log = logging.getLogger(__name__)
LEASE_SECONDS = 300


def naive(value=None):
    return (timestamp(value) if value is not None else datetime.now(timezone.utc)).replace(tzinfo=None)


def dump(value):
    return json.dumps(value,sort_keys=True,separators=(',', ':'),allow_nan=False)


def canonical_evidence(value):
    """Allowlist the versioned domain schema before retaining any evidence."""
    keys = ('id','shop_id','source','location_id','created_at','processed_at','updated_at','customer_id',
            'test','edited','returns','exchanges','currency','taxes_included','total_cents','received_cents',
            'payment_rounding_cents','refund_rounding_cents','financial_status','outstanding_cents')
    result = {k:value[k] for k in keys}
    def project(rows, fields):
        return [{k:r[k] for k in fields} for r in rows]
    result['lines'] = project(value['lines'], ('id','quantity','original_cents','discounts_cents','tax_cents','gift_card','merchandise'))
    result['payments'] = project(value['payments'], ('id','kind','status','cents','rounding_cents','processed_at'))
    result['refunds'] = []
    for r in value['refunds']:
        item = {k:r[k] for k in ('id','shipping_cents','shipping_tax_cents','adjustments','duties')}
        item['lines'] = project(r['lines'], ('id','line_id','quantity','subtotal_cents','tax_cents'))
        item['transactions'] = project(r['transactions'], ('id','kind','status','cents','rounding_cents'))
        result['refunds'].append(item)
    # Arrays with resource IDs are semantic sets: provider ordering isn't a revision.
    for key in ('lines','payments','refunds'):
        result[key].sort(key=lambda v:v['id'])
    for line in result['lines']:
        line['discounts_cents'] = sorted(line['discounts_cents'])
    for r in result['refunds']:
        r['lines'].sort(key=lambda v:v['id']); r['transactions'].sort(key=lambda v:v['id'])
    return result


def insert_missing(session, model, values, columns):
    dialect = session.get_bind().dialect.name
    if dialect == 'sqlite':
        from sqlalchemy.dialects.sqlite import insert
    elif dialect == 'postgresql':
        from sqlalchemy.dialects.postgresql import insert
    else:
        raise RuntimeError('loyalty_unsupported_database')
    initialized = model(**values).model_dump(exclude={'id'})
    session.execute(insert(model).values(**initialized).on_conflict_do_nothing(index_elements=columns))


class LostLease(RuntimeError):
    pass


@dataclass(frozen=True)
class Lease:
    inbox_id: int
    entitlement_id: int
    shop: str
    order_id: str
    token: str
    expected_revision: int
    evidence: dict


class Store:
    def __init__(self, engine):
        self.engine = engine

    def transaction(self, operation):
        if not schema_ready(self.engine):
            raise RuntimeError('loyalty_schema_not_ready')
        for attempt in range(5):
            try:
                with Session(self.engine, expire_on_commit=False) as session:
                    if self.engine.dialect.name == 'sqlite':
                        session.execute(text('BEGIN IMMEDIATE'))
                    result = operation(session)
                    session.commit()
                    return result
            except DBAPIError as exc:
                code = getattr(exc.orig, 'sqlstate', None)
                retry = code in ('40001','40P01','55P03') or (self.engine.dialect.name == 'sqlite' and isinstance(exc,OperationalError) and any(s in str(exc.orig).lower() for s in ('locked','busy')))
                if not retry or attempt == 4:
                    raise
                time.sleep(min(.05 * 2**attempt, .8) + random.random()*.02)

    def receive(self, shop, delivery_id, topic, evidence, payload_hash, received_at=None):
        received = naive(received_at)
        # Caller has verified raw HMAC. A receipt contains identifiers and event
        # evidence only, never a raw webhook or a replicated customer profile.
        clean = {k:evidence[k] for k in ('order_id','customer_id','updated_at','merge_ids','merge_status','reason','event_id','refund_id') if k in evidence}
        def operation(s):
            previous = s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop==shop,LoyaltyInbox.delivery_id==delivery_id)).first()
            if previous:
                if previous.topic != topic or previous.payload_hash != payload_hash:
                    raise ValueError('delivery_conflict')
                return previous.id
            status = 'review' if clean.get('merge_ids') or clean.get('reason') else 'queued'
            # UPSERT handles concurrent duplicate receipts as well as first orders.
            insert_missing(s,LoyaltyInbox,dict(shop=shop,delivery_id=delivery_id,topic=topic,
                order_id=clean.get('order_id'),payload_hash=payload_hash,evidence_json=dump(clean),
                received_at=received,available_at=received,status=status,reason=clean.get('reason','identity_merge_review' if clean.get('merge_ids') else '')),
                ['shop','delivery_id'])
            row = s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop==shop,LoyaltyInbox.delivery_id==delivery_id)).one()
            if row.topic != topic or row.payload_hash != payload_hash:
                raise ValueError('delivery_conflict')
            if row.order_id:
                insert_missing(s,LoyaltyEntitlement,dict(shop=shop,order_id=row.order_id,created_at=received,next_check_at=received),['shop','order_id'])
            for customer in (clean.get('merge_ids',[]) if clean.get('merge_status') in ('completed','succeeded') else []):
                customer = gid('Customer',customer)
                insert_missing(s,LoyaltyAccount,dict(shop=shop,customer_id=customer),['shop','customer_id'])
                s.execute(update(LoyaltyAccount).where(LoyaltyAccount.shop==shop,LoyaltyAccount.customer_id==customer).values(identity_hold=True))
            return row.id
        result = self.transaction(operation)
        log.info('loyalty receipt committed inbox_id=%s topic=%s',result,topic)
        return result

    def claim(self, shop, *, now=None):
        current = naive(now); expires = current + timedelta(seconds=LEASE_SECONDS)
        def operation(s):
            candidates = s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop==shop,
                LoyaltyInbox.order_id.is_not(None),LoyaltyInbox.available_at<=current,
                or_(LoyaltyInbox.status=='queued', (LoyaltyInbox.status=='processing') & (LoyaltyInbox.lease_until<=current)))
                .order_by(LoyaltyInbox.available_at,LoyaltyInbox.id).limit(50)).all()
            for inbox in candidates:
                token = uuid4().hex
                # Lock order before inbox in every transaction. Conditional UPDATE
                # makes the lease atomic even when multiple workers saw the row.
                row = s.execute(update(LoyaltyEntitlement).where(LoyaltyEntitlement.shop==shop,
                    LoyaltyEntitlement.order_id==inbox.order_id,
                    or_(LoyaltyEntitlement.lease_until.is_(None),LoyaltyEntitlement.lease_until<=current))
                    .values(lease_token=token,lease_until=expires).returning(LoyaltyEntitlement.id,LoyaltyEntitlement.revision)).first()
                if not row:
                    continue
                changed = s.execute(update(LoyaltyInbox).where(LoyaltyInbox.id==inbox.id,
                    or_(LoyaltyInbox.status=='queued',(LoyaltyInbox.status=='processing') & (LoyaltyInbox.lease_until<=current)))
                    .values(status='processing',lease_token=token,lease_until=expires,attempts=LoyaltyInbox.attempts+1))
                if changed.rowcount != 1:
                    s.execute(update(LoyaltyEntitlement).where(LoyaltyEntitlement.id==row.id,LoyaltyEntitlement.lease_token==token).values(lease_token=None,lease_until=None))
                    continue
                return Lease(inbox.id,row.id,shop,inbox.order_id,token,row.revision,json.loads(inbox.evidence_json))
        return self.transaction(operation)

    def locked(self, s, lease, current):
        row = s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.id==lease.entitlement_id).with_for_update()).one()
        inbox = s.get(LoyaltyInbox,lease.inbox_id)
        if (row.lease_token != lease.token or inbox.lease_token != lease.token or inbox.status!='processing'
            or row.lease_until is None or row.lease_until <= current or inbox.lease_until is None or inbox.lease_until<=current
            or row.revision!=lease.expected_revision):
            raise LostLease('loyalty_lease_lost')
        return row,inbox

    def finish(self, lease, evidence, policy, *, posting=False, now=None):
        current = naive(now)
        def operation(s):
            # A program's activation policy is pinned shop-wide, including first
            # inserts. A later cutoff/location change needs a reviewed migration.
            insert_missing(s,LoyaltyReconciliation,dict(shop=lease.shop,policy_hash=policy.digest()),['shop'])
            program=s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.shop==lease.shop).with_for_update()).one()
            if not program.policy_hash:
                program.policy_hash=policy.digest();s.add(program)
            row,inbox = self.locked(s,lease,current)
            delta=0; result=None; clean=None
            try:
                clean = canonical_evidence(evidence)
                if clean['id']!=row.order_id or lease.shop!=policy.shop_domain:
                    raise Review('order_identity_mismatch')
                row.observed_evidence_json=dump(clean)
                if program.policy_hash!=policy.digest():
                    raise Review('program_policy_changed')
                updated = naive(clean['updated_at'])
                incoming = naive(lease.evidence['updated_at']) if lease.evidence.get('updated_at') else None
                if (row.source_updated_at and updated<row.source_updated_at) or (incoming and updated<incoming):
                    raise Review('stale_canonical_read')
                digest = fingerprint(clean)
                if row.source_updated_at==updated and row.evidence_hash and row.evidence_hash!=digest:
                    raise Review('same_timestamp_conflict')
                if (row.identity_hold or (row.customer_id and clean['customer_id']!=row.customer_id)
                    or (lease.evidence.get('customer_id') and lease.evidence['customer_id']!=clean['customer_id'])):
                    row.identity_hold=True
                    raise Review('identity_reassignment')
                if clean['customer_id'] is not None:
                    row.customer_id=gid('Customer',clean['customer_id'])
                # The local canonical observation is an upper bound on when
                # this association existed. Headers and webhook creation dates
                # cannot backdate it. Preserve the earliest matching observation.
                if row.customer_id and not json.loads(row.attachment_json):
                    row.attachment_json=dump({'kind':'canonical_observation',
                        'customer_id':row.customer_id,'at':current.replace(tzinfo=timezone.utc).isoformat()})
                previous=json.loads(row.evidence_json)
                incoming_refunds={r['id']:r for r in clean['refunds']}
                if lease.evidence.get('refund_id') and lease.evidence['refund_id'] not in incoming_refunds:
                    raise Review('refund_not_visible')
                if any(incoming_refunds.get(r['id'])!=r for r in previous.get('refunds',[])):
                    raise Review('refund_history_changed')
                if row.policy_hash and row.policy_hash!=policy.digest():
                    raise Review('program_policy_changed')
                if row.manual_hold:
                    raise Review('manual_correction_hold')
                attached = json.loads(row.attachment_json)
                result = calculate(clean,policy,attached)
                if row.basis_hash and result.basis_hash and row.basis_hash!=result.basis_hash:
                    raise Review('original_basis_changed')
                if row.posted_points and result.state!='eligible':
                    raise Review('awarded_order_now_ineligible')
                if result.state=='eligible':
                    customer = gid('Customer',clean['customer_id'])
                    insert_missing(s,LoyaltyAccount,dict(shop=row.shop,customer_id=customer,created_at=current),['shop','customer_id'])
                    account=s.exec(select(LoyaltyAccount).where(LoyaltyAccount.shop==row.shop,LoyaltyAccount.customer_id==customer).with_for_update()).one()
                    if account.identity_hold:
                        raise Review('identity_merge_review')
                    row.account_id=account.id; row.customer_id=customer
                    row.attachment_json=dump(attached)
                    row.basis_hash=result.basis_hash
                row.status=result.state if posting or result.state!='eligible' else 'shadow'
                row.reason=result.reason; row.candidate_points=result.points; row.net_cents=result.net_cents
                row.rule_version=RULE_VERSION; row.policy_hash=policy.digest()
                row.evidence_hash=digest; row.evidence_json=dump(clean); row.source_updated_at=updated
                if posting and result.state=='eligible':
                    delta=result.points-row.posted_points
                    if delta:
                        self.post(s,row,result.points,'earned' if delta>0 else 'refund',current)
            except Review as exc:
                row.status='review'; row.reason=str(exc)
            except (ValueError,TypeError,KeyError):
                row.status='review'; row.reason='incomplete_evidence'
            row.checked_at=current; row.next_check_at=current+timedelta(hours=6)
            row.lease_token=None; row.lease_until=None
            inbox.status='review' if row.status=='review' else 'done'
            inbox.reason=row.reason; inbox.completed_at=current; inbox.lease_until=None; inbox.lease_token=None
            s.add(row); s.add(inbox)
            return delta
        delta=self.transaction(operation)
        log.info('loyalty evaluated order_id=%s inbox_id=%s delta=%s',lease.order_id,lease.inbox_id,delta)
        return delta

    @staticmethod
    def post(s,row,target,reason,current,*,business_key=None,actor=None):
        delta=target-row.posted_points
        if not delta:
            return
        row.revision+=1
        s.add(LoyaltyLedger(shop=row.shop,account_id=row.account_id,entitlement_id=row.id,
            revision=row.revision,business_key=business_key or f'order:{row.order_id}:revision:{row.revision}',
            delta=delta,resulting_points=target,reason=reason,rule_version=RULE_VERSION,
            evidence_hash=row.evidence_hash,evidence_json=row.evidence_json,actor_user_id=actor,created_at=current))
        row.posted_points=target

    def fail(self, lease, reason='canonical_fetch_failed', *, now=None):
        current=naive(now)
        def operation(s):
            row,inbox=self.locked(s,lease,current)
            inbox.status='review' if inbox.attempts>=8 else 'queued'
            inbox.reason=reason; inbox.available_at=current+timedelta(seconds=min(3600,15*2**min(inbox.attempts,8)))
            inbox.lease_token=None; inbox.lease_until=None
            row.status='review'; row.reason=reason; row.lease_token=None; row.lease_until=None
            row.next_check_at=current+timedelta(hours=6)
            s.add(row); s.add(inbox)
        self.transaction(operation)
        log.warning('loyalty fetch retry order_id=%s reason=%s',lease.order_id,reason)

    def minimize_old_receipts(self, shop, days, *, now=None, limit=100):
        whole(days,minimum=1)
        if not 1<=limit<=100:raise ValueError('invalid_retention_limit')
        cutoff=naive(now)-timedelta(days=days)
        def operation(s):
            rows=s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop==shop,LoyaltyInbox.status=='done',~LoyaltyInbox.topic.startswith('admin/'),
                LoyaltyInbox.completed_at<cutoff,LoyaltyInbox.evidence_json!='{}')
                .order_by(LoyaltyInbox.id).limit(limit).with_for_update()).all()
            for row in rows:
                # Ledger and order evidence retain accounting reproducibility.
                # Delivery IDs/hashes remain tombstones for receipt deduplication.
                row.evidence_json='{}';s.add(row)
            return len(rows)
        return self.transaction(operation)

    def balance(self, shop, customer_id):
        with Session(self.engine) as s:
            return int(s.exec(select(func.coalesce(func.sum(LoyaltyLedger.delta),0)).join(LoyaltyAccount,
                LoyaltyLedger.account_id==LoyaltyAccount.id).where(LoyaltyAccount.shop==shop,LoyaltyAccount.customer_id==customer_id)).one())

    def rebuild_check(self,shop):
        """No cached account balance exists. Compare per-order state to ledger sums."""
        with Session(self.engine) as s:
            rows=s.exec(select(LoyaltyEntitlement.id,LoyaltyEntitlement.posted_points,
                func.coalesce(func.sum(LoyaltyLedger.delta),0)).outerjoin(LoyaltyLedger,LoyaltyLedger.entitlement_id==LoyaltyEntitlement.id)
                .where(LoyaltyEntitlement.shop==shop).group_by(LoyaltyEntitlement.id,LoyaltyEntitlement.posted_points)).all()
            return [dict(entitlement_id=id,stored=stored,ledger=int(total)) for id,stored,total in rows if stored!=total]
