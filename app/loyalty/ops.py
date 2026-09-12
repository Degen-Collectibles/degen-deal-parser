"""Audited, bounded local reconciliation and compensating corrections."""
import json
import re
from uuid import uuid4
from sqlmodel import select
from .domain import Review, fingerprint, whole
from .models import LoyaltyAccount,LoyaltyEntitlement,LoyaltyInbox,LoyaltyLedger,LoyaltyReconciliation
from .service import dump,insert_missing,naive
from ..models import AuditLog


def key(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',value):
        raise ValueError('invalid_request_reference')
    return value


def replay(store,shop,ids,request_key,authorize):
    request_key=key(request_key)
    if not 1<=len(ids)<=25 or len(set(ids))!=len(ids) or any(type(id) is not int or id<=0 for id in ids):
        raise ValueError('invalid_replay_bounds')
    current=naive()
    def operation(s):
        actor=authorize(s,'admin.loyalty.reconcile')
        # Serialize request keys even for disjoint order sets. Same lock order as posting.
        insert_missing(s,LoyaltyReconciliation,dict(shop=shop),['shop'])
        s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.shop==shop).with_for_update()).one()
        existing=s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop==shop,LoyaltyInbox.delivery_id.startswith(f'replay:{request_key}:',autoescape=True))).all()
        if existing:
            actual=sorted(json.loads(r.evidence_json).get('entitlement_id') for r in existing)
            if actual!=sorted(ids):raise ValueError('request_key_conflict')
            return len(existing)
        rows=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.shop==shop,LoyaltyEntitlement.id.in_(ids)).order_by(LoyaltyEntitlement.id).with_for_update()).all()
        if len(rows)!=len(ids):raise ValueError('unknown_entitlement')
        # Lock before checking again: concurrent replay requests with the same key
        # wait on the same rows and observe the winner's committed inbox records.
        existing=s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop==shop,LoyaltyInbox.delivery_id.startswith(f'replay:{request_key}:',autoescape=True))).all()
        if existing:return len(existing)
        for row in rows:
            evidence={'order_id':row.order_id,'entitlement_id':row.id}
            insert_missing(s,LoyaltyInbox,dict(shop=shop,delivery_id=f'replay:{request_key}:{row.id}',topic='admin/replay',order_id=row.order_id,
                payload_hash=fingerprint(evidence),evidence_json=dump(evidence),received_at=current,available_at=current),['shop','delivery_id'])
        s.add(AuditLog(actor_user_id=actor.id,action='loyalty.replay',resource_key='admin.loyalty.reconcile',details_json=dump({'shop':shop,'entitlement_ids':sorted(ids),'request_key':request_key})))
        return len(rows)
    return store.transaction(operation)


def correct(store,shop,entitlement_id,expected_revision,target,request_key,reason,reference,authorize):
    key(request_key);key(reference);whole(target);whole(expected_revision)
    if reason not in ('erroneous_award','refund_review','reconciliation'):
        raise ValueError('invalid_correction_reason')
    current=naive();business_key='correction:'+request_key
    def operation(s):
        actor=authorize(s,'admin.loyalty.correct')
        row=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.id==entitlement_id,LoyaltyEntitlement.shop==shop).with_for_update()).first()
        if row is None:raise ValueError('unknown_entitlement')
        previous=s.exec(select(LoyaltyLedger).where(LoyaltyLedger.shop==shop,LoyaltyLedger.business_key==business_key)).first()
        if previous:
            if previous.entitlement_id!=row.id or previous.resulting_points!=target or previous.reason!=reason:
                raise ValueError('request_key_conflict')
            return previous.id
        if row.revision!=expected_revision:raise Review('revision_conflict')
        if row.lease_until and row.lease_until>current:raise Review('order_busy')
        # Corrections require retained factual basis and an existing account. No
        # new/historical awards, identity transfers, or unlimited credit endpoint.
        if not row.account_id or not row.basis_hash or not row.evidence_hash or target>max(row.candidate_points,row.posted_points) or abs(target-row.posted_points)>10000 or target==row.posted_points:
            raise ValueError('correction_out_of_bounds')
        account=s.exec(select(LoyaltyAccount).where(LoyaltyAccount.id==row.account_id).with_for_update()).one()
        if row.identity_hold or account.identity_hold:raise Review('identity_merge_review')
        before=row.posted_points
        store.post(s,row,target,reason,current,business_key=business_key,actor=actor.id)
        row.manual_hold=True;row.status='review';row.reason='manual_correction_hold';row.lease_token=None;row.lease_until=None;s.add(row)
        s.add(AuditLog(actor_user_id=actor.id,action='loyalty.correction',resource_key='admin.loyalty.correct',details_json=dump({
            'shop':shop,'entitlement_id':row.id,'before_points':before,'target_points':target,'revision':row.revision,
            'reason':reason,'reference':reference,'request_key':request_key})))
        s.flush()
        return row.revision
    return store.transaction(operation)


def verify_attachment(store, policy, entitlement_id, expected_revision, evidence_hash,
                      attached_at, request_key, reference, authorize):
    """Verify retained evidence, then queue a fresh canonical calculation.

    This is an attestation to an external case's actual attachment time, never a
    customer picker or a points override. No ledger write occurs in this action.
    """
    from .domain import calculate, timestamp
    from .models import LoyaltyReconciliation
    key(request_key);key(reference);whole(expected_revision)
    at=timestamp(attached_at);current=naive()
    if naive(at)>current:raise Review('attachment_in_future')
    def operation(s):
        actor=authorize(s,'admin.loyalty.correct')
        program=s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.shop==policy.shop_domain).with_for_update()).first()
        if not program or program.policy_hash!=policy.digest():raise Review('program_policy_changed')
        row=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.id==entitlement_id,LoyaltyEntitlement.shop==policy.shop_domain).with_for_update()).first()
        if row is None:raise ValueError('unknown_entitlement')
        proof={'kind':'verified_attachment','customer_id':row.customer_id,'at':at.isoformat(),
               'request_key':request_key,'reference':reference,'observed_hash':evidence_hash,'actor_user_id':actor.id}
        previous=json.loads(row.attachment_json)
        if previous.get('request_key')==request_key:
            if previous!=proof:raise ValueError('request_key_conflict')
            return row.id
        if row.revision!=expected_revision:raise Review('revision_conflict')
        if row.lease_until and row.lease_until>current:raise Review('order_busy')
        if row.identity_hold or row.manual_hold:raise Review('manual_identity_hold')
        if row.reason not in ('attachment_outside_window','attachment_timing_unverified'):
            raise Review('attachment_verification_not_applicable')
        evidence=json.loads(row.observed_evidence_json)
        if fingerprint(evidence)!=evidence_hash:raise Review('canonical_evidence_changed')
        if not row.customer_id or evidence.get('customer_id')!=row.customer_id:raise Review('identity_reassignment')
        account=s.exec(select(LoyaltyAccount).where(LoyaltyAccount.shop==row.shop,LoyaltyAccount.customer_id==row.customer_id).with_for_update()).first()
        if account and account.identity_hold:raise Review('identity_merge_review')
        result=calculate(evidence,policy,proof)
        if result.state!='eligible':raise Review('attachment_order_ineligible')
        if row.basis_hash and row.basis_hash!=result.basis_hash:raise Review('original_basis_changed')
        row.attachment_json=dump(proof);row.revision+=1
        row.status='pending';row.reason='verified_attachment_awaiting_canonical';row.next_check_at=current
        row.lease_token=None;row.lease_until=None;s.add(row)
        job={'order_id':row.order_id,'entitlement_id':row.id}
        insert_missing(s,LoyaltyInbox,dict(shop=row.shop,delivery_id=f'attachment:{row.id}:{row.revision}',topic='admin/attachment',
            order_id=row.order_id,payload_hash=fingerprint(job),evidence_json=dump(job),received_at=current,available_at=current),['shop','delivery_id'])
        s.add(AuditLog(actor_user_id=actor.id,action='loyalty.verify_attachment',resource_key='admin.loyalty.correct',
            details_json=dump({'entitlement_id':row.id,'proof':proof,'previous_proof':previous,'revision':row.revision})))
        return row.id
    return store.transaction(operation)
