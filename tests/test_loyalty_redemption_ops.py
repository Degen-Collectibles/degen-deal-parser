import json
from app.loyalty.domain import fingerprint
import asyncio
from sqlmodel import Session,select
from app.models import AuditLog
from app.loyalty.redemption import Redemptions,reconcile
from test_loyalty_ops import ops,login
from test_loyalty_pos import add_entry
from test_loyalty_redemption import reserve,evidence,SHOP,CUSTOMER

def test_reviewed_release_requires_auth_csrf_attestation_and_audit(ops):
    c,store,uid,_=ops
    add_entry(store.engine,1000,customer='3',shop=SHOP)
    rewards=Redemptions(store);row=reserve(rewards)
    class Provider:
        async def order_evidence(self,row):return None
        async def deactivate(self,row):pass
    asyncio.run(reconcile(rewards,Provider(),row))
    path='/loyalty/rewards/release'
    assert c.post(path,data={}).status_code in (401,403)
    csrf=login(c)
    page=c.get('/loyalty');assert 'Release unused reserved points' in page.text
    payload=dict(csrf_token=csrf,redemption_id=row.id,reference='review-123',registers_verified='no')
    assert c.post(path,data=payload).status_code==400
    payload['registers_verified']='yes'
    assert c.post(path,data=payload,follow_redirects=False).status_code==303
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==0
    assert c.post(path,data=payload,follow_redirects=False).status_code==303
    with Session(store.engine) as s:
        assert len(s.exec(select(AuditLog).where(AuditLog.action=='loyalty.reward_release')).all())==1
    # A late order still consumes its reward; release is not a blind spot.
    result=rewards.settle(SHOP,row.id,evidence(row))
    assert result.reason=='late_sale_after_reviewed_release'
    assert not rewards.overview(SHOP,CUSTOMER)['needs_review']
    assert result.state=='review'  # visible to Ops; the debit already consumed the points


def test_review_retry_requires_auth_csrf_and_retains_hold(ops):
    from app.loyalty.redemption import RedemptionError
    c,store,uid,_=ops
    add_entry(store.engine,1000,customer='3',shop=SHOP)
    rewards=Redemptions(store);row=reserve(rewards)
    rewards.failed(SHOP,row.id,RedemptionError('order_payment_or_refund_review'))
    path='/loyalty/rewards/retry'
    assert c.post(path,data={}).status_code in (401,403)
    csrf=login(c)
    payload=dict(redemption_id=row.id,reference='refund-review-123')
    assert c.post(path,data=payload).status_code==403
    payload['csrf_token']=csrf
    assert 'Recheck Shopify evidence' in c.get('/loyalty').text
    assert c.post(path,data=payload,follow_redirects=False).status_code==303
    row=rewards.get(SHOP,row.id)
    assert row.checked_at is None and row.state=='cancelling'
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500
    with Session(store.engine) as s:
        audit=s.exec(select(AuditLog).where(AuditLog.action=='loyalty.reward_retry')).one()
        assert 'refund-review-123' in audit.details_json


def test_resolve_debited_review_requires_attestation_and_is_audited(ops):
    from app.loyalty.redemption import RedemptionError
    c,store,uid,_=ops
    add_entry(store.engine,1000,customer='3',shop=SHOP)
    rewards=Redemptions(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row,partial_refund=True))
    balance_before=store.balance(SHOP,CUSTOMER)
    path='/loyalty/rewards/resolve'
    assert c.post(path,data={}).status_code in (401,403)
    csrf=login(c)
    payload=dict(redemption_id=row.id,reference='resolve-review-123',debit_verified='yes',evidence_hash=fingerprint(json.loads(rewards.get(SHOP,row.id).evidence_json)))
    assert c.post(path,data=payload).status_code==403
    payload['csrf_token']=csrf;payload['debit_verified']='no'
    assert c.post(path,data=payload).status_code==400
    payload['debit_verified']='yes'
    for _ in range(2): assert c.post(path,data=payload,follow_redirects=False).status_code==303
    assert rewards.get(SHOP,row.id).state=='committed'
    assert store.balance(SHOP,CUSTOMER)==balance_before
    with Session(store.engine) as s:
        assert len(s.exec(select(AuditLog).where(AuditLog.action=='loyalty.reward_resolve')).all())==1
    payload['redemption_id']=999999
    assert c.post(path,data=payload).status_code==409


def test_resolve_cannot_release_unspent_hold(ops):
    c,store,uid,_=ops
    add_entry(store.engine,1000,customer='3',shop=SHOP)
    rewards=Redemptions(store);row=reserve(rewards)
    rewards.transition(SHOP,row.id,'review',reason='provider_retry')
    csrf=login(c)
    payload=dict(csrf_token=csrf,redemption_id=row.id,reference='unsafe-release',debit_verified='yes',evidence_hash=fingerprint(json.loads(rewards.get(SHOP,row.id).evidence_json)))
    assert c.post('/loyalty/rewards/resolve',data=payload).status_code==409
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500
