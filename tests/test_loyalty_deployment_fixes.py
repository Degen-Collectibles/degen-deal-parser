"""Deployment audit regressions: durable recovery and fail-closed issuance."""
import asyncio
import json
from app.loyalty.domain import fingerprint
from datetime import timedelta
from types import SimpleNamespace
import pytest
from sqlalchemy import text
from sqlmodel import Session, select
from app.loyalty.models import LoyaltyRedemption, LoyaltyReconciliation
from app.loyalty.redemption import Redemptions, RedemptionError, prepare, reconcile
from app.loyalty.service import naive
from app.loyalty.reward_policy import Reward
from test_loyalty_service import store
from test_loyalty_redemption import funded, reserve, evidence, SHOP, CUSTOMER, SALE
from test_loyalty_ops import ops, login
from test_loyalty_pos import add_entry


def test_pause_blocks_new_hold_and_provider_preparation(store):
    rewards=funded(store)
    paused=Redemptions(store, posting_allowed=lambda:False)
    with pytest.raises(RedemptionError,match='posting_paused'): reserve(paused)
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==0
    row=rewards.reserve(SHOP,CUSTOMER,'a'*64,'fresh_request_001',Reward())
    class Provider:
        async def ensure_discount(self,row): raise AssertionError('must not create')
    with pytest.raises(RedemptionError,match='posting_paused'):
        asyncio.run(prepare(paused,Provider(),row))


def test_pinned_policy_blocks_issuance_and_status(store):
    from test_loyalty_domain import POLICY
    funded(store)
    with Session(store.engine) as s:
        pin=s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.shop==SHOP)).one()
        pin.policy_hash='f'*64;s.add(pin);s.commit()
    rewards=Redemptions(store,policy_getter=lambda:POLICY)
    assert not rewards.issuance_ready(SHOP)
    with pytest.raises(RedemptionError,match='program_policy_changed'):reserve(rewards)


def test_old_webhook_retry_survives_failure_and_concurrent_wakeup(store):
    rewards=funded(store);row=reserve(rewards)
    proof=evidence(row,updated_at=SALE.isoformat(),refunds=[])
    rewards.settle(SHOP,row.id,proof,now=SALE)
    with Session(store.engine) as s:
        item=s.get(LoyaltyRedemption,row.id);item.created_at=naive()-timedelta(days=61)
        item.checked_at=naive()-timedelta(days=1);s.add(item);s.commit()
    assert not rewards.pending(SHOP)
    def wake(key, **changes):
        store.receive(SHOP,key,'orders/updated',dict(order_id=proof['order_id'],updated_at=SALE.isoformat(),**changes),key)
    wake('wake-one')
    attempt=rewards.pending(SHOP)[0]
    rewards.failed(SHOP,row.id,TimeoutError());rewards.checked(SHOP,row.id)
    with Session(store.engine) as s:
        item=s.get(LoyaltyRedemption,row.id);item.checked_at=naive()-timedelta(seconds=31);s.add(item);s.commit()
    assert rewards.pending(SHOP)[0].id==row.id
    wake('wake-two')
    rewards.succeeded(SHOP,row.id,attempt.work_generation,proof)
    item=rewards.get(SHOP,row.id)
    assert item.work_generation>item.acknowledged_generation
    # An eventually consistent read cannot consume a refund notification.
    wake('wake-refund',refund_id='gid://shopify/Refund/42')
    item=rewards.get(SHOP,row.id)
    rewards.succeeded(SHOP,row.id,item.work_generation,proof)
    assert rewards.get(SHOP,row.id).acknowledged_generation<item.work_generation
    proof['refunds']=[{'id':'gid://shopify/Refund/42'}]
    rewards.succeeded(SHOP,row.id,item.work_generation,proof)
    assert rewards.get(SHOP,row.id).acknowledged_generation==item.work_generation


@pytest.mark.parametrize('late',[False,True])
def test_review_decision_survives_replay_and_new_refund_reopens(ops,late):
    c,store,_,_=ops
    add_entry(store.engine,1000,customer='3',shop=SHOP)
    rewards=Redemptions(store);row=reserve(rewards)
    starting=store.balance(SHOP,CUSTOMER)
    if late:
        with Session(store.engine) as s:
            item=s.get(LoyaltyRedemption,row.id);item.state='cancelled';s.add(item);s.commit()
    proof=evidence(row,partial_refund=not late,updated_at=SALE.isoformat(),refunds=[])
    for _ in range(2):
        assert rewards.settle(SHOP,row.id,proof,now=SALE).state=='review'
    csrf=login(c)
    assert c.post('/loyalty/rewards/resolve',data=dict(csrf_token=csrf,redemption_id=row.id,
        reference='audit-resolve',debit_verified='yes',evidence_hash=fingerprint(json.loads(rewards.get(SHOP,row.id).evidence_json))),follow_redirects=False).status_code==303
    for _ in range(2):
        assert rewards.settle(SHOP,row.id,proof,now=SALE).reason=='administrator_retained_debit'
    changed=dict(proof,partial_refund=True,refunds=[{'id':'new-refund'}],updated_at=(SALE+timedelta(days=1)).isoformat())
    assert rewards.settle(SHOP,row.id,changed,now=SALE).state=='review'
    assert store.balance(SHOP,CUSTOMER)==starting-500
    changed.update(full_refund=True,updated_at=(SALE+timedelta(days=2)).isoformat())
    assert rewards.settle(SHOP,row.id,changed,now=SALE).state=='restored'
    assert store.balance(SHOP,CUSTOMER)==starting


def test_observation_survives_posting_pause_and_day8_retry(store,monkeypatch):
    from app.loyalty import redemption_shopify as module
    from app.loyalty import redemption as ledger
    from test_loyalty_domain import order
    rewards=funded(store);row=reserve(rewards)
    sale=order(9000,id='gid://shopify/Order/99');sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    class Reader:
        def __init__(self,*a,**kw):pass
        async def order(self,*a):return sale
    monkeypatch.setattr(module,'Reader',Reader)
    clock=[SALE]
    original=naive
    monkeypatch.setattr(ledger,'naive',lambda value=None:original(clock[0] if value is None else value))
    monkeypatch.setattr(module,'naive',lambda value=None:original(clock[0] if value is None else value))
    provider=module.DiscountProvider(None,SHOP,'synthetic','1','2')
    async def query(q,variables):
        if 'RewardOrders' in q:return {'orders':{'nodes':[{'id':sale['id']}],'pageInfo':{'hasNextPage':False}}}
        return {'order':{'id':sale['id'],'updatedAt':sale['updated_at'],
            'lineItems':{'nodes':[],'pageInfo':{'hasNextPage':False}},
            'discountApplications':{'nodes':[{'code':row.code}],'pageInfo':{'hasNextPage':False}}}}
    provider.query=query
    paused=Redemptions(store,posting_allowed=lambda:False)
    with pytest.raises(RedemptionError,match='posting_paused'):
        asyncio.run(reconcile(paused,provider,row))
    observed=rewards.get(SHOP,row.id)
    assert observed.first_observed_at==original(SALE) and observed.order_id==sale['id']
    assert observed.debit_ledger_id is None
    clock[0]=SALE+timedelta(days=8)
    assert asyncio.run(reconcile(rewards,provider,observed)).state=='committed'
    assert store.balance(SHOP,CUSTOMER)==500


def test_observed_used_order_cannot_be_relabelled_unused(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.observe(SHOP,row.id,evidence(row))
    class Provider:
        async def order_evidence(self,row):return None
        async def deactivate(self,row):raise AssertionError('known used order must stay in review')
    with pytest.raises(RedemptionError,match='order_changed_retry'):
        asyncio.run(reconcile(rewards,Provider(),rewards.get(SHOP,row.id)))
    assert rewards.get(SHOP,row.id).order_id==evidence(row)['order_id']
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500


def test_additive_upgrade_preserves_reservations(store):
    from app.loyalty.schema import install_schema,schema_ready
    rewards=funded(store);row=reserve(rewards)
    with store.engine.begin() as c:
        for column in ('observed_order_json','manual_decision_hash','work_generation','acknowledged_generation','review_cause','resolved_evidence_hash'):
            c.execute(text('ALTER TABLE loyalty_redemption DROP COLUMN '+column))
    assert not schema_ready(store.engine)
    install_schema(store.engine);install_schema(store.engine)
    assert schema_ready(store.engine)
    saved=rewards.get(SHOP,row.id)
    assert saved.code==row.code and saved.points==500 and saved.work_generation==0


def test_readiness_fails_closed_for_existing_pos_reads(store):
    from test_loyalty_pos import configuration
    from app.loyalty.readiness import ready
    from scripts.check_deploy_health import healthy
    settings=configuration()
    assert ready(store.engine,settings,SimpleNamespace())
    with store.engine.begin() as c:c.execute(text('ALTER TABLE loyalty_redemption DROP COLUMN work_generation'))
    assert not ready(store.engine,settings,SimpleNamespace())
    assert not healthy({'ok':True,'db_ok':True})
    assert not healthy({'ok':True,'db_ok':True,'loyalty_ready':False})
    assert healthy({'ok':True,'db_ok':True,'loyalty_ready':True})


def test_readiness_requires_live_configured_worker(store,monkeypatch):
    monkeypatch.setattr("app.inventory.shopify.resolve_shopify_access_token",lambda _:"synthetic")
    from app.loyalty.readiness import ready
    from test_loyalty_pos import configuration
    settings=configuration(loyalty_processing_enabled=True,loyalty_location_ids='2',
        loyalty_launch_at='2026-01-01T00:00:00Z',loyalty_read_access_verified=True,
        loyalty_evidence_retention_days=30,loyalty_budget_cents=0)
    assert not ready(store.engine,settings,SimpleNamespace())
    assert not ready(store.engine,settings,SimpleNamespace(loyalty_task=SimpleNamespace(done=lambda:True)))
    assert ready(store.engine,settings,SimpleNamespace(loyalty_task=SimpleNamespace(done=lambda:False)))

def test_reviewed_exception_is_audited_fenced_and_refundable(ops):
    from app.loyalty.domain import fingerprint
    from app.loyalty.service import canonical_evidence
    from app.models import AuditLog
    from test_loyalty_domain import order,refund
    c,store,_,_=ops
    add_entry(store.engine,1000,customer='3',shop=SHOP)
    rewards=Redemptions(store);row=reserve(rewards)
    starting=store.balance(SHOP,CUSTOMER)
    sale=order(9000,id='gid://shopify/Order/99',location_id='gid://shopify/Location/999')
    sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    def observe():
        rewards.observe(SHOP,row.id,evidence(row,canonical_order=sale))
        rewards.failed(SHOP,row.id,RedemptionError('order_payment_or_refund_review'))
        return fingerprint(canonical_evidence(sale))
    prior_rebuild=store.rebuild_check(SHOP)
    digest=observe();path='/loyalty/rewards/adjudicate'
    assert c.post(path,data={}).status_code in (401,403)
    csrf=login(c)
    assert 'Apply reviewed reward decision' in c.get('/loyalty').text
    data=dict(csrf_token=csrf,redemption_id=row.id,reference='reviewed-sale',
              evidence_hash=digest,decision='debit',exception_verified='no')
    assert c.post(path,data=data).status_code==400
    data['exception_verified']='yes';data['evidence_hash']='0'*64
    assert c.post(path,data=data).status_code==409
    data['evidence_hash']=digest
    for _ in range(2):assert c.post(path,data=data,follow_redirects=False).status_code==303
    assert store.balance(SHOP,CUSTOMER)==starting-500
    observe()
    assert rewards.get(SHOP,row.id).state=='committed'
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==0
    # Materially new evidence reopens review, and a stale form cannot act on it.
    sale.update(financial_status='REFUNDED',updated_at=(SALE+timedelta(days=1)).isoformat())
    sale['refunds']=[refund(9000)];sale['refunds'][0]['lines'][0]['quantity']=1
    new_digest=observe()
    assert rewards.get(SHOP,row.id).state=='review'
    assert c.post('/loyalty/rewards/resolve',data=dict(csrf_token=csrf,redemption_id=row.id,reference='wrong-path',debit_verified='yes',evidence_hash=fingerprint(json.loads(rewards.get(SHOP,row.id).evidence_json)))).status_code==409
    assert c.post(path,data=data).status_code==409
    data.update(evidence_hash=new_digest,decision='restore')
    for _ in range(2):assert c.post(path,data=data,follow_redirects=False).status_code==303
    assert rewards.get(SHOP,row.id).state=='restored'
    assert store.balance(SHOP,CUSTOMER)==starting
    with Session(store.engine) as s:
        audits=s.exec(select(AuditLog).where(AuditLog.action=='loyalty.reward_adjudicate')).all()
        assert len(audits)==2
    assert store.rebuild_check(SHOP)==prior_rebuild

@pytest.mark.parametrize('restore',[False,True])
def test_concurrent_review_decisions_post_once_on_both_engines(store,restore):
    from concurrent.futures import ThreadPoolExecutor
    from sqlmodel import SQLModel
    from app.models import AuditLog,User
    from app.loyalty.domain import fingerprint
    from app.loyalty.service import canonical_evidence
    from test_loyalty_domain import order,refund,POLICY
    SQLModel.metadata.create_all(store.engine,tables=[User.__table__,AuditLog.__table__])
    rewards=funded(store);row=reserve(rewards)
    sale=order(9000,id='gid://shopify/Order/99',source='web')
    sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    if restore:
        sale.update(financial_status='REFUNDED',updated_at=(SALE+timedelta(days=1)).isoformat())
        sale['refunds']=[refund(9000)];sale['refunds'][0]['lines'][0]['quantity']=1
    rewards.observe(SHOP,row.id,evidence(row,canonical_order=sale))
    rewards.failed(SHOP,row.id,RedemptionError('order_payment_or_refund_review'))
    service=Redemptions(store,policy_getter=lambda:POLICY)
    args=(SHOP,row.id,fingerprint(canonical_evidence(sale)),'restore' if restore else 'debit','reviewed-sale',lambda *a:SimpleNamespace(id=None))
    with ThreadPoolExecutor(2) as pool:
        assert list(pool.map(lambda _:service.adjudicate_reviewed(*args),range(2)))==[row.id,row.id]
    assert store.balance(SHOP,CUSTOMER)==(1000 if restore else 500)
    with Session(store.engine) as s:
        assert len(s.exec(select(AuditLog).where(AuditLog.action=='loyalty.reward_adjudicate')).all())==1
    assert store.rebuild_check(SHOP)==[]


def test_pending_refund_evidence_survives_receipt_retention(store):
    from app.loyalty.models import LoyaltyInbox
    rewards=funded(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row),now=SALE)
    store.receive(SHOP,'old-refund-wakeup','refunds/create',dict(order_id=evidence(row)['order_id'],
        refund_id='gid://shopify/Refund/99'),'a'*64,SALE)
    with Session(store.engine) as s:
        receipt=s.exec(select(LoyaltyInbox).where(LoyaltyInbox.delivery_id=='old-refund-wakeup')).one()
        receipt.status='done';receipt.completed_at=naive(SALE);s.add(receipt);s.commit()
    store.minimize_old_receipts(SHOP,1,now=SALE+timedelta(days=40))
    with Session(store.engine) as s:
        receipt=s.exec(select(LoyaltyInbox).where(LoyaltyInbox.delivery_id=='old-refund-wakeup')).one()
        assert 'Refund/99' in receipt.evidence_json

def test_exception_debit_keeps_earning_worker_and_available_points_working(store):
    from sqlmodel import SQLModel
    from app.models import AuditLog,User
    from test_loyalty_domain import order,refund,POLICY
    from test_loyalty_refund_recovery import earn
    from app.loyalty.service import canonical_evidence
    from app.loyalty.models import LoyaltyEntitlement
    SQLModel.metadata.create_all(store.engine,tables=[User.__table__,AuditLog.__table__])
    rewards=funded(store);row=reserve(rewards)
    sale=order(9000,id='gid://shopify/Order/99')
    sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    assert earn(store,sale,'exception-sale')==90
    rewards.observe(SHOP,row.id,evidence(row,canonical_order=sale))
    rewards.failed(SHOP,row.id,RedemptionError('order_payment_or_refund_review'))
    service=Redemptions(store,policy_getter=lambda:POLICY)
    service.adjudicate_reviewed(SHOP,row.id,fingerprint(canonical_evidence(sale)),'debit','sale-review',lambda *a:SimpleNamespace(id=None))
    assert rewards.overview(SHOP,CUSTOMER)['available_points']==590
    assert not rewards.overview(SHOP,CUSTOMER)['needs_review']
    with Session(store.engine) as s:
        entitlement=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.order_id==sale['id'])).one()
        assert not entitlement.manual_hold and entitlement.status=='eligible'
    sale.update(financial_status='REFUNDED',updated_at=(SALE+timedelta(days=1)).isoformat())
    sale['refunds']=[refund(9000)];sale['refunds'][0]['lines'][0]['quantity']=1
    rewards.observe(SHOP,row.id,evidence(row,canonical_order=sale))
    args=(SHOP,row.id,fingerprint(canonical_evidence(sale)),'restore','refund-review',lambda *a:SimpleNamespace(id=None))
    with pytest.raises(RedemptionError,match='earning_refund_review'): service.adjudicate_reviewed(*args)
    assert earn(store,sale,'exception-refund')==-90
    # Manual earning correction holds do not prevent a verified reward refund.
    with Session(store.engine) as s:
        entitlement=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.order_id==sale['id'])).one()
        entitlement.manual_hold=True;entitlement.reason='manual_correction_hold';s.add(entitlement);s.commit()
    service.adjudicate_reviewed(*args)
    assert store.balance(SHOP,CUSTOMER)==1000
    assert store.rebuild_check(SHOP)==[]


def test_old_resolution_form_cannot_acknowledge_new_refund(ops):
    c,store,_,_=ops
    add_entry(store.engine,1000,customer='3',shop=SHOP)
    rewards=Redemptions(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row,partial_refund=True,updated_at=SALE.isoformat()),now=SALE)
    old_hash=fingerprint(json.loads(rewards.get(SHOP,row.id).evidence_json))
    rewards.settle(SHOP,row.id,evidence(row,partial_refund=True,refunds=[{'id':'new-refund'}],updated_at=(SALE+timedelta(days=1)).isoformat()),now=SALE)
    assert c.post('/loyalty/rewards/resolve',data=dict(csrf_token=login(c),redemption_id=row.id,
        reference='old-form',debit_verified='yes',evidence_hash=old_hash)).status_code==409
    assert rewards.get(SHOP,row.id).state=='review'


def test_success_without_order_clears_retry_generation(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.failed(SHOP,row.id,TimeoutError())
    pending=rewards.get(SHOP,row.id)
    assert pending.work_generation>pending.acknowledged_generation
    rewards.succeeded(SHOP,row.id,pending.work_generation,None)
    saved=rewards.get(SHOP,row.id)
    assert saved.work_generation==saved.acknowledged_generation
