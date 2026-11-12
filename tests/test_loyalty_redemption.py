from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import asyncio
import pytest
from app.loyalty.redemption import Redemptions, RedemptionError, reconcile
from app.loyalty.reward_policy import Reward, redemption_gates
from app.loyalty.redemption_shopify import discount_input
from test_loyalty_service import store, receipt, claim, finish
from test_loyalty_domain import order, POLICY, SALE, calculate, attachment

SHOP=POLICY.shop_domain
CUSTOMER='gid://shopify/Customer/3'


def funded(store):
    receipt(store)
    finish(store,claim(store),order(100000))
    return Redemptions(store)

def reserve(rewards,key='request_key_00001'):
    return rewards.reserve(SHOP,CUSTOMER,'a'*64,key,Reward(),now=SALE)

def evidence(row,**changes):
    result=dict(order_id='gid://shopify/Order/99',customer_id=CUSTOMER,code=row.code,
        discount_cents=1000,eligible_cents=10000,currency='USD',stacked=False,
        test=False,paid=True,full_refund=False,partial_refund=False)
    result.update(changes)
    return result

def test_reservation_is_idempotent_and_does_not_post(store):
    rewards=funded(store); row=reserve(rewards)
    assert reserve(rewards).id==row.id
    assert rewards.overview(SHOP,CUSTOMER)==dict(balance_points=1000,reserved_points=500,available_points=500,needs_review=False)
    with pytest.raises(RedemptionError,match='pending'):reserve(rewards,'request_key_00002')
    with pytest.raises(RedemptionError,match='conflict'):
        rewards.reserve(SHOP,CUSTOMER,'b'*64,'request_key_00001',Reward(),now=SALE)

def test_concurrent_cashiers_only_reserve_once(store):
    rewards=funded(store)
    def attempt(n):
        try:return reserve(rewards,f'request_key_{n:05}').id
        except RedemptionError:return None
    with ThreadPoolExecutor(max_workers=4) as pool: results=list(pool.map(attempt,range(8)))
    assert len([x for x in results if x])==1
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500

def test_sale_and_full_return_post_once_and_rebuild_matches(store):
    rewards=funded(store);row=reserve(rewards)
    for _ in range(3):rewards.settle(SHOP,row.id,evidence(row),now=SALE)
    assert store.balance(SHOP,CUSTOMER)==500
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==0
    assert store.rebuild_check(SHOP)==[]
    for _ in range(3):rewards.settle(SHOP,row.id,evidence(row,full_refund=True),now=SALE+timedelta(minutes=1))
    assert store.balance(SHOP,CUSTOMER)==1000
    assert store.rebuild_check(SHOP)==[]

@pytest.mark.parametrize('changes',[{'customer_id':'gid://shopify/Customer/9'},
    {'discount_cents':2000},{'eligible_cents':9999},{'stacked':True},{'paid':False},{'currency':'CAD'}])
def test_invalid_order_never_spends_points(store,changes):
    rewards=funded(store);row=reserve(rewards)
    with pytest.raises(RedemptionError):rewards.settle(SHOP,row.id,evidence(row,**changes),now=SALE)
    assert store.balance(SHOP,CUSTOMER)==1000

def test_partial_return_does_not_restore_points(store):
    rewards=funded(store);row=reserve(rewards)
    result=rewards.settle(SHOP,row.id,evidence(row,partial_refund=True),now=SALE)
    assert result.state=='review' and store.balance(SHOP,CUSTOMER)==500

def test_expiry_and_no_order_do_not_release_points(store):
    rewards=funded(store);row=reserve(rewards)
    class Provider:
        async def order_evidence(self,row):return None
        async def deactivate(self,row):pass
    result=asyncio.run(reconcile(rewards,Provider(),row))
    assert result.state=='review'
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500

def test_config_and_discount_contract(store):
    assert redemption_gates(object())==['redemption_disabled']
    with pytest.raises(ValueError):Reward(discount_cents=1500)
    row=reserve(funded(store));row.eligibility_json='{"requested":["gid://shopify/ProductVariant/11"],"eligible":["gid://shopify/ProductVariant/11"]}';data=discount_input(row)
    assert data['usageLimit']==1 and not any(data['combinesWith'].values())
    assert data['context']=={'customers':{'add':[CUSTOMER]}}
    assert data['minimumRequirement']['subtotal']['greaterThanOrEqualToSubtotal']=='100.00'
    assert data['customerGets']['value']['discountAmount']['amount']=='10.00'
    assert data['customerGets']['items']=={'products':{'productVariantsToAdd':['gid://shopify/ProductVariant/11']}}
    assert 'appliesOnOneTimePurchase' not in data['customerGets']
    assert 'appliesOnSubscription' not in data['customerGets']

def test_earning_uses_paid_merchandise_after_reward():
    sale=order(9000)
    sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    assert calculate(sale,POLICY,attachment()).points==90

def test_stale_snapshot_cannot_clear_partial_refund_review(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row,partial_refund=True,updated_at=(SALE+timedelta(hours=1)).isoformat()),now=SALE)
    with pytest.raises(RedemptionError,match='stale'):
        rewards.settle(SHOP,row.id,evidence(row,updated_at=SALE.isoformat()),now=SALE)
    assert rewards.get(SHOP,row.id).reason=='partial_refund_review'

def test_unchecked_reservation_is_prioritized_over_polled_orders(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row),now=SALE)
    next_row=reserve(rewards,'another_request_001')
    assert rewards.pending(SHOP,1)[0].id==next_row.id


def test_paused_reconciliation_never_creates_a_discount(store):
    rewards=funded(store);row=reserve(rewards)
    class Provider:
        async def order_evidence(self,row):return None
        async def ensure_discount(self,row):raise AssertionError('new issuance while paused')
        async def deactivate(self,row):pass
    result=asyncio.run(reconcile(rewards,Provider(),row,allow_prepare=False))
    assert result.state=='review' and rewards.overview(SHOP,CUSTOMER)['reserved_points']==500


def test_rule_version_identifies_changed_terms():
    assert Reward().version != Reward(points=1000).version
    assert Reward().version != Reward(discount_cents=500).version


def test_recently_checked_historical_orders_do_not_fill_active_queue(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row))
    assert rewards.pending(SHOP)==[]
    new=reserve(rewards,'next_request_0002')
    assert [r.id for r in rewards.pending(SHOP)]==[new.id]


def test_canonical_receipt_wakes_known_sale_for_refund_check(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row))
    assert rewards.pending(SHOP)==[]
    store.receive(SHOP,'refund-event','refunds/create',dict(order_id=evidence(row)['order_id']), 'b'*64)
    assert [r.id for r in rewards.pending(SHOP)]==[row.id]


def test_polling_reserves_capacity_for_active_and_historical_work(store):
    from sqlmodel import Session
    from app.loyalty.models import LoyaltyRedemption
    rewards=funded(store);template=reserve(rewards)
    with Session(store.engine) as s:
        for n in range(12):
            data=template.model_dump(exclude={'id'})
            data.update(request_key=f'queue_request_{n:05}',code='DEGEN-'+f'{n:032X}',
                state='ready' if n<10 else 'committed',order_id=None if n<10 else f'gid://shopify/Order/{100+n}')
            s.add(LoyaltyRedemption(**data))
        s.commit()
    batch=rewards.pending(SHOP,5)
    assert len(batch)==5
    assert sum(row.state in ('ready','reserved') for row in batch)==4
    assert sum(row.state=='committed' for row in batch)==1
