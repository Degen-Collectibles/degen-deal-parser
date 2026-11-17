"""Regression coverage for independently reproduced reward recovery failures."""
import asyncio
import pytest
from sqlmodel import Session
from app.loyalty.models import LoyaltyRedemption
from app.loyalty.redemption import Redemptions, RedemptionError, reconcile
from test_loyalty_service import store
from test_loyalty_redemption import funded, reserve, evidence, SHOP, CUSTOMER, SALE

@pytest.mark.parametrize('state', ['committed', 'cancelled'])
def test_historical_transport_errors_never_rehold_or_block(store, state):
    rewards = funded(store); row = reserve(rewards)
    if state == 'committed':
        rewards.settle(SHOP, row.id, evidence(row), now=SALE)
    else:
        with Session(store.engine) as s:
            item=s.get(LoyaltyRedemption,row.id)
            item.state='cancelled'; item.reason='administrator_verified_unused'
            s.add(item); s.commit()
    for _ in range(12): rewards.failed(SHOP,row.id,TimeoutError())
    assert rewards.get(SHOP,row.id).state == state
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points'] == 0
    reserve(rewards,'request_key_00002')


def test_successful_empty_check_resets_consecutive_failures(store):
    rewards=funded(store); row=reserve(rewards)
    class Provider:
        async def order_evidence(self,row): return None
        async def deactivate(self,row): pass
    for _ in range(7): rewards.failed(SHOP,row.id,TimeoutError())
    asyncio.run(reconcile(rewards,Provider(),rewards.get(SHOP,row.id)))
    assert rewards.get(SHOP,row.id).failure_count == 0


def test_verified_partial_refund_allows_next_reward_without_restoring(store):
    rewards=funded(store); row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row,partial_refund=True),now=SALE)
    assert store.balance(SHOP,CUSTOMER)==500
    assert not rewards.overview(SHOP,CUSTOMER)['needs_review']
    assert rewards.owned_pending(SHOP,CUSTOMER,'a'*64) is None
    reserve(rewards,'request_key_00002')


def test_posting_pause_preserves_hold_and_ledger(store):
    rewards=funded(store); row=reserve(rewards)
    paused=Redemptions(store,posting_allowed=lambda:False)
    class Provider:
        async def order_evidence(self,row): return evidence(row)
    for _ in range(10):
        with pytest.raises(RedemptionError,match='posting_paused'):
            asyncio.run(reconcile(paused,Provider(),paused.get(SHOP,row.id)))
    assert store.balance(SHOP,CUSTOMER)==1000
    assert paused.get(SHOP,row.id).state=='reserved'
    assert paused.overview(SHOP,CUSTOMER)['reserved_points']==500
    assert paused.get(SHOP,row.id).debit_ledger_id is None


def test_negative_balance_is_readable_and_paginated(store):
    from test_loyalty_pos import add_entry, configuration
    from app.loyalty.queries import ledger_snapshot
    from app.loyalty.pos_auth import POSAccess
    add_entry(store.engine,100,customer='101')
    add_entry(store.engine,-500,customer='101')
    settings=configuration()
    data,cursor=ledger_snapshot(store.engine,settings.loyalty_shop_domain,'101',limit=1)
    assert data['balance_points']=='-400' and data['needs_review']
    access=POSAccess.from_settings(settings)
    decoded=access.decode_cursor(access.encode_cursor(cursor),'101')
    second,_=ledger_snapshot(store.engine,settings.loyalty_shop_domain,'101',limit=1,cursor=decoded)
    assert second['balance_points']=='-400'


def test_reward_transport_shop_must_match():
    from test_loyalty_pos import configuration
    from app.loyalty.reward_policy import redemption_gates
    settings=configuration(loyalty_redemption_enabled=True,loyalty_redemption_all_staff_enabled=True,
        loyalty_redemption_provider_verified=True,loyalty_location_ids='2',shopify_store_domain='different.myshopify.com')
    assert redemption_gates(settings)


@pytest.mark.parametrize('partial', [False,True])
def test_old_history_is_only_polled_on_wakeup_or_verified_access(store, partial):
    from datetime import timedelta
    from app.loyalty.service import naive
    rewards=funded(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row,partial_refund=partial),now=SALE)
    with Session(store.engine) as s:
        item=s.get(LoyaltyRedemption,row.id)
        item.created_at=naive()-timedelta(days=61)
        item.checked_at=naive()-timedelta(days=1)
        s.add(item);s.commit()
    assert rewards.pending(SHOP)==[]
    assert [x.id for x in rewards.pending(SHOP,read_all_orders=True)]==[row.id]
    with Session(store.engine) as s:
        item=s.get(LoyaltyRedemption,row.id);item.checked_at=None;s.add(item);s.commit()
    assert [x.id for x in rewards.pending(SHOP)]==[row.id]


def test_policy_mismatch_blocks_reward_posting(store):
    from dataclasses import replace
    from datetime import timedelta
    from test_loyalty_domain import POLICY
    rewards=funded(store); row=reserve(rewards)
    current=replace(POLICY,launch_at=POLICY.launch_at+timedelta(seconds=1))
    guarded=Redemptions(store,policy_getter=lambda:current)
    with pytest.raises(RedemptionError,match='program_policy_changed'):
        guarded.settle(SHOP,row.id,evidence(row),now=SALE)
    assert store.balance(SHOP,CUSTOMER)==1000
    assert rewards.get(SHOP,row.id).debit_ledger_id is None


def test_reviewed_earning_cannot_be_reversed_by_reward_worker(store):
    from app.loyalty.models import LoyaltyEntitlement
    from test_loyalty_refund_recovery import earn,proof
    from test_loyalty_domain import order,refund
    from datetime import timedelta
    rewards=funded(store);row=reserve(rewards)
    sale=order(9000,id='gid://shopify/Order/99')
    sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    earn(store,sale,'purchase')
    rewards.settle(SHOP,row.id,proof(row,sale),now=SALE)
    with Session(store.engine) as s:
        from sqlmodel import select
        entitlement=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.order_id==sale['id'])).one()
        entitlement.status='review';entitlement.reason='program_policy_changed';s.add(entitlement);s.commit()
    sale.update(edited=True,financial_status='REFUNDED',updated_at=(SALE+timedelta(minutes=1)).isoformat())
    sale['refunds']=[refund(9000)];sale['refunds'][0]['lines'][0]['quantity']=1
    balance=store.balance(SHOP,CUSTOMER)
    with pytest.raises(RedemptionError,match='earning_refund_review'):
        rewards.settle(SHOP,row.id,proof(row,sale))
    assert store.balance(SHOP,CUSTOMER)==balance
    assert rewards.get(SHOP,row.id).restore_ledger_id is None


def test_unapproved_configured_reward_terms_rejected():
    from test_loyalty_pos import configuration
    from app.loyalty.reward_policy import Reward
    with pytest.raises(ValueError,match='reward_terms_not_approved'):
        Reward.from_settings(configuration(loyalty_reward_points=50))


def test_debited_evidence_review_stays_visible_without_second_hold(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.settle(SHOP,row.id,evidence(row),now=SALE)
    rewards.failed(SHOP,row.id,RedemptionError('original_basis_changed'))
    assert rewards.get(SHOP,row.id).state=='review'
    assert rewards.get(SHOP,row.id).reason=='original_basis_changed'
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==0
    reserve(rewards,'request_key_00002')


def test_unspent_review_requires_explicit_cancel_and_deactivation(store):
    rewards=funded(store);row=reserve(rewards)
    rewards.failed(SHOP,row.id,RedemptionError('reward_eligibility_changed'))
    calls=[]
    class Provider:
        async def order_evidence(self,row): return None
        async def deactivate(self,row): calls.append(row.id)
    asyncio.run(reconcile(rewards,Provider(),rewards.get(SHOP,row.id)))
    assert calls==[] and rewards.get(SHOP,row.id).reason=='reward_eligibility_changed'
    cancelling=rewards.transition(SHOP,row.id,'cancelling')
    result=asyncio.run(reconcile(rewards,Provider(),cancelling))
    assert calls==[row.id] and result.reason=='unused_code_requires_review'
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500
