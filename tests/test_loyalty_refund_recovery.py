from copy import deepcopy
from datetime import timedelta
import asyncio
import pytest
from sqlmodel import Session, select
from app.loyalty.domain import calculate, full_refund
from app.loyalty.models import LoyaltyEntitlement
from app.loyalty.redemption import Redemptions, RedemptionError, reconcile
from app.loyalty.service import canonical_evidence
from test_loyalty_service import store, receipt, claim, finish
from test_loyalty_domain import order, refund, POLICY, SALE, attachment
from test_loyalty_redemption import reserve, evidence, SHOP, CUSTOMER


def proof(row, sale):
    result = calculate(sale, POLICY, attachment())
    return evidence(row, order_id=sale['id'], updated_at=sale['updated_at'],
        observed_at=SALE.isoformat(), basis_hash=result.basis_hash,
        refunds=deepcopy(sale['refunds']), canonical_order=canonical_evidence(sale),
        full_refund=full_refund(sale), partial_refund=bool(sale['refunds']) and not full_refund(sale))


def earn(store, sale, key):
    store.receive(SHOP, key, 'orders/updated', dict(order_id=sale['id'],
        customer_id=CUSTOMER, updated_at=sale['updated_at']), 'a'*64, SALE)
    return finish(store, claim(store), sale)


@pytest.mark.parametrize('earning_first', [True, False])
@pytest.mark.parametrize('settlement_first', [True, False])
def test_full_refund_workers_converge_without_spendable_refunded_earnings(store, earning_first, settlement_first):
    receipt(store); finish(store, claim(store), order(50000))
    rewards = Redemptions(store); row = reserve(rewards)
    sale = order(9000, id='gid://shopify/Order/99')
    sale['lines'][0].update(original_cents=10000, discounts_cents=[1000])
    if settlement_first: rewards.settle(SHOP, row.id, proof(row, sale), now=SALE)
    assert earn(store, sale, 'purchase') == 90
    old_sale = deepcopy(sale)
    sale.update(edited=True, financial_status='REFUNDED', updated_at=(SALE+timedelta(minutes=1)).isoformat())
    sale['refunds'] = [refund(9000)]
    sale['refunds'][0]['lines'][0]['quantity'] = 1
    if earning_first: assert earn(store, sale, 'refund') == -90
    result = rewards.settle(SHOP, row.id, proof(row, sale), now=SALE)
    assert result.state == 'restored'
    assert rewards.overview(SHOP, CUSTOMER)['available_points'] == 500
    assert earn(store, sale, 'refund-repeat') == 0
    assert earn(store, old_sale, 'delayed-sale') == 0
    assert rewards.overview(SHOP, CUSTOMER)['available_points'] == 500
    assert store.rebuild_check(SHOP) == []


def test_partial_then_full_preserves_history_and_restores_once(store):
    receipt(store); finish(store, claim(store), order(50000))
    rewards = Redemptions(store); row = reserve(rewards)
    sale = order(9000, id='gid://shopify/Order/99')
    sale['lines'][0].update(original_cents=10000, discounts_cents=[1000])
    sale.update(financial_status='PARTIALLY_REFUNDED', updated_at=(SALE+timedelta(minutes=1)).isoformat())
    sale['refunds'] = [refund(1000)]
    assert rewards.settle(SHOP, row.id, proof(row, sale)).state == 'review'
    bad = deepcopy(sale); bad['refunds'] = []
    with pytest.raises(RedemptionError, match='same_timestamp_conflict'):
        rewards.settle(SHOP, row.id, proof(row, bad))
    bad['updated_at'] = (SALE+timedelta(minutes=2)).isoformat()
    with pytest.raises(RedemptionError, match='refund_history_changed'):
        rewards.settle(SHOP, row.id, proof(row, bad))
    sale.update(edited=True, financial_status='REFUNDED', updated_at=(SALE+timedelta(minutes=2)).isoformat())
    sale['refunds'].append(refund(8000, 31)); sale['refunds'][1]['lines'][0]['quantity'] = 1
    for _ in range(2): assert rewards.settle(SHOP, row.id, proof(row, sale)).state == 'restored'
    assert store.balance(SHOP, CUSTOMER) == 500


def test_refunding_merchandise_but_retaining_gift_card_is_partial():
    sale = order(19000, financial_status='PARTIALLY_REFUNDED')
    sale['lines'][0].update(original_cents=10000, discounts_cents=[1000])
    sale['lines'].append(dict(sale['lines'][0], id='gid://shopify/LineItem/12',
        gift_card=True, original_cents=10000, discounts_cents=[0]))
    sale['refunds'] = [refund(9000)]; sale['refunds'][0]['lines'][0]['quantity'] = 1
    assert calculate(sale, POLICY, attachment()).net_cents == 0
    assert not full_refund(sale)


def test_failures_are_durable_sanitized_and_do_not_release_holds(store):
    receipt(store); finish(store, claim(store), order(50000))
    rewards = Redemptions(store); row = reserve(rewards)
    class Provider:
        async def order_evidence(self, row): raise RuntimeError('secret response')
    for _ in range(8):
        with pytest.raises(RuntimeError): asyncio.run(reconcile(rewards, Provider(), rewards.get(SHOP, row.id)))
    row = rewards.get(SHOP, row.id)
    assert row.state == 'review' and row.failure_count == 8 and row.reason == 'provider_retry'
    assert rewards.overview(SHOP, CUSTOMER)['reserved_points'] == 500
    class Missing:
        async def order_evidence(self, row): return None
        async def deactivate(self, row): raise AssertionError('must not turn a failed reconciliation into a releasable reservation')
    assert asyncio.run(reconcile(rewards, Missing(), row)).reason == 'provider_retry'


def test_reviewed_purchase_earnings_cannot_fund_a_reward(store):
    receipt(store); finish(store, claim(store), order(50000))
    with Session(store.engine) as s:
        row=s.exec(select(LoyaltyEntitlement)).one(); row.status='review'; s.add(row); s.commit()
    rewards=Redemptions(store)
    assert rewards.overview(SHOP, CUSTOMER)['available_points'] == 0
    with pytest.raises(RedemptionError, match='insufficient_points'): reserve(rewards)


def test_additive_reconciliation_upgrade_preserves_existing_reservation(store):
    from sqlalchemy import text
    from app.loyalty.schema import install_schema, schema_ready
    receipt(store); finish(store, claim(store), order(50000))
    rewards=Redemptions(store); row=reserve(rewards)
    with store.engine.begin() as c:
        c.execute(text('ALTER TABLE loyalty_redemption DROP COLUMN evidence_json'))
        c.execute(text('ALTER TABLE loyalty_redemption DROP COLUMN failure_count'))
    assert not schema_ready(store.engine)
    install_schema(store.engine); install_schema(store.engine)
    assert schema_ready(store.engine)
    upgraded=rewards.get(SHOP,row.id)
    assert upgraded.code==row.code and upgraded.evidence_json=='{}' and upgraded.failure_count==0
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500


def test_changed_original_basis_blocks_edited_full_refund(store):
    receipt(store); finish(store,claim(store),order(50000))
    rewards=Redemptions(store); row=reserve(rewards)
    sale=order(9000,id='gid://shopify/Order/99')
    sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    rewards.settle(SHOP,row.id,proof(row,sale))
    sale['lines'][0]['quantity']=2
    sale.update(edited=True,financial_status='REFUNDED',updated_at=(SALE+timedelta(minutes=1)).isoformat())
    sale['refunds']=[refund(9000)]; sale['refunds'][0]['lines'][0]['quantity']=2
    with pytest.raises(RedemptionError,match='original_basis_changed'):
        rewards.settle(SHOP,row.id,proof(row,sale))
    assert store.balance(SHOP,CUSTOMER)==0


def test_full_cash_and_quantity_do_not_replace_line_allocation_proof():
    from app.loyalty.domain import Review
    sale=order(9000,financial_status='REFUNDED')
    sale['refunds']=[refund(1000)]
    part=sale['refunds'][0]; part['lines'][0]['quantity']=1
    part['shipping_cents']=8000; part['transactions'][0]['cents']=9000
    with pytest.raises(Review): calculate(sale,POLICY,attachment())
