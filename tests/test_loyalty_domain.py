from copy import deepcopy
from datetime import datetime, timezone, timedelta
import pytest
from app.loyalty.domain import (Policy, Review, calculate, cents, gid, timestamp)

UTC = timezone.utc
SALE = datetime(2026, 9, 11, 12, tzinfo=UTC)
POLICY = Policy('test.myshopify.com', 'gid://shopify/Shop/1', frozenset({'gid://shopify/Location/2'}), SALE - timedelta(days=1))

def order(amount=1999, **changes):
    value = dict(id='gid://shopify/Order/10', shop_id=POLICY.shop_id, source='pos', location_id='gid://shopify/Location/2',
        created_at=SALE.isoformat(), processed_at=SALE.isoformat(), updated_at=SALE.isoformat(),
        customer_id='gid://shopify/Customer/3', test=False, edited=False, returns=False, exchanges=False, currency='USD',
        taxes_included=False, total_cents=amount, received_cents=amount, payment_rounding_cents=0, refund_rounding_cents=0, financial_status="PAID", outstanding_cents=0,
        lines=[dict(id='gid://shopify/LineItem/11', quantity=1, original_cents=amount, discounts_cents=[0],
            tax_cents=0, gift_card=False, merchandise=True)],
        payments=[dict(id='gid://shopify/OrderTransaction/21', kind='SALE', status='SUCCESS', rounding_cents=0, cents=amount, processed_at=SALE.isoformat())], refunds=[])
    value.update(changes)
    return value

def attachment(o=None, at=SALE):
    return {'customer_id': (o or order())['customer_id'], 'at': at.isoformat(), 'kind': 'canonical_observation'}

def refund(amount, rid=30, **changes):
    value = dict(id=f'gid://shopify/Refund/{rid}', lines=[dict(id=f'gid://shopify/RefundLineItem/{rid}', line_id='gid://shopify/LineItem/11', quantity=0, subtotal_cents=amount, tax_cents=0)], shipping_cents=0, shipping_tax_cents=0, adjustments=False, duties=False,
        transactions=[dict(id=f'gid://shopify/OrderTransaction/{rid}', kind='REFUND', status='SUCCESS', rounding_cents=0, cents=amount)])
    value.update(changes)
    return value

@pytest.mark.parametrize('amount,points', [(1949,19),(1950,20),(1999,20),(0,0),(50,1),(49,0)])
def test_whole_order_half_up(amount,points):
    assert calculate(order(amount), POLICY, attachment()).points == points

def test_cumulative_refunds_and_full_reversal():
    o=order()
    assert calculate(o,POLICY,attachment()).points==20
    o['refunds']=[refund(25)]
    assert calculate(o,POLICY,attachment()).points==20
    o['refunds'].append(refund(25,31))
    assert calculate(o,POLICY,attachment()).points==19
    o['refunds'].append(refund(1949,32))
    assert calculate(o,POLICY,attachment()).points==0

def test_all_allocations_and_inclusive_tax_no_double_refund():
    o=order(5500, taxes_included=True)
    o['lines'][0].update(original_cents=5500, discounts_cents=[300,250], tax_cents=450)
    o['total_cents']=o['received_cents']=4950
    o['payments'][0]['cents']=4950
    assert calculate(o,POLICY,attachment()).net_cents==4500
    o['refunds']=[refund(1000)]
    o['refunds'][0]['lines'][0]['tax_cents']=100
    o['refunds'][0]['transactions'][0]['cents']=1100
    assert calculate(o,POLICY,attachment()).points==35

def test_inclusive_refund_subtotal_is_disambiguated_by_transaction():
    o=order(5500,taxes_included=True)
    o['lines'][0]['tax_cents']=500
    o['refunds']=[refund(1100)]
    o['refunds'][0]['lines'][0]['tax_cents']=100
    assert calculate(o,POLICY,attachment()).points==40

def test_gift_issuance_excluded_and_gift_tender_not_excluded():
    o=order(5000)
    o['lines'].append(dict(o['lines'][0],id='gid://shopify/LineItem/12',gift_card=True,original_cents=2000))
    o['total_cents']=o['received_cents']=7000
    o['payments'][0].update(cents=7000,gateway='gift_card')
    assert calculate(o,POLICY,attachment()).points==50

def test_tax_shipping_only_refund_has_no_effect():
    o=order(5000)
    o['refunds']=[refund(0,lines=[],shipping_cents=900,shipping_tax_cents=100,transactions=[dict(id='gid://shopify/OrderTransaction/30',kind='REFUND',status='SUCCESS',rounding_cents=0,cents=1000)])]
    assert calculate(o,POLICY,attachment()).points==50

@pytest.mark.parametrize('changes,reason', [({'source':'web'},'channel_excluded'),({'source':'tiktok'},'channel_excluded'),({'test':True},'test_order'),({'location_id':'gid://shopify/Location/99'},'location_excluded'),({'created_at':'2026-09-09T00:00:00Z'},'prelaunch'),({'processed_at':'2026-09-09T00:00:00Z'},'prelaunch')])
def test_exclusions(changes,reason):
    result=calculate(order(**changes),POLICY,attachment())
    assert result.state=='excluded' and result.reason==reason

@pytest.mark.parametrize('changes', [{'edited':True},{'exchanges':True},{'location_id':None},{'created_at':None},{'currency':'CAD'},{'received_cents':1}])
def test_unsupported_evidence_reviews(changes):
    with pytest.raises(Review): calculate(order(**changes),POLICY,attachment())

def test_attachment_no_contact_requirement_and_uncertain_timing_review():
    o=order()
    assert calculate(o,POLICY,attachment()).points==20
    with pytest.raises(Review,match='attachment'): calculate(o,POLICY,None)
    with pytest.raises(Review,match='attachment'): calculate(o,POLICY,dict(attachment(),kind='updated_at'))
    assert calculate(o,POLICY,dict(attachment(at=SALE+timedelta(days=7)),kind='verified_attachment')).points==20
    with pytest.raises(Review,match='attachment'): calculate(o,POLICY,attachment(at=SALE+timedelta(days=7,seconds=1)))
    assert calculate(order(customer_id=None),POLICY,None).state=='pending'

@pytest.mark.parametrize('value', [True,1.2,'1.001','NaN','Infinity','-1','',None])
def test_money_rejects_inexact_or_invalid(value):
    with pytest.raises(Review): cents(value)

@pytest.mark.parametrize('value', [True,0,-1,'gid://shopify/Order/3','1.2','',None])
def test_customer_identifier_strict(value):
    with pytest.raises(Review): gid('Customer',value)

def test_exact_money_id_and_timestamp():
    assert cents('19.99')==1999
    assert gid('Customer','003')=='gid://shopify/Customer/3'
    with pytest.raises(Review): timestamp('2026-09-10')

@pytest.mark.parametrize('mutate', ['pending','mixed','amount_only','excess','duplicate','adjustment'])
def test_refund_ambiguity_reviews(mutate):
    o=order(); r=refund(100); o['refunds']=[r]
    if mutate=='pending': r['transactions'][0]['status']='PENDING'
    if mutate=='mixed': r['transactions'].append(dict(r['transactions'][0],id='gid://shopify/OrderTransaction/99',status='FAILURE'))
    if mutate=='amount_only': r['lines']=[]
    if mutate=='excess': r['lines'][0]['subtotal_cents']=2000; r['transactions'][0]['cents']=2000
    if mutate=='duplicate': o['refunds'].append(deepcopy(r))
    if mutate=='adjustment': r['adjustments']=True
    with pytest.raises(Review): calculate(o,POLICY,attachment())


def test_ordinary_return_refund_automatic_but_exchange_reviews():
    o=order(returns=True);o['refunds']=[refund(50)]
    assert calculate(o,POLICY,attachment()).points==19
    o['exchanges']=True
    with pytest.raises(Review,match='exchange'):calculate(o,POLICY,attachment())


@pytest.mark.parametrize('adjustment',[-2,1])
@pytest.mark.parametrize('rounded_amount',[False,True])
def test_cash_rounding_exact_evidence_and_failed_payment_retry(adjustment,rounded_amount):
    o=order();o['payment_rounding_cents']=adjustment;o['payments'][0]['rounding_cents']=adjustment
    if rounded_amount:
        o['received_cents']+=adjustment;o['payments'][0]['cents']+=adjustment
    o['payments'].insert(0,dict(o['payments'][0],id='gid://shopify/OrderTransaction/99',status='FAILURE'))
    assert calculate(o,POLICY,attachment()).points==20
    o['payment_rounding_cents']=0
    with pytest.raises(Review,match='payment'):calculate(o,POLICY,attachment())


def test_cash_refund_rounding_does_not_change_merchandise_basis():
    o=order(returns=True);r=refund(50);o['refunds']=[r]
    r['transactions'][0].update(cents=49,rounding_cents=-1);o['refund_rounding_cents']=-1
    assert calculate(o,POLICY,attachment()).points==19
    r['transactions'][0]['rounding_cents']=0
    with pytest.raises(Review):calculate(o,POLICY,attachment())


def test_rounding_does_not_disambiguate_two_different_tax_interpretations():
    o=order(100,taxes_included=True);o['lines'][0]['tax_cents']=1
    r=refund(50);r['lines'][0]['tax_cents']=1;r['transactions'][0].update(cents=50,rounding_cents=-1)
    o['refunds']=[r];o['refund_rounding_cents']=-1
    with pytest.raises(Review,match='refund_allocation'):calculate(o,POLICY,attachment())
