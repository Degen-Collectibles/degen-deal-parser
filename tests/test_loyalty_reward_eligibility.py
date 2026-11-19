"""Checkout restrictions must be enforced by Shopify, not client price claims."""
import asyncio
import json
from copy import deepcopy
from datetime import timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.loyalty.redemption import Redemptions, RedemptionError
from app.loyalty.redemption_shopify import DiscountProvider, discount_input, verify_discount
from app.loyalty.reward_policy import Reward, variant_ids
from app.loyalty.schema import install_schema, schema_ready
from app.routers.loyalty_redemption import build_router
from test_loyalty_redemption import funded, reserve, SHOP, CUSTOMER
from test_loyalty_redemption_provider import fixture
from test_loyalty_pos import configuration, token, add_entry
from test_loyalty_service import store
from test_loyalty_domain import order, calculate, attachment, POLICY

V1='gid://shopify/ProductVariant/11'
V2='gid://shopify/ProductVariant/12'
V3='gid://shopify/ProductVariant/13'


def catalog():
    return {'shop':{'id':'gid://shopify/Shop/1','myshopifyDomain':'test.myshopify.com',
                    'currencyCode':'USD','taxesIncluded':False},
            'nodes':[{'__typename':'ProductVariant','id':v,
                      'inventoryItem':{'requiresShipping':v!=V3},
                      'product':{'isGiftCard':v==V2}} for v in [V1,V2,V3]]}


def provider(data):
    p=DiscountProvider(None,'test.myshopify.com','synthetic','1','2')
    async def query(q,variables): return data
    p.query=query
    return p


def test_catalog_not_client_classifies_merchandise():
    result=asyncio.run(provider(catalog()).eligibility([V3,V2,V1]))
    assert result=={'requested':[V1,V2,V3],'eligible':[V1]}


@pytest.mark.parametrize('change', ['null','duplicate','missing','unknown_type','missing_flag',
                                   'wrong_shop','tax_inclusive','currency','none_eligible'])
def test_incomplete_or_unsupported_catalog_never_issues(change):
    data=catalog()
    if change=='null':data['nodes'][0]=None
    elif change=='duplicate':data['nodes'][1]=deepcopy(data['nodes'][0])
    elif change=='missing':data['nodes'].pop()
    elif change=='unknown_type':data['nodes'][0]['__typename']='Product'
    elif change=='missing_flag':data['nodes'][0]['inventoryItem']={}
    elif change=='wrong_shop':data['shop']['id']='gid://shopify/Shop/999'
    elif change=='tax_inclusive':data['shop']['taxesIncluded']=True
    elif change=='currency':data['shop']['currencyCode']='CAD'
    else:data['nodes'][0]['inventoryItem']['requiresShipping']=False
    with pytest.raises(RedemptionError):asyncio.run(provider(data).eligibility([V1,V2,V3]))


@pytest.mark.parametrize('ids', [[],[V1,V1],['11'],[11],['gid://shopify/Product/11'],[V1]*101,None])
def test_ids_are_bounded_and_typed(ids):
    with pytest.raises(ValueError):variant_ids(ids)


def test_discount_has_only_bound_variants_and_readback_rejects_widening():
    row,node=fixture()
    assert discount_input(row)['customerGets']['items']=={'products':{'productVariantsToAdd':[V1]}}
    assert verify_discount(row,node)==node['id']
    for bad in [
        {'__typename':'AllDiscountItems','allItems':True},
        {**node['codeDiscount']['customerGets']['items'],
         'products':{'nodes':[{'id':'gid://shopify/Product/1'}],'pageInfo':{'hasNextPage':False}}},
        {**node['codeDiscount']['customerGets']['items'],
         'productVariants':{'nodes':[{'id':V1},{'id':V2}],'pageInfo':{'hasNextPage':False}}},
        {**node['codeDiscount']['customerGets']['items'],
         'productVariants':{'nodes':[{'id':V1}],'pageInfo':{'hasNextPage':True}}},
    ]:
        changed=deepcopy(node);changed['codeDiscount']['customerGets']['items']=bad
        with pytest.raises(RedemptionError):verify_discount(row,changed)
    row.eligibility_json='{}'
    with pytest.raises(RedemptionError):discount_input(row)


def test_idempotency_cannot_change_eligible_scope(store):
    rewards=funded(store)
    args=(SHOP,CUSTOMER,'a'*64,'same_checkout_0001',Reward())
    scope={'requested':[V1,V2],'eligible':[V1]}
    first=rewards.reserve(*args,eligibility=scope)
    assert rewards.reserve(*args,eligibility=scope).id==first.id
    with pytest.raises(RedemptionError,match='request_key_conflict'):
        rewards.reserve(*args,eligibility={'requested':[V1,V2],'eligible':[V1,V2]})
    with pytest.raises(RedemptionError,match='request_key_conflict'):
        rewards.reserve(*args,eligibility={'requested':[V1],'eligible':[V1]})


def test_api_rejects_bad_catalog_before_holding_points(store):
    add_entry(store.engine,1000)
    class Denied:
        async def eligibility(self,ids):raise RedemptionError('catalog_incomplete')
        async def ensure_discount(self,row):raise AssertionError('must not issue')
    settings=configuration(loyalty_redemption_enabled=True,loyalty_redemption_all_staff_enabled=True,
        loyalty_redemption_provider_verified=True,loyalty_location_ids='2',
        loyalty_launch_at='2026-01-01T00:00:00Z',loyalty_processing_enabled=True,
        loyalty_posting_enabled=True,loyalty_read_access_verified=True,loyalty_budget_cents=0,
        loyalty_exception_owner='test',loyalty_evidence_retention_days=30,
        loyalty_ledger_retention_days=30,loyalty_backup_retention_days=30)
    app=FastAPI();app.include_router(build_router(store.engine,settings,lambda _:Denied()))
    c=TestClient(app); headers={'Authorization':'Bearer '+token()}
    path='/api/loyalty/pos/customers/101/rewards'
    for data in [{'action':'eligibility','variant_ids':[V1]},
                 {'action':'reserve','reward_points':500,'request_key':'catalog_failure_001','variant_ids':[V1]}]:
        result=c.post(path,headers=headers,json=data)
        assert result.status_code==409 and result.json()['error']=='catalog_incomplete'
    assert c.get(path,headers=headers).json()['reserved_points']==0


def test_additive_scope_upgrade_does_not_rewrite_existing_hold(store):
    rewards=funded(store);row=reserve(rewards)
    with store.engine.begin() as conn:
        conn.execute(text('ALTER TABLE loyalty_redemption DROP COLUMN eligibility_json'))
    assert not schema_ready(store.engine)
    install_schema(store.engine);install_schema(store.engine)
    saved=rewards.get(SHOP,row.id)
    assert saved.code==row.code and saved.eligibility_json=='{}'
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500
    with pytest.raises(RedemptionError):discount_input(saved)


def test_known_service_value_earns_no_points_in_mixed_order():
    sale=order(14000)
    sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    sale['lines'].append(dict(sale['lines'][0],id='gid://shopify/LineItem/12',
        original_cents=5000,discounts_cents=[0],merchandise=False))
    assert calculate(sale,POLICY,attachment()).points==90


def test_custom_only_catalog_check_still_verifies_shop_and_currency():
    data=catalog();data['nodes']=[]
    assert asyncio.run(provider(data).eligibility([],custom_sales=True)) == {
        'requested':[], 'eligible':[], 'custom_sales':True}
    data['shop']['taxesIncluded']=True
    with pytest.raises(RedemptionError,match='pricing_unsupported'):
        asyncio.run(provider(data).eligibility([],custom_sales=True))


def test_custom_scope_cannot_include_services_or_gift_cards():
    with pytest.raises(RedemptionError,match='custom_sale_excluded_items'):
        asyncio.run(provider(catalog()).eligibility([V1,V2,V3],custom_sales=True))


def test_custom_code_scope_is_explicit_and_readback_cannot_change_it():
    row,node=fixture()
    row.eligibility_json=json.dumps({'requested':[], 'eligible':[], 'custom_sales':True})
    assert discount_input(row)['customerGets']['items']=={'all':True}
    with pytest.raises(RedemptionError):verify_discount(row,node)
    node['codeDiscount']['customerGets']['items']={'__typename':'AllDiscountItems','allItems':True}
    assert verify_discount(row,node)==node['id']
    row.eligibility_json=json.dumps({'requested':[V1], 'eligible':[V1]})
    with pytest.raises(RedemptionError):verify_discount(row,node)


def test_custom_only_reservation_is_idempotent_and_cannot_change_scope(store):
    rewards=funded(store)
    args=(SHOP,CUSTOMER,'a'*64,'custom_checkout_0001',Reward())
    scope={'requested':[],'eligible':[],'custom_sales':True}
    first=rewards.reserve(*args,eligibility=scope)
    assert rewards.reserve(*args,eligibility=scope).id==first.id
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500
    with pytest.raises(RedemptionError,match='request_key_conflict'):
        rewards.reserve(*args,eligibility={'requested':[V1],'eligible':[V1]})


def test_custom_receipt_minimum_refund_and_missing_variant_proof(monkeypatch):
    from test_loyalty_domain import order, SALE, refund
    from app.loyalty import redemption_shopify as module
    sale=order(9000);sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    row,_=fixture();row.shop='test.myshopify.com';row.first_observed_at=SALE.replace(tzinfo=None)
    row.eligibility_json=json.dumps({'requested':[],'eligible':[],'custom_sales':True})
    class Reader:
        def __init__(self,*a,**kw):pass
        async def order(self,*a):return sale
    monkeypatch.setattr(module,'Reader',Reader)
    p=DiscountProvider(None,row.shop,'synthetic','1','2')
    line={'id':sale['lines'][0]['id'],'variant':None}
    async def query(q,v):
        if 'RewardOrders' in q:return {'orders':{'nodes':[{'id':sale['id']}],'pageInfo':{'hasNextPage':False}}}
        return {'order':{'id':sale['id'],'updatedAt':sale['updated_at'],
            'lineItems':{'nodes':[line],'pageInfo':{'hasNextPage':False}},
            'discountApplications':{'nodes':[{'__typename':'DiscountCodeApplication','code':row.code}],
                                    'pageInfo':{'hasNextPage':False}}}}
    p.query=query
    proof=asyncio.run(p.order_evidence(row))
    assert proof['eligible_cents']==10000 and proof['discount_cents']==1000
    sale.update(edited=True,financial_status='REFUNDED');sale['refunds']=[refund(9000)]
    sale['refunds'][0]['lines'][0]['quantity']=1
    assert asyncio.run(p.order_evidence(row))['full_refund']
    line.pop('variant')
    with pytest.raises(RedemptionError,match='order_line_scope_unverified'):
        asyncio.run(p.order_evidence(row))
    line['variant']=None;sale['lines'][0]['merchandise']=False
    with pytest.raises(RedemptionError,match='ineligible_discount_review'):
        asyncio.run(p.order_evidence(row))
