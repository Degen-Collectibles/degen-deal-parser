from copy import deepcopy
from datetime import datetime,timezone
from types import SimpleNamespace
import asyncio
import json
import pytest
from app.loyalty.redemption_shopify import verify_discount,DiscountProvider,LOOKUP
from app.loyalty.redemption import RedemptionError

def fixture():
    row=SimpleNamespace(id=1,code='DEGEN-'+'A'*32,customer_id='gid://shopify/Customer/3',
        points=500,discount_cents=1000,minimum_cents=10000,
        created_at=datetime(2026,1,1),expires_at=datetime(2099,1,1),provider_id=None,eligibility_json=json.dumps({'requested':['gid://shopify/ProductVariant/11'],'eligible':['gid://shopify/ProductVariant/11']}))
    node={'id':'gid://shopify/DiscountCodeNode/1','codeDiscount':{
        'title':'Degen reward 1','usageLimit':1,'appliesOncePerCustomer':True,
        'startsAt':'2026-01-01T00:00:00Z','endsAt':'2099-01-01T00:00:00Z',
        'combinesWith':{'orderDiscounts':False,'productDiscounts':False,'shippingDiscounts':False},
        'context':{'__typename':'DiscountCustomers','customers':[{'id':row.customer_id}]},
        'customerGets':{'appliesOnOneTimePurchase':True,'appliesOnSubscription':False,
            'value':{'__typename':'DiscountAmount','amount':{'amount':'10.00','currencyCode':'USD'},'appliesOnEachItem':False},
            'items':{'__typename':'DiscountProducts','products':{'nodes':[],'pageInfo':{'hasNextPage':False}},'productVariants':{'nodes':[{'id':'gid://shopify/ProductVariant/11'}],'pageInfo':{'hasNextPage':False}}}},
        'minimumRequirement':{'__typename':'DiscountMinimumSubtotal',
            'greaterThanOrEqualToSubtotal':{'amount':'100.00','currencyCode':'USD'}}}}
    return row,node

@pytest.mark.parametrize('field,value',[('usageLimit',2),('context',{'__typename':'DiscountBuyerSelectionAll'}),
    ('endsAt',None),('combinesWith',{'orderDiscounts':True}),('minimumRequirement',None)])
def test_changed_provider_restrictions_are_rejected(field,value):
    row,node=fixture();node['codeDiscount'][field]=value
    with pytest.raises(RedemptionError):verify_discount(row,node)

def test_recovers_same_code_without_duplicate_mutation():
    row,node=fixture()
    provider=DiscountProvider(None,'test.myshopify.com','synthetic','1','2')
    calls=[]
    async def query(q,variables):
        calls.append((q,variables));return {'codeDiscountNodeByCode':node}
    provider.query=query
    async def eligibility(ids):return json.loads(row.eligibility_json)
    provider.eligibility=eligibility
    assert asyncio.run(provider.ensure_discount(row))==node['id']
    assert calls==[(LOOKUP,{'code':row.code})]

def test_wrong_currency_or_per_item_discount_is_rejected():
    row,node=fixture();node['codeDiscount']['customerGets']['value']['amount']['currencyCode']='CAD'
    with pytest.raises(RedemptionError):verify_discount(row,node)

def test_canonical_paid_order_and_full_refund_evidence(monkeypatch):
    from test_loyalty_domain import order,SALE,refund
    from app.loyalty import redemption_shopify as module
    sale=order(9000);sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
    row,_=fixture();row.shop='test.myshopify.com';row.first_observed_at=SALE.replace(tzinfo=None)
    class Reader:
        def __init__(self,*a,**kw):pass
        async def order(self,*a):return sale
    monkeypatch.setattr(module,'Reader',Reader)
    provider=DiscountProvider(None,row.shop,'synthetic','1','2')
    async def query(q,variables):
        if 'RewardOrders' in q:return {'orders':{'nodes':[{'id':sale['id']}],'pageInfo':{'hasNextPage':False}}}
        return {'order':{'id':sale['id'],'updatedAt':sale['updated_at'],'lineItems':{'nodes':[{'id':x['id'],'variant':{'id':'gid://shopify/ProductVariant/11'}} for x in sale['lines']],'pageInfo':{'hasNextPage':False}},'discountApplications':{
            'nodes':[{'__typename':'DiscountCodeApplication','code':row.code}],'pageInfo':{'hasNextPage':False}}}}
    provider.query=query
    proof=asyncio.run(provider.order_evidence(row))
    assert proof['paid'] and proof['discount_cents']==1000 and proof['eligible_cents']==10000
    assert not proof['full_refund']
    gift=dict(sale['lines'][0],id='gid://shopify/LineItem/12',gift_card=True,
              original_cents=10000,discounts_cents=[0])
    sale['lines'].append(gift)
    sale['total_cents']=sale['received_cents']=sale['payments'][0]['cents']=19000
    proof=asyncio.run(provider.order_evidence(row))
    assert proof['eligible_cents']==10000 and proof['discount_cents']==1000
    gift['discounts_cents']=[100]
    with pytest.raises(RedemptionError,match='ineligible_discount_review'):
        asyncio.run(provider.order_evidence(row))
    sale['lines'].pop()
    sale['total_cents']=sale['received_cents']=sale['payments'][0]['cents']=9000
    sale['refunds']=[refund(9000)]
    assert not asyncio.run(provider.order_evidence(row))['full_refund']
    sale.update(edited=True,financial_status='REFUNDED')
    sale['refunds'][0]['lines'][0]['quantity']=1
    row.state='committed';row.order_id=sale['id']
    assert asyncio.run(provider.order_evidence(row))['full_refund']
    row.state='ready'
    assert asyncio.run(provider.order_evidence(row))['full_refund']
    row.state='review'
    assert asyncio.run(provider.order_evidence(row))['full_refund']
    for field,value in [('order_id','gid://shopify/Order/999')]:
        original=getattr(row,field);setattr(row,field,value)
        with pytest.raises(RedemptionError):asyncio.run(provider.order_evidence(row))
        setattr(row,field,original)
    sale['refunds'][0]['lines'][0]['quantity']=0
    with pytest.raises(RedemptionError):asyncio.run(provider.order_evidence(row))
    sale['refunds'][0]['lines'][0]['quantity']=1
    sale['exchanges']=True
    with pytest.raises(RedemptionError):asyncio.run(provider.order_evidence(row))
    sale['exchanges']=False
    sale['outstanding_cents']=100
    with pytest.raises(RedemptionError):asyncio.run(provider.order_evidence(row))
    row,node=fixture();node['codeDiscount']['customerGets']['value']['appliesOnEachItem']=True
    with pytest.raises(RedemptionError):verify_discount(row,node)


@pytest.mark.parametrize('conflicting', [False,True])
def test_concurrent_creation_recovers_only_verified_same_code(conflicting):
    from app.loyalty.redemption_shopify import CREATE
    row,node=fixture()
    if conflicting: node['codeDiscount']['usageLimit']=2
    provider=DiscountProvider(None,'test.myshopify.com','synthetic','1','2')
    calls=[]
    async def query(q,variables):
        calls.append(q)
        if q==CREATE:return {'discountCodeBasicCreate':{'userErrors':[{'code':'TAKEN'}],'codeDiscountNode':None}}
        return {'codeDiscountNodeByCode':None if len(calls)==1 else node}
    async def eligibility(ids):return json.loads(row.eligibility_json)
    provider.query=query;provider.eligibility=eligibility
    if conflicting:
        with pytest.raises(RedemptionError,match='discount_identity_conflict'):
            asyncio.run(provider.ensure_discount(row))
    else:
        assert asyncio.run(provider.ensure_discount(row))==node['id']
    assert calls==[LOOKUP,CREATE,LOOKUP]


@pytest.mark.parametrize('node,provider_id,expected',[
    ({'codeDiscount':{'title':'Degen reward 1','status':'EXPIRED','asyncUsageCount':0}},'gid://shopify/DiscountCodeNode/1',True),
    ({'codeDiscount':{'title':'Degen reward 1','status':'ACTIVE','asyncUsageCount':0}},'gid://shopify/DiscountCodeNode/1',False),
    ({'codeDiscount':{'title':'Degen reward 1','status':'EXPIRED','asyncUsageCount':1}},'gid://shopify/DiscountCodeNode/1',False),
    ({'codeDiscount':{'title':'Someone else','status':'EXPIRED','asyncUsageCount':0}},'gid://shopify/DiscountCodeNode/1',False),
    ({'codeDiscount':{'title':'Degen reward 1','status':'EXPIRED'}},'gid://shopify/DiscountCodeNode/1',False),
    (None,None,True),   # never created at Shopify
    (None,'gid://shopify/DiscountCodeNode/1',False),   # created, but no longer readable
])
def test_unused_confirmation_requires_deactivated_code_with_zero_uses(node,provider_id,expected):
    row,_=fixture();row.provider_id=provider_id
    provider=DiscountProvider(None,'test.myshopify.com','synthetic','1','2')
    async def query(q,variables):
        assert variables=={'code':row.code}
        return {'codeDiscountNodeByCode':node}
    provider.query=query
    assert asyncio.run(provider.confirmed_unused(row)) is expected
