import asyncio
from copy import deepcopy
import pytest
from app.loyalty.shopify_reader import Reader, ReadError, project_order
from test_loyalty_domain import POLICY


def bag(cents):
    amount=f'{cents//100}.{cents%100:02d}'
    return {'shopMoney':{'amount':amount,'currencyCode':'USD'},'presentmentMoney':{'amount':amount,'currencyCode':'USD'}}

def head():
    return dict(id='gid://shopify/Order/10',sourceName='pos',retailLocation={'id':'gid://shopify/Location/2'},createdAt='2026-09-11T12:00:00Z',processedAt='2026-09-11T12:00:00Z',updatedAt='2026-09-11T12:00:00Z',customer={'id':'gid://shopify/Customer/3'},test=False,edited=False,currencyCode='USD',presentmentCurrencyCode='USD',taxesIncluded=False,totalPriceSet=bag(5000),totalReceivedSet=bag(5000),totalOutstandingSet=bag(0),displayFinancialStatus="PAID",totalCashRoundingAdjustment={"paymentSet":bag(0),"refundSet":bag(0)},
        transactions=[dict(id='gid://shopify/OrderTransaction/21',kind='SALE',status='SUCCESS',amountRoundingSet=None,amountSet=bag(5000),processedAt='2026-09-11T12:00:00Z')],transactionsCount={'count':1,'precision':'EXACT'},refunds=[],returns={'nodes':[],'pageInfo':{'hasNextPage':False,'endCursor':None}})

def line(id=11):
    return dict(id=f'gid://shopify/LineItem/{id}',quantity=1,originalTotalSet=bag(5000),discountAllocations=[],taxLines=[],isGiftCard=False,requiresShipping=True,variant={'id':'gid://shopify/ProductVariant/1'})

def page(nodes,next=False,cursor=None):
    return {'nodes':nodes,'pageInfo':{'hasNextPage':next,'endCursor':cursor}}

def test_projection_exact_allocations_and_no_contacts():
    h=head(); h['customer']['email']='private@example.test'
    l=line(); l['discountAllocations']=[{'allocatedAmountSet':bag(500)}]
    result=project_order(h,[l],[],POLICY.shop_id)
    assert result['lines'][0]['discounts_cents']==[500]
    assert 'private' not in str(result) and 'email' not in str(result)

def test_pagination_and_consistency():
    calls=[]
    async def request(query,variables):
        calls.append(variables)
        if 'LoyaltyOrderHead' in query: return {'order':head(),'shop':{'id':POLICY.shop_id,'currencyCode':'USD'}}
        return {'node':{'connection':page([line(12 if variables.get('after') else 11)],not variables.get('after'),'end' if not variables.get('after') else None)}}
    r=Reader(None,POLICY.shop_domain,'synthetic',request_limit=20)
    r.request=request
    result=asyncio.run(r.order('gid://shopify/Order/10',POLICY.shop_id))
    assert len(result['lines'])==2
    assert len(calls)==4

@pytest.mark.parametrize('bad', [page([],True,''),page([],True,'again'),{'nodes':[]}])
def test_incomplete_and_repeated_pages_rejected(bad):
    async def request(q,v): return {'node':{'connection':bad}}
    r=Reader(None,POLICY.shop_domain,'synthetic');r.request=request
    with pytest.raises(ReadError): asyncio.run(r.connection('Order','gid://shopify/Order/10','lineItems','id'))

def test_partial_graphql_errors_rejected_and_throttle_retried(monkeypatch):
    from app.loyalty import shopify_reader as module
    responses=[{'errors':[{'extensions':{'code':'THROTTLED'}}]}, {'data':{'ok':True}}]
    async def transport(*a,**k): return responses.pop(0)
    async def sleep(*a): pass
    monkeypatch.setattr(module,'shopify_graphql_request',transport);monkeypatch.setattr(module.asyncio,'sleep',sleep)
    r=Reader(None,POLICY.shop_domain,'synthetic')
    assert asyncio.run(r.request('query {}',{}))=={'ok':True}
    responses.append({'data':{'ok':True},'errors':[{'message':'secret provider text'}]})
    with pytest.raises(ReadError,match='graphql_incomplete'): asyncio.run(r.request('query {}',{}))

def test_changed_during_pagination_and_transaction_truncation():
    count=0
    async def request(q,v):
        nonlocal count
        if 'LoyaltyOrderHead' in q:
            count+=1;h=head()
            if count>1:h['updatedAt']='2026-09-11T13:00:00Z'
            return {'order':h,'shop':{'id':POLICY.shop_id,'currencyCode':'USD'}}
        return {'node':{'connection':page([line()])}}
    r=Reader(None,POLICY.shop_domain,'synthetic');r.request=request
    with pytest.raises(ReadError,match='changed_during_read'):asyncio.run(r.order('gid://shopify/Order/10',POLICY.shop_id))
    h=head();h['transactionsCount']['count']=2
    with pytest.raises(ReadError,match='transactions_incomplete'):project_order(h,[line()],[],POLICY.shop_id)

def test_refund_connections_project_successful_money_and_shipping():
    from app.loyalty.domain import calculate
    from test_loyalty_domain import attachment,POLICY
    h=head();r=dict(id='gid://shopify/Refund/30',totalRefundedSet=bag(1200),duties=[],adjustments=[],
        lines=[dict(id='gid://shopify/RefundLineItem/31',lineItem={'id':'gid://shopify/LineItem/11'},quantity=0,subtotalSet=bag(1000),totalTaxSet=bag(100))],
        shipping=[dict(id='gid://shopify/RefundShippingLine/32',subtotalAmountSet=bag(100),taxAmountSet=bag(0))],
        transactions=[dict(id='gid://shopify/OrderTransaction/33',kind='REFUND',status='SUCCESS',amountRoundingSet=None,amountSet=bag(1200))])
    h['refunds']=[{'id':r['id']}];h['transactions'].append(dict(r['transactions'][0],processedAt='2026-09-11T13:00:00Z'));h['transactionsCount']['count']=2
    o=project_order(h,[line()],[r],POLICY.shop_id)
    assert calculate(o,POLICY,attachment()).points==40
    h['transactionsCount']['precision']='AT_LEAST'
    with pytest.raises(ReadError):project_order(h,[line()],[r],POLICY.shop_id)

def test_request_budget_blocks_transport_before_network(monkeypatch):
    r=Reader(None,POLICY.shop_domain,'synthetic',request_limit=0)
    with pytest.raises(ReadError,match='budget'):asyncio.run(r.request('query {}',{}))

def test_queries_are_pinned_read_only_and_contact_free():
    from app.loyalty import shopify_reader as m
    assert m.SHOPIFY_API_VERSION=='2026-04'
    for query in (m.HEAD,m.LINE,m.TRANSACTION):
        for forbidden in ('mutation','email','phone','priceAfterAllDiscountsBeforeTaxesSet','discountedTotalSet'):
            assert forbidden not in query


def test_return_exchanges_are_distinguished_and_incomplete_returns_rejected():
    h=head();h['returns']=page([{'id':'gid://shopify/Return/1'}])
    with pytest.raises(ReadError,match='returns_incomplete'):project_order(h,[line()],[],POLICY.shop_id)
    details=[{'id':'gid://shopify/Return/1','exchangeLineItems':page([])}]
    assert project_order(h,[line()],[],POLICY.shop_id,details)['exchanges'] is False
    details[0]['exchangeLineItems']=page([{'id':'gid://shopify/ExchangeLineItem/1'}])
    assert project_order(h,[line()],[],POLICY.shop_id,details)['exchanges'] is True


def test_updated_scan_is_pos_filtered():
    async def request(q,v):
        assert v['query'].startswith('source_name:pos updated_at:')
        return {'orders':page([])}
    reader=Reader(None,POLICY.shop_domain,'synthetic');reader.request=request
    assert asyncio.run(reader.updated_page('2026-09-10','2026-09-11'))==([],False,None)


def test_cash_rounding_fields_and_signed_projection():
    h=head();h['totalCashRoundingAdjustment']['paymentSet']=bag(-1)
    # Format negative exact money without Python floor-division notation.
    h['totalCashRoundingAdjustment']['paymentSet']={k:{'amount':'-0.01','currencyCode':'USD'} for k in ('shopMoney','presentmentMoney')}
    h['transactions'][0]['amountRoundingSet']=h['totalCashRoundingAdjustment']['paymentSet']
    result=project_order(h,[line()],[],POLICY.shop_id)
    assert result['payment_rounding_cents']==result['payments'][0]['rounding_cents']==-1
