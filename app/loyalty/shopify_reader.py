"""Read-only Shopify Admin 2026-04 adapter using the existing shared transport.

No contact fields. Connections are exhausted or rejected; an order head is read
again after its children to detect concurrent changes. Bound work per cycle.
"""
import asyncio
import httpx
from .domain import Review, cents, fingerprint, gid, whole, MAX_CENTS
from decimal import Decimal
from ..inventory.shopify import shopify_graphql_request
from ..shopify_api import SHOPIFY_API_VERSION

MONEY = '{ shopMoney { amount currencyCode } presentmentMoney { amount currencyCode } }'
TRANSACTION = 'id kind status processedAt amountSet ' + MONEY + ' amountRoundingSet ' + MONEY
LINE = ('id quantity isGiftCard requiresShipping variant { id } originalTotalSet ' + MONEY +
        ' discountAllocations { allocatedAmountSet ' + MONEY + ' } taxLines { priceSet '+MONEY+' }')
HEAD = '''query LoyaltyOrderHead($id: ID!) {
 shop { id currencyCode }
 order(id: $id) {
  id sourceName retailLocation { id } createdAt processedAt updatedAt
  customer { id } test edited currencyCode presentmentCurrencyCode taxesIncluded
  totalPriceSet MONEY totalReceivedSet MONEY totalOutstandingSet MONEY displayFinancialStatus
  totalCashRoundingAdjustment { paymentSet MONEY refundSet MONEY }
  transactions(first: 250) { TRANSACTION }
  transactionsCount { count precision }
  refunds { id updatedAt }
  returns(first: 1) { nodes { id } pageInfo { hasNextPage endCursor } }
 }
}'''.replace('TRANSACTION',TRANSACTION).replace('MONEY',MONEY)


class ReadError(Review):
    pass


def money(bag, *, signed=False):
    try:
        shop,presentment=bag['shopMoney'],bag['presentmentMoney']
        if shop['currencyCode']!='USD' or presentment['currencyCode']!='USD':
            raise ReadError('unsupported_currency')
        def exact(value):
            if signed and isinstance(value,str) and value.startswith('-'):
                return -cents(value[1:])
            return cents(value)
        value=exact(shop['amount'])
        if exact(presentment['amount'])!=value:
            raise ReadError('mixed_money_evidence')
        return value
    except (KeyError,TypeError):
        raise ReadError('money_incomplete') from None


def nodes_page(value):
    try:
        nodes=value['nodes'];info=value['pageInfo'];more=info['hasNextPage'];cursor=info['endCursor']
        if not isinstance(nodes,list) or type(more) is not bool or any(not isinstance(n,dict) for n in nodes):
            raise ReadError('connection_incomplete')
        if more and (not nodes or not isinstance(cursor,str) or not cursor.strip()):
            raise ReadError('connection_cursor_invalid')
        return nodes,more,cursor
    except (KeyError,TypeError):
        raise ReadError('connection_incomplete') from None


class Reader:
    def __init__(self,client,domain,token,*,request_limit=100):
        self.client=client;self.domain=domain;self.token=token
        self.remaining=request_limit
        self.delay=0

    async def request(self,query,variables):
        if SHOPIFY_API_VERSION!='2026-04':
            raise ReadError('api_version_unvalidated')
        for attempt in range(4):
            if self.remaining<=0:
                raise ReadError('api_budget_exhausted')
            self.remaining-=1
            if self.delay:
                await asyncio.sleep(min(self.delay,5));self.delay=0
            try:
                payload=await shopify_graphql_request(self.client,store_domain=self.domain,access_token=self.token,query=query,variables=variables)
            except httpx.HTTPStatusError as exc:
                status=exc.response.status_code
                if status==429 or status>=500:
                    if attempt<3:
                        try: delay=float(exc.response.headers.get('Retry-After',2**attempt))
                        except ValueError: delay=2**attempt
                        await asyncio.sleep(max(0,min(delay,10)));continue
                raise ReadError('shopify_http_'+str(status)) from None
            except (httpx.RequestError,ValueError):
                if attempt<3:
                    await asyncio.sleep(2**attempt);continue
                raise ReadError('shopify_transport_failed') from None
            if not isinstance(payload,dict):
                raise ReadError('graphql_incomplete')
            errors=payload.get('errors')
            if errors:
                if isinstance(errors,list) and all(isinstance(e,dict) and isinstance(e.get('extensions'),dict) and e['extensions'].get('code')=='THROTTLED' for e in errors) and attempt<3:
                    await asyncio.sleep(2**attempt);continue
                raise ReadError('graphql_incomplete')
            data=payload.get('data')
            if not isinstance(data,dict):
                raise ReadError('graphql_incomplete')
            cost=(payload.get('extensions') or {}).get('cost') or {}
            throttle=cost.get('throttleStatus') or {}
            try:
                requested=float(cost.get('requestedQueryCost',0));available=float(throttle.get('currentlyAvailable',requested));rate=float(throttle.get('restoreRate',1))
                if requested>available and rate>0: self.delay=(requested-available)/rate
            except (TypeError,ValueError):
                raise ReadError('throttle_evidence_invalid') from None
            return data
        raise ReadError('shopify_throttled')

    async def connection(self,resource,id,field,selection):
        # All query structure is internal constants, never webhook input.
        allowed={'Order':{'lineItems','returns'},'Refund':{'refundLineItems','refundShippingLines','transactions','orderAdjustments'}}
        if field not in allowed.get(resource,set()):
            raise ReadError('invalid_connection')
        query=f'''query LoyaltyConnection($id: ID!, $after: String) {{ node(id: $id) {{ ... on {resource} {{ connection: {field}(first: 50, after: $after) {{ nodes {{ {selection} }} pageInfo {{ hasNextPage endCursor }} }} }} }} }}'''
        cursor=None;seen_cursors=set();seen_ids=set();result=[]
        for _ in range(50):
            data=await self.request(query,{'id':id,'after':cursor})
            try: nodes,more,next_cursor=nodes_page(data['node']['connection'])
            except (KeyError,TypeError): raise ReadError('connection_incomplete') from None
            for node in nodes:
                nid=node.get('id')
                if not nid or nid in seen_ids:
                    raise ReadError('duplicate_or_missing_node')
                seen_ids.add(nid);result.append(node)
            if not more: return result
            if next_cursor in seen_cursors:
                raise ReadError('connection_cursor_repeated')
            seen_cursors.add(next_cursor);cursor=next_cursor
        raise ReadError('connection_page_limit')

    async def order(self,order_id,shop_id):
        order_id=gid('Order',order_id)
        first=await self.request(HEAD,{'id':order_id})
        try:
            if first['shop']['id']!=shop_id or first['shop']['currencyCode']!='USD':
                raise ReadError('shop_identity_or_currency_mismatch')
            head=first['order']
            if not isinstance(head,dict) or head['id']!=order_id:
                raise ReadError('order_unavailable_access_horizon')
            lines=await self.connection('Order',order_id,'lineItems',LINE)
            return_nodes, more, _ = nodes_page(head['returns'])
            returns = []
            if return_nodes or more:
                returns = await self.connection('Order',order_id,'returns',
                    'id exchangeLineItems(first: 1) { nodes { id } pageInfo { hasNextPage endCursor } }')
            refunds=[]
            for ref in head['refunds']:
                rid=gid('Refund',ref['id'])
                data=await self.request('query LoyaltyRefund($id: ID!) { node(id: $id) { ... on Refund { id updatedAt totalRefundedSet '+MONEY+' duties { __typename } } } }',{'id':rid})
                r=data['node']
                if not isinstance(r,dict) or r.get('id')!=rid or r.get('updatedAt')!=ref['updatedAt']:
                    raise ReadError('changed_during_read')
                r['lines']=await self.connection('Refund',rid,'refundLineItems','id quantity lineItem { id } subtotalSet '+MONEY+' totalTaxSet '+MONEY)
                r['shipping']=await self.connection('Refund',rid,'refundShippingLines','id subtotalAmountSet '+MONEY+' taxAmountSet '+MONEY)
                r['transactions']=await self.connection('Refund',rid,'transactions',TRANSACTION)
                r['adjustments']=await self.connection('Refund',rid,'orderAdjustments','id')
                refunds.append(r)
            last=await self.request(HEAD,{'id':order_id})
            if fingerprint(first)!=fingerprint(last):
                raise ReadError('changed_during_read')
            return project_order(head,lines,refunds,shop_id,returns)
        except (KeyError,TypeError,ValueError) as exc:
            if isinstance(exc,Review): raise
            raise ReadError('canonical_incomplete') from None

    async def updated_page(self,start,end,cursor=None):
        query='''query LoyaltyUpdatedOrders($query: String!, $after: String) {
          orders(first: 50, after: $after, sortKey: UPDATED_AT, query: $query) {
           nodes { id updatedAt } pageInfo { hasNextPage endCursor }
          }
        }'''
        # Dates originate from persisted UTC scan boundaries; no user query text.
        data=await self.request(query,{'query':f"source_name:pos updated_at:>='{start}' updated_at:<'{end}'",'after':cursor})
        try: return nodes_page(data['orders'])
        except KeyError: raise ReadError('orders_page_incomplete') from None


def project_order(h,lines,refunds,shop_id,return_details=None):
    try:
        txs=h['transactions'];count=h['transactionsCount']
        if count['precision']!='EXACT' or whole(count['count'])!=len(txs):
            raise ReadError('transactions_incomplete')
        returns,more,_=nodes_page(h['returns'])
        if (returns or more) and not return_details:
            raise ReadError('returns_incomplete')
        exchanges=False
        for ret in return_details or []:
            gid('Return',ret['id'])
            nodes,has_more,_=nodes_page(ret['exchangeLineItems'])
            exchanges = exchanges or bool(nodes or has_more)
        if h['currencyCode']!='USD' or h['presentmentCurrencyCode']!='USD':
            raise ReadError('unsupported_currency')
        result=dict(id=gid('Order',h['id']),shop_id=shop_id,source=h['sourceName'],
            location_id=gid('Location',h['retailLocation']['id']) if h['retailLocation'] else None,
            customer_id=gid('Customer',h['customer']['id']) if h['customer'] else None,
            created_at=h['createdAt'],processed_at=h['processedAt'],updated_at=h['updatedAt'],
            test=h['test'],edited=h['edited'],returns=bool(returns or more),exchanges=exchanges,currency=h['currencyCode'],
            taxes_included=h['taxesIncluded'],total_cents=money(h['totalPriceSet']),received_cents=money(h['totalReceivedSet']),
            payment_rounding_cents=money(h['totalCashRoundingAdjustment']['paymentSet'],signed=True),
            refund_rounding_cents=money(h['totalCashRoundingAdjustment']['refundSet'],signed=True),
            financial_status=h['displayFinancialStatus'],outstanding_cents=money(h['totalOutstandingSet']),
            lines=[],payments=[],refunds=[])
        for l in lines:
            if type(l['requiresShipping']) is not bool: raise ReadError('line_classification_unverified')
            result['lines'].append(dict(id=gid('LineItem',l['id']),quantity=whole(l['quantity']),
                original_cents=money(l['originalTotalSet']),discounts_cents=[money(d['allocatedAmountSet']) for d in l['discountAllocations']],
                tax_cents=sum(money(t['priceSet']) for t in l['taxLines']),gift_card=l['isGiftCard'],
                merchandise=l['requiresShipping'] and bool(l['variant'] and gid('ProductVariant',l['variant']['id']))))
        for tx in txs:
            result['payments'].append(dict(id=gid('OrderTransaction',tx['id']),kind=tx['kind'],status=tx['status'],cents=money(tx['amountSet']),rounding_cents=money(tx['amountRoundingSet'],signed=True) if tx['amountRoundingSet'] else 0,processed_at=tx['processedAt']))
        refund_tx_ids=set()
        if len(refunds)!=len(h['refunds']) or {r['id'] for r in refunds}!={r['id'] for r in h['refunds']}:
            raise ReadError('refund_history_incomplete')
        for r in refunds:
            parts=[dict(id=gid('RefundLineItem',p['id']),line_id=gid('LineItem',p['lineItem']['id']),quantity=whole(p['quantity']),subtotal_cents=money(p['subtotalSet']),tax_cents=money(p['totalTaxSet'])) for p in r['lines']]
            transactions=[dict(id=gid('OrderTransaction',t['id']),kind=t['kind'],status=t['status'],cents=money(t['amountSet']),rounding_cents=money(t['amountRoundingSet'],signed=True) if t['amountRoundingSet'] else 0) for t in r['transactions']]
            if sum(t['cents'] for t in transactions) not in (money(r['totalRefundedSet']), money(r['totalRefundedSet']) + sum(t['rounding_cents'] for t in transactions)):
                raise ReadError('refund_total_mismatch')
            refund_tx_ids.update(t['id'] for t in transactions)
            result['refunds'].append(dict(id=gid('Refund',r['id']),lines=parts,transactions=transactions,
                shipping_cents=sum(money(v['subtotalAmountSet']) for v in r['shipping']),shipping_tax_cents=sum(money(v['taxAmountSet']) for v in r['shipping']),
                adjustments=bool(r['adjustments']),duties=bool(r['duties'])))
        if refund_tx_ids!={t['id'] for t in result['payments'] if t['kind']=='REFUND'}:
            raise ReadError('refund_transactions_incomplete')
        if sum(t['rounding_cents'] for r in result['refunds'] for t in r['transactions'] if t['status']=='SUCCESS') != result['refund_rounding_cents']:
            raise ReadError('refund_rounding_mismatch')
        return result
    except (KeyError,TypeError,ValueError) as exc:
        if isinstance(exc,Review): raise
        raise ReadError('canonical_incomplete') from None
