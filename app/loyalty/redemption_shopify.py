"""Bounded Shopify discount transport. Never trusts POS prices or paid assertions.

Requires separately approved read_products and write_discounts. Only verified
physical catalog variants qualify. Degen custom sales are physical merchandise;
custom-sale rewards use an order-wide code and reject mixed excluded catalog items.
No live call occurs unless redemption gates pass.
"""
from datetime import timezone
import json
from .reward_policy import variant_ids
from .domain import Policy, calculate, gid, cents, full_refund
from .redemption import RedemptionError
from .service import naive, canonical_evidence
from .shopify_reader import Reader
from ..inventory.shopify import shopify_graphql_request

LOOKUP = '''query RewardCode($code:String!) {
 codeDiscountNodeByCode(code:$code) { id codeDiscount { ... on DiscountCodeBasic {
 title usageLimit appliesOncePerCustomer startsAt endsAt
 combinesWith { orderDiscounts productDiscounts shippingDiscounts }
 context { __typename ... on DiscountCustomers { customers { id } } }
 customerGets {
  appliesOnOneTimePurchase appliesOnSubscription
  value { __typename ... on DiscountAmount { amount { amount currencyCode } appliesOnEachItem } }
  items { __typename ... on AllDiscountItems { allItems }
   ... on DiscountProducts {
    products(first:1) { nodes { id } pageInfo { hasNextPage } }
    productVariants(first:100) { nodes { id } pageInfo { hasNextPage } }
   }
  }
 }
 minimumRequirement { __typename ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount currencyCode } } }
 } } }
}'''
CREATE = '''mutation RewardCodeCreate($input:DiscountCodeBasicInput!) {
 discountCodeBasicCreate(basicCodeDiscount:$input) {
  codeDiscountNode { id } userErrors { field code }
 }
}'''
DEACTIVATE = '''mutation RewardCodeDisable($id:ID!) {
 discountCodeDeactivate(id:$id) { codeDiscountNode { id } userErrors { field code } }
}'''


CATALOG = """query RewardVariants($ids:[ID!]!) {
 shop { id myshopifyDomain currencyCode taxesIncluded }
 nodes(ids:$ids) { __typename ... on ProductVariant {
  id inventoryItem { requiresShipping } product { isGiftCard }
 } }
}"""


def bound_variants(row):
    try:
        data=json.loads(row.eligibility_json)
        custom=data.get('custom_sales') is True
        if set(data) != ({'requested','eligible','custom_sales'} if custom else {'requested','eligible'}): raise ValueError()
        requested=variant_ids(data['requested'],allow_empty=custom); eligible=variant_ids(data['eligible'],allow_empty=custom)
        if custom and requested != eligible: raise ValueError()
        if not set(eligible)<=set(requested): raise ValueError()
        return eligible
    except (AttributeError, KeyError, TypeError, ValueError):
        raise RedemptionError('reward_eligibility_unverified') from None


def includes_custom_sales(row):
    bound_variants(row)
    return json.loads(row.eligibility_json).get('custom_sales') is True


def discount_items(row):
    return {'all':True} if includes_custom_sales(row) else {'products':{'productVariantsToAdd':bound_variants(row)}}


def verify_items(row,items):
    if includes_custom_sales(row):
        return items == {'__typename':'AllDiscountItems','allItems':True}
    return (items['__typename']=='DiscountProducts' and
        items['products']['nodes']==[] and items['products']['pageInfo']['hasNextPage'] is False and
        items['productVariants']['pageInfo']['hasNextPage'] is False and
        sorted(x['id'] for x in items['productVariants']['nodes'])==bound_variants(row))


def money_string(cents):
    return f'{cents//100}.{cents%100:02d}'


def discount_input(row):
    return dict(title=f'Degen reward {row.id}', code=row.code,
        startsAt=row.created_at.replace(tzinfo=timezone.utc).isoformat(),
        endsAt=row.expires_at.replace(tzinfo=timezone.utc).isoformat(),
        context={'customers': {'add': [row.customer_id]}},
        # Stores without subscriptions reject even explicit false/default flags.
        # Omit these optional inputs and verify their effective values on readback.
        customerGets={'value': {'discountAmount': {'amount': money_string(row.discount_cents), 'appliesOnEachItem': False}},
                      'items': discount_items(row)},
        minimumRequirement={'subtotal': {'greaterThanOrEqualToSubtotal': money_string(row.minimum_cents)}},
        usageLimit=1, appliesOncePerCustomer=True,
        combinesWith={'orderDiscounts':False, 'productDiscounts':False, 'shippingDiscounts':False})


def verify_discount(row,node):
    """A recovered code must retain every snapshotted restriction, not just its title."""
    try:
        d=node['codeDiscount']; gets=d['customerGets']; value=gets['value']; items=gets['items']
        context=d['context']; minimum=d['minimumRequirement']
        valid=(d['title']==f'Degen reward {row.id}' and d['usageLimit']==1 and
            d['appliesOncePerCustomer'] is True and naive(d['startsAt'])==row.created_at and
            naive(d['endsAt'])==row.expires_at and
            d['combinesWith']==dict(orderDiscounts=False,productDiscounts=False,shippingDiscounts=False) and
            context['__typename']=='DiscountCustomers' and context['customers']==[{'id':row.customer_id}] and
            gets['appliesOnOneTimePurchase'] is True and gets['appliesOnSubscription'] is False and
            value['__typename']=='DiscountAmount' and value['appliesOnEachItem'] is False and
            value['amount']['currencyCode']=='USD' and cents(value['amount']['amount'])==row.discount_cents and
            verify_items(row,items) and
            minimum['__typename']=='DiscountMinimumSubtotal' and
            minimum['greaterThanOrEqualToSubtotal']['currencyCode']=='USD' and
            cents(minimum['greaterThanOrEqualToSubtotal']['amount'])==row.minimum_cents)
        if not valid: raise ValueError()
        id=gid('DiscountCodeNode',node['id'])
        if row.provider_id and id!=row.provider_id: raise ValueError()
        return id
    except (KeyError,TypeError,ValueError):
        raise RedemptionError('discount_identity_conflict') from None


class DiscountProvider:
    def __init__(self, client, domain, token, shop_id, locations):
        self.client=client; self.domain=domain; self.token=token
        self.shop_id=gid('Shop',shop_id)
        self.locations=frozenset(gid('Location',x.strip()) for x in locations.split(',') if x.strip())
        if not self.locations: raise RedemptionError('redemption_location_unset')
        self.remaining=30

    async def query(self, query, variables):
        self.remaining-=1
        if self.remaining<0: raise RedemptionError('provider_budget_exhausted')
        result=await shopify_graphql_request(self.client,store_domain=self.domain,
            access_token=self.token,query=query,variables=variables)
        if result.get('errors') or not isinstance(result.get('data'),dict):
            raise RedemptionError('discount_provider_unavailable')
        return result['data']

    async def eligibility(self, requested, *, custom_sales=False):
        if type(custom_sales) is not bool: raise RedemptionError('invalid_variants')
        requested=variant_ids(requested,allow_empty=custom_sales)
        data=await self.query(CATALOG, {'ids':requested})
        shop=data.get('shop', {})
        if shop.get('id')!=self.shop_id or shop.get('myshopifyDomain')!=self.domain:
            raise RedemptionError('catalog_identity_mismatch')
        if shop.get('currencyCode')!='USD' or shop.get('taxesIncluded') is not False:
            raise RedemptionError('reward_pricing_unsupported')
        nodes=data.get('nodes')
        if not isinstance(nodes,list) or len(nodes)!=len(requested):
            raise RedemptionError('catalog_incomplete')
        eligible=[]; seen=set()
        for node in nodes:
            if not isinstance(node,dict) or node.get('__typename')!='ProductVariant' or node.get('id') not in requested or node['id'] in seen:
                raise RedemptionError('catalog_incomplete')
            seen.add(node['id'])
            physical=(node.get('inventoryItem') or {}).get('requiresShipping')
            gift=(node.get('product') or {}).get('isGiftCard')
            if type(physical) is not bool or type(gift) is not bool:
                raise RedemptionError('catalog_incomplete')
            if physical and not gift: eligible.append(node['id'])
        if custom_sales:
            if sorted(eligible)!=requested: raise RedemptionError('custom_sale_excluded_items')
            return {'requested':requested,'eligible':sorted(eligible),'custom_sales':True}
        if not eligible: raise RedemptionError('no_eligible_merchandise')
        return {'requested':requested,'eligible':sorted(eligible)}

    async def ensure_discount(self,row):
        if naive() >= row.expires_at: raise RedemptionError('reward_expired')
        expected=json.loads(row.eligibility_json)
        bound_variants(row)
        if await self.eligibility(expected['requested'], **({'custom_sales':True} if includes_custom_sales(row) else {})) != expected:
            raise RedemptionError('reward_eligibility_changed')
        data=await self.query(LOOKUP,{'code':row.code})
        node=data['codeDiscountNodeByCode']
        if node:
            return verify_discount(row,node)
        if getattr(self, 'issuance_check', None): await self.issuance_check()
        data=await self.query(CREATE,{'input':discount_input(row)})
        result=data['discountCodeBasicCreate']
        if result.get('userErrors') or not result.get('codeDiscountNode'):
            # The request and recovery worker may race to create the same unique
            # code. Recover only an exact matching readback, never a second code.
            recovered=(await self.query(LOOKUP,{'code':row.code}))['codeDiscountNodeByCode']
            if recovered is not None:
                return verify_discount(row,recovered)
            raise RedemptionError('discount_creation_unconfirmed')
        node=(await self.query(LOOKUP,{'code':row.code}))['codeDiscountNodeByCode']
        verified=verify_discount(row,node)
        if verified!=result['codeDiscountNode']['id']: raise RedemptionError('discount_identity_conflict')
        return verified

    async def deactivate(self,row):
        node=(await self.query(LOOKUP,{'code':row.code}))['codeDiscountNodeByCode']
        if node is None: return
        if node['codeDiscount'].get('title')!=f'Degen reward {row.id}':
            raise RedemptionError('discount_identity_conflict')
        result=(await self.query(DEACTIVATE,{'id':node['id']}))['discountCodeDeactivate']
        if result.get('userErrors') or (result.get('codeDiscountNode') or {}).get('id')!=node['id']:
            raise RedemptionError('discount_disable_unconfirmed')

    async def order_evidence(self,row):
        # Search is a discovery hint only. Canonical evidence and exact code are checked.
        if getattr(row, 'order_id', None):
            order_id = gid('Order', row.order_id)
        else:
            data=await self.query('''query RewardOrders($query:String!) {
              orders(first:2,query:$query) { nodes { id } pageInfo { hasNextPage } }
            }''',{'query':'discount_code:'+row.code})
            conn=data['orders']
            if conn['pageInfo']['hasNextPage'] or len(conn['nodes'])>1:
                raise RedemptionError('multiple_reward_orders_review')
            if not conn['nodes']: return None
            order_id=gid('Order',conn['nodes'][0]['id'])
        reader=Reader(self.client,self.domain,self.token,request_limit=100)
        order=canonical_evidence(await reader.order(order_id,self.shop_id))
        data=await self.query('''query RewardOrderDiscount($id:ID!) {
          order(id:$id) { id updatedAt lineItems(first:100) { nodes { id variant { id } } pageInfo { hasNextPage } } discountApplications(first:100) {
            nodes { __typename ... on DiscountCodeApplication { code } }
            pageInfo { hasNextPage }
          } }
        }''',{'id':order_id})
        extra=data['order'];apps=extra['discountApplications']
        if gid('Order',extra['id'])!=order_id:raise RedemptionError('order_identity_mismatch')
        if naive(extra['updatedAt']) != naive(order['updated_at']):
            raise RedemptionError('order_changed_retry')
        if apps['pageInfo']['hasNextPage'] or len(apps['nodes'])!=1 or apps['nodes'][0].get('code')!=row.code:
            raise RedemptionError('reward_discount_conflict')
        if order['customer_id']!=row.customer_id or order['test']:
            raise RedemptionError('order_identity_mismatch')
        observed_at=row.first_observed_at or naive()
        if getattr(self, 'observation', None):
            observed_at=await self.observation(dict(order_id=order_id,customer_id=order['customer_id'],code=row.code,canonical_order=order))
        eligible_lines=order['lines']
        if getattr(row,'eligibility_json','{}') != '{}':
            allowed=set(bound_variants(row)); connection=extra['lineItems']
            nodes=connection['nodes']
            if any('variant' not in n or (n['variant'] is not None and
                   (not isinstance(n['variant'],dict) or not n['variant'].get('id'))) for n in nodes):
                raise RedemptionError('order_line_scope_unverified')
            mapping={n['id']:(n.get('variant') or {}).get('id') for n in nodes}
            if (connection['pageInfo']['hasNextPage'] or len(mapping)!=len(nodes)
                or set(mapping)!={x['id'] for x in order['lines']}):
                raise RedemptionError('order_line_scope_unverified')
            custom=includes_custom_sales(row)
            eligible_lines=[x for x in order['lines'] if
                (mapping[x['id']] in allowed or (custom and mapping[x['id']] is None))
                and x['merchandise'] and not x['gift_card']]
            eligible_line_ids={x['id'] for x in eligible_lines}
            if any(sum(x['discounts_cents']) for x in order['lines'] if x['id'] not in eligible_line_ids):
                raise RedemptionError('ineligible_discount_review')
            if order['taxes_included']: raise RedemptionError('reward_pricing_unsupported')
        if any(sum(x['discounts_cents']) for x in order['lines'] if x['gift_card']):
            raise RedemptionError('gift_card_discount_review')
        policy=Policy(row.shop,self.shop_id,self.locations,row.created_at.replace(tzinfo=timezone.utc))
        attachment={'kind':'canonical_observation','customer_id':row.customer_id,
                    'at':observed_at.replace(tzinfo=timezone.utc).isoformat()}
        if getattr(row, 'order_id', None) and row.order_id != order_id:
            raise RedemptionError('order_conflict')
        try: result=calculate(order,policy,attachment)
        except ValueError: raise RedemptionError('order_payment_or_refund_review') from None
        if result.state!='eligible': raise RedemptionError('order_payment_or_refund_review')
        discounts=sum(sum(x['discounts_cents']) for x in order['lines'])
        eligible=sum(x['original_cents']-(x['tax_cents'] if order['taxes_included'] else 0)
                     for x in eligible_lines if x['merchandise'] and not x['gift_card'])
        return dict(order_id=order_id,customer_id=order['customer_id'],code=row.code,
            discount_cents=discounts,eligible_cents=eligible,currency=order['currency'],
            stacked=False,test=False,paid=True,full_refund=full_refund(order),
            partial_refund=bool(order['refunds']) and not full_refund(order),
            basis_hash=result.basis_hash, refunds=order['refunds'], canonical_order=order,
            observed_at=observed_at.replace(tzinfo=timezone.utc).isoformat(),updated_at=order['updated_at'])
