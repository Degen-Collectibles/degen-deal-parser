import time
import pytest
import jwt
from fastapi.testclient import TestClient
from pydantic import SecretStr
from scripts.loyalty_checkout_native import create_app,seed,DEV_SHOP,DEV_CLIENT,CUSTOMER
from test_loyalty_service import store
SECRET='synthetic-checkout-key-'+'x'*40

def test_native_checkout_auth_customer_bounds_and_restart_balance(store):
    seed(store.engine);seed(store.engine)
    class Provider:
        async def eligibility(self, ids):return {'requested':ids,'eligible':ids}
        async def ensure_discount(self,row):return 'gid://shopify/DiscountCodeNode/1'
        async def order_evidence(self,row):return None
    app=create_app(store.engine,SecretStr(SECRET),'checkout.example.test',time.time()+60,lambda _:Provider())
    now=int(time.time())
    token=jwt.encode(dict(iss=f'https://{DEV_SHOP}/admin',dest=f'https://{DEV_SHOP}',aud=DEV_CLIENT,
        sub='42',sid='dev-test',iat=now,nbf=now,exp=now+60),SECRET,algorithm='HS256')
    headers={'Authorization':'Bearer '+token};base='/api/loyalty/pos/customers/'+CUSTOMER
    with TestClient(app) as client:
        assert client.get('/health').json()['earning_enabled'] is True
        assert client.get('/admin').status_code==404
        assert client.get(base.replace(CUSTOMER,'101'),headers=headers).status_code==404
        assert client.get(base).status_code==401
        assert client.get(base,headers=headers).json()['balance_points']=='500'
        assert client.get(base,headers=dict(headers,Host='evil.example')).status_code==403
        result=client.post(base+'/rewards',headers=headers,json={'action':'reserve','request_key':'native_checkout_001','variant_ids':['gid://shopify/ProductVariant/11']})
        assert result.status_code==200,result.text
        assert result.json()['state']=='ready'
        seed(store.engine)
        status=client.get(base+'/rewards',headers=headers).json()
        assert status['available_points']==0 and status['reserved_points']==500

def test_expired_preview_rejects_every_request(store):
    app=create_app(store.engine,SecretStr(SECRET),'checkout.example.test',time.time()-1,lambda _:None)
    with TestClient(app) as client:assert client.get('/health').status_code==503


@pytest.mark.parametrize('partial_first',[False,True])
@pytest.mark.parametrize('custom',[False,True])
def test_native_harness_automatically_earns_and_restores_after_partial_then_full(store,partial_first,custom):
    """Exercise HTTP redemption plus the real earning worker, without ledger repair."""
    from copy import deepcopy
    from datetime import datetime, timezone, timedelta
    from sqlmodel import Session, select
    from app.loyalty.domain import Policy, calculate, full_refund
    from app.loyalty.service import canonical_evidence
    from app.loyalty.models import LoyaltyLedger
    from scripts.loyalty_checkout_native import SHOP_ID, LOCATION
    from test_loyalty_domain import order, refund

    seed(store.engine)
    stamp=datetime.now(timezone.utc)-timedelta(seconds=100)
    class Provider:
        sale=None
        async def eligibility(self,ids,**kwargs):return {'requested':ids,'eligible':ids,**kwargs}
        async def ensure_discount(self,row):return 'gid://shopify/DiscountCodeNode/1'
        async def order_evidence(self,row):
            if self.sale is None:return None
            sale=deepcopy(self.sale)
            policy=Policy(DEV_SHOP,SHOP_ID,frozenset({'gid://shopify/Location/'+LOCATION}),
                          datetime(2026,9,1,tzinfo=timezone.utc))
            attached={'kind':'canonical_observation','customer_id':sale['customer_id'],'at':stamp.isoformat()}
            result=calculate(sale,policy,attached)
            return dict(order_id=sale['id'],customer_id=sale['customer_id'],code=row.code,
                discount_cents=1000,eligible_cents=10000,currency='USD',stacked=False,test=False,paid=True,
                full_refund=full_refund(sale),partial_refund=bool(sale['refunds']) and not full_refund(sale),
                basis_hash=result.basis_hash,refunds=sale['refunds'],canonical_order=canonical_evidence(sale),
                observed_at=stamp.isoformat(),updated_at=sale['updated_at'])
    provider=Provider()
    app=create_app(store.engine,SecretStr(SECRET),'checkout.example.test',time.time()+60,lambda _:provider)
    now=int(time.time())
    token=jwt.encode(dict(iss=f'https://{DEV_SHOP}/admin',dest=f'https://{DEV_SHOP}',aud=DEV_CLIENT,
        sub='42',sid='dev-test',iat=now,nbf=now,exp=now+60),SECRET,algorithm='HS256')
    headers={'Authorization':'Bearer '+token};base='/api/loyalty/pos/customers/'+CUSTOMER
    with TestClient(app) as client:
        assert client.get(base,headers=headers).json()['balance_points']=='500'
        row=client.post(base+'/rewards',headers=headers,json={'action':'reserve',
            'request_key':'integrated_checkout_001',
            'variant_ids':[] if custom else ['gid://shopify/ProductVariant/11'],
            **({'custom_sales':True} if custom else {})}).json()
        assert row['state']=='ready'
        sale=order(9000,shop_id=SHOP_ID,customer_id='gid://shopify/Customer/'+CUSTOMER,
            location_id='gid://shopify/Location/'+LOCATION,created_at=stamp.isoformat(),
            processed_at=stamp.isoformat(),updated_at=stamp.isoformat())
        sale['payments'][0]['processed_at']=stamp.isoformat()
        sale['lines'][0].update(original_cents=10000,discounts_cents=[1000])
        provider.sale=sale
        def refresh():
            response=client.post(base+'/rewards',headers=headers,
                json={'action':'refresh','redemption_id':row['redemption_id']})
            assert response.status_code==200,response.text
            return response.json()['state']
        for _ in range(2):
            assert refresh()=='committed'
            assert client.get(base,headers=headers).json()['balance_points']=='90'
        if partial_first:
            sale['financial_status']='PARTIALLY_REFUNDED'
            sale['updated_at']=(stamp+timedelta(seconds=10)).isoformat()
            sale['refunds']=[refund(1000)]
            assert refresh()=='review'
            assert client.get(base,headers=headers).json()['balance_points']=='80'
        sale.update(financial_status='REFUNDED',edited=True,
                    updated_at=(stamp+timedelta(seconds=20)).isoformat())
        sale['refunds'].append(refund(8000 if partial_first else 9000,31))
        sale['refunds'][-1]['lines'][0]['quantity']=1
        for _ in range(3):
            assert refresh()=='restored'
            assert client.get(base,headers=headers).json()['balance_points']=='500'
        assert store.rebuild_check(DEV_SHOP)==[]
        with Session(store.engine) as session:
            entries=session.exec(select(LoyaltyLedger)).all()
            assert sum(e.reason=='reward_redeemed' for e in entries)==1
            assert sum(e.reason=='reward_restored' for e in entries)==1

@pytest.mark.parametrize('app_key,secret',[('wrong-app','x'*40),
    (DEV_CLIENT,'too-short'),(DEV_CLIENT,'x'*40+'\n')])
def test_native_launch_rejects_wrong_app_or_malformed_cli_secret(monkeypatch,app_key,secret):
    import asyncio
    from scripts.loyalty_checkout_native import serve
    monkeypatch.setenv('SHOPIFY_API_KEY',app_key)
    monkeypatch.setenv('SHOPIFY_API_SECRET',secret)
    monkeypatch.setenv('PORT','8769')
    monkeypatch.setenv('APP_URL','https://dev-preview.example.test')
    with pytest.raises(ValueError,match='Exact DEV CLI context required|Invalid DEV CLI secret'):
        asyncio.run(serve())
