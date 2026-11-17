from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.routers.loyalty_redemption import build_router
from test_loyalty_pos import configuration,token,add_entry,SHOP
from test_loyalty_service import store

PATH='/api/loyalty/pos/customers/101/rewards'

def client(store,enabled=True):
    settings=configuration(loyalty_redemption_enabled=enabled,loyalty_redemption_all_staff_enabled=True,
        loyalty_redemption_provider_verified=True,
        loyalty_location_ids='2', loyalty_launch_at='2026-01-01T00:00:00Z',
        loyalty_processing_enabled=True, loyalty_posting_enabled=True, loyalty_read_access_verified=True,
        loyalty_budget_cents=0, loyalty_exception_owner='test-owner', loyalty_evidence_retention_days=30,
        loyalty_ledger_retention_days=30, loyalty_backup_retention_days=30)
    class Provider:
        async def eligibility(self, ids):return {'requested':ids,'eligible':ids}
        async def ensure_discount(self,row):return 'gid://shopify/DiscountCodeNode/1'
        async def order_evidence(self,row):return None
        async def deactivate(self,row):pass
    app=FastAPI();app.include_router(build_router(store.engine,settings,lambda _:Provider()))
    return TestClient(app)

def test_disabled_has_no_side_effects(store):
    c=client(store,False)
    assert c.post(PATH,json={'action':'reserve','request_key':'test_request_0001','variant_ids':['gid://shopify/ProductVariant/11']}).status_code==401
    assert c.post(PATH,headers={'Authorization':'Bearer '+token()},
        json={'action':'reserve','request_key':'test_request_0001','variant_ids':['gid://shopify/ProductVariant/11']}).status_code==503

def test_auth_scope_body_and_idempotency(store):
    add_entry(store.engine,1000)
    c=client(store);headers={'Authorization':'Bearer '+token()}
    assert c.get(PATH).status_code==401
    assert c.get(PATH,headers=dict(Authorization='Bearer '+token(aud='other'))).status_code==401
    data={'action':'reserve','request_key':'test_request_0001','variant_ids':['gid://shopify/ProductVariant/11']}
    a=c.post(PATH,headers=headers,json=data);assert a.status_code==200,a.text
    b=c.post(PATH,headers=headers,json=data);assert a.json()==b.json()
    result=c.get(PATH,headers=headers).json()
    assert result['available_points']==500 and result['reserved_points']==500
    assert result['pending']['redemption_id']==a.json()['redemption_id']
    other=c.get(PATH,headers={'Authorization':'Bearer '+token(sid='other-session')}).json()
    assert other['pending'] is None
    assert c.post(PATH,headers=headers,json=dict(data,points=1)).status_code==400
    assert c.post(PATH,headers=headers,content='x'*1025).status_code==400
    assert c.post(PATH,headers=dict(headers,Origin='https://evil.example'),json=data).status_code==403
    assert c.post(PATH.replace('101','102'),headers=headers,json={'action':'refresh','redemption_id':a.json()['redemption_id']}).status_code==409

def test_cors_allows_only_shopify_post_and_never_cookies(store):
    c=client(store)
    result=c.options(PATH,headers={'Origin':'https://cdn.shopify.com',
        'Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'authorization,content-type'})
    assert result.status_code==204
    assert 'POST' in result.headers['Access-Control-Allow-Methods']
    assert 'access-control-allow-credentials' not in result.headers

def test_missing_provider_token_blocks_before_reservation(store,monkeypatch):
    from app.routers import loyalty_redemption as module
    from app.loyalty.redemption import Redemptions
    add_entry(store.engine,1000)
    settings=configuration(loyalty_redemption_enabled=True,loyalty_redemption_all_staff_enabled=True,
        loyalty_redemption_provider_verified=True,loyalty_location_ids='2')
    monkeypatch.setattr(module,'resolve_shopify_access_token',lambda _:'')
    app=FastAPI();app.include_router(build_router(store.engine,settings))
    c=TestClient(app)
    assert c.post(PATH,headers={'Authorization':'Bearer '+token()},
        json={'action':'reserve','request_key':'test_request_0001','variant_ids':['gid://shopify/ProductVariant/11']}).status_code==503
    assert Redemptions(store).overview(SHOP,'101')['reserved_points']==0


def test_pause_blocks_new_issuance_but_allows_existing_status_and_cancel(store):
    add_entry(store.engine,1000)
    headers={'Authorization':'Bearer '+token()}
    live=client(store)
    row=live.post(PATH,headers=headers,json={'action':'reserve','request_key':'pause_request_001','variant_ids':['gid://shopify/ProductVariant/11']}).json()
    paused=client(store,False)
    state=paused.get(PATH,headers=headers)
    assert state.status_code==200 and state.json()['issuance_enabled'] is False
    assert state.json()['pending']['redemption_id']==row['redemption_id']
    assert paused.post(PATH,headers=headers,json={'action':'reserve','request_key':'pause_request_002','variant_ids':['gid://shopify/ProductVariant/11']}).status_code==503
    checked=paused.post(PATH,headers=headers,json={'action':'refresh','redemption_id':row['redemption_id']})
    assert checked.status_code==200 and checked.json()['state']=='review'
    assert paused.get(PATH,headers=headers).json()['reserved_points']==500


def test_malformed_json_is_a_client_error(store):
    c=client(store)
    response=c.post(PATH,headers={'Authorization':'Bearer '+token(),'Content-Type':'application/json'},content='{')
    assert response.status_code==400 and response.json()['error']=='invalid_request'


def test_register_cancel_frees_points_when_provider_confirms_unused(store):
    add_entry(store.engine,1000)
    c=client(store);headers={'Authorization':'Bearer '+token()}
    reserve={'action':'reserve','request_key':'cancel_request_001','variant_ids':['gid://shopify/ProductVariant/11']}
    row=c.post(PATH,headers=headers,json=reserve).json()
    # Without positive confirmation the hold stays for review, as before.
    held=c.post(PATH,headers=headers,json={'action':'cancel','redemption_id':row['redemption_id']})
    assert held.status_code==200 and held.json()['state']=='review'
    assert c.get(PATH,headers=headers).json()['reserved_points']==500

def test_register_cancel_with_confirmation_allows_a_new_reward(store):
    add_entry(store.engine,1000)
    settings=client.__globals__['configuration'](loyalty_redemption_enabled=True,loyalty_redemption_all_staff_enabled=True,
        loyalty_redemption_provider_verified=True,
        loyalty_location_ids='2', loyalty_launch_at='2026-01-01T00:00:00Z',
        loyalty_processing_enabled=True, loyalty_posting_enabled=True, loyalty_read_access_verified=True,
        loyalty_budget_cents=0, loyalty_exception_owner='test-owner', loyalty_evidence_retention_days=30,
        loyalty_ledger_retention_days=30, loyalty_backup_retention_days=30)
    class Provider:
        async def eligibility(self, ids):return {'requested':ids,'eligible':ids}
        async def ensure_discount(self,row):return 'gid://shopify/DiscountCodeNode/'+str(row.id)
        async def order_evidence(self,row):return None
        async def deactivate(self,row):pass
        async def confirmed_unused(self,row):return True
    app=FastAPI();app.include_router(build_router(store.engine,settings,lambda _:Provider()))
    c=TestClient(app);headers={'Authorization':'Bearer '+token()}
    reserve={'action':'reserve','request_key':'cancel_request_001','variant_ids':['gid://shopify/ProductVariant/11']}
    row=c.post(PATH,headers=headers,json=reserve).json()
    done=c.post(PATH,headers=headers,json={'action':'cancel','redemption_id':row['redemption_id']})
    assert done.status_code==200,done.text
    assert done.json()['state']=='cancelled' and done.json()['can_cancel'] is False and done.json()['code'] is None
    state=c.get(PATH,headers=headers).json()
    assert state['reserved_points']==0 and state['available_points']==1000 and state['pending'] is None
    again=c.post(PATH,headers=headers,json=dict(reserve,request_key='cancel_request_002'))
    assert again.status_code==200 and again.json()['redemption_id']!=row['redemption_id']
