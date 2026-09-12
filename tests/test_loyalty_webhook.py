import base64
import hashlib
import hmac
import json
from types import SimpleNamespace
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session, select, create_engine
from app.loyalty.models import LoyaltyInbox
from app.loyalty.schema import install_schema
from app.loyalty.service import Store
from app.routers import shopify_loyalty_webhooks as module

SECRET='synthetic-webhook-secret'
@pytest.fixture
def client(tmp_path,monkeypatch):
    engine=create_engine('sqlite:///'+str(tmp_path/'hook.db'),connect_args={'check_same_thread':False})
    install_schema(engine)
    settings=SimpleNamespace(loyalty_receiving_enabled=True,loyalty_shop_domain='test.myshopify.com',shopify_store_domain='test.myshopify.com',loyalty_shop_id='gid://shopify/Shop/1',shopify_webhook_secret=SECRET,loyalty_receipt_retention_days=30,loyalty_body_limit_bytes=1024)
    monkeypatch.setattr(module,'settings',settings);monkeypatch.setattr(module,'store',Store(engine))
    app=FastAPI();app.include_router(module.router)
    with TestClient(app) as c: yield c,engine,settings

def send(client,payload=None,**headers):
    payload=payload if payload is not None else {'id':10,'customer':{'id':3,'email':'private@example.test','phone':'555'},'created_at':'2026-09-11T12:00:00Z','updated_at':'2026-09-11T12:00:00Z'}
    body=json.dumps(payload).encode()
    h={'x-shopify-shop-domain':'test.myshopify.com','x-shopify-topic':'orders/create','x-shopify-webhook-id':'delivery-1','x-shopify-event-id':'event-1','x-shopify-hmac-sha256':base64.b64encode(hmac.new(SECRET.encode(),body,hashlib.sha256).digest()).decode(),'content-type':'application/json'}
    h.update(headers)
    return client.post('/webhooks/shopify/loyalty',content=body,headers=h)

def test_committed_minimized_receipt_deduplication(client):
    c,engine,_=client
    assert send(c).status_code==200
    assert send(c).status_code==200
    with Session(engine) as s:
        row=s.exec(select(LoyaltyInbox)).one()
        assert row.order_id=='gid://shopify/Order/10'
        assert 'private' not in row.evidence_json and 'phone' not in row.evidence_json
        assert 'attachment' not in json.loads(row.evidence_json)

@pytest.mark.parametrize('headers,code',[({'x-shopify-hmac-sha256':'bad'},401),({'x-shopify-shop-domain':'evil.myshopify.com'},403),({'x-shopify-topic':'customers/update'},400),({'x-shopify-webhook-id':''},400)])
def test_header_bounds(client,headers,code):
    c,engine,_=client
    assert send(c,**headers).status_code==code
    with Session(engine) as s:assert not s.exec(select(LoyaltyInbox)).all()

def test_body_bounds_disabled_and_schema_failure(client,monkeypatch):
    c,engine,settings=client
    assert send(c,{'id':10,'padding':'x'*2000}).status_code==413
    settings.loyalty_receiving_enabled=False
    assert send(c).status_code==503
    settings.loyalty_receiving_enabled=True
    monkeypatch.setattr(module,'schema_ready',lambda e:False)
    assert send(c).status_code==503

def test_commit_failure_never_acknowledged(client,monkeypatch):
    c,_,_=client
    def broken(*a,**k):raise RuntimeError('database down secret text')
    monkeypatch.setattr(module.store,'receive',broken)
    response=send(c)
    assert response.status_code==503 and 'secret' not in response.text

def test_updated_and_delayed_payment_do_not_invent_attachment_timestamp(client):
    c,engine,_=client
    assert send(c,**{'x-shopify-topic':'orders/updated'}).status_code==200
    with Session(engine) as s: assert 'attachment' not in json.loads(s.exec(select(LoyaltyInbox)).one().evidence_json)

def test_merge_receipt_holds_ids_without_contacts(client):
    c,engine,_=client
    p={'admin_graphql_api_customer_kept_id':'gid://shopify/Customer/3','admin_graphql_api_customer_deleted_id':'gid://shopify/Customer/4','status':'failed','errors':[{'message':'private name'}]}
    assert send(c,p,**{'x-shopify-topic':'customers/merge'}).status_code==200
    with Session(engine) as s:
        row=s.exec(select(LoyaltyInbox)).one()
        assert row.status=='review' and 'private name' not in row.evidence_json

def test_raw_body_tampering_and_missing_order_rejected(client):
    c,_,_=client
    response=c.post('/webhooks/shopify/loyalty',content=b'{"id":11}',headers={'x-shopify-shop-domain':'test.myshopify.com','x-shopify-topic':'orders/updated','x-shopify-webhook-id':'tamper','x-shopify-hmac-sha256':base64.b64encode(hmac.new(SECRET.encode(),b'{"id":10}',hashlib.sha256).digest()).decode()})
    assert response.status_code==401
    assert send(c,{'id':True}).status_code==400

def test_paid_unsigned_header_is_never_attachment_proof(client):
    c,engine,_=client
    assert send(c,**{'x-shopify-topic':'orders/paid','x-shopify-triggered-at':'2026-09-11T12:01:00Z'}).status_code==200
    with Session(engine) as s:
        evidence=json.loads(s.exec(select(LoyaltyInbox)).one().evidence_json)
        assert 'attachment' not in evidence
        assert evidence['event_id']=='event-1'
