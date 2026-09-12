from contextlib import contextmanager
from datetime import timedelta
import json
from types import SimpleNamespace
import pytest
from fastapi import FastAPI,Request
from fastapi.testclient import TestClient
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, SQLModel, select, create_engine
from app.models import User,RolePermission,AuditLog
from app.loyalty.models import LoyaltyLedger,LoyaltyEntitlement,LoyaltyInbox
from app.loyalty.schema import install_schema
from app.loyalty.service import Store
from app.routers import loyalty as module
from test_loyalty_domain import order,attachment,POLICY,SALE
from test_loyalty_service import receipt,claim,finish

@pytest.fixture
def ops(tmp_path,monkeypatch):
    engine=create_engine('sqlite:///'+str(tmp_path/'ops.db'),connect_args={'check_same_thread':False})
    SQLModel.metadata.create_all(engine,tables=[User.__table__,RolePermission.__table__,AuditLog.__table__]);install_schema(engine)
    store=Store(engine)
    receipt(store);finish(store,claim(store))
    with Session(engine) as s:
        user=User(username='synthetic-admin',password_hash='unused',role='admin');s.add(user);s.commit();uid=user.id
    @contextmanager
    def session():
        with Session(engine) as s:yield s
    from app import shared
    monkeypatch.setattr(shared,'managed_session',session);monkeypatch.setattr(shared,'recent_db_failure',lambda:False)
    monkeypatch.setattr(module,'store',store)
    settings=SimpleNamespace(loyalty_shop_domain=POLICY.shop_domain,shopify_store_domain=POLICY.shop_domain,loyalty_shop_id=POLICY.shop_id,loyalty_location_ids=','.join(POLICY.locations),loyalty_launch_at=POLICY.launch_at.isoformat(),loyalty_exception_owner='synthetic-test-owner',loyalty_receiving_enabled=False,loyalty_processing_enabled=True,loyalty_posting_enabled=True,loyalty_read_access_verified=True,loyalty_budget_cents=0,loyalty_evidence_retention_days=30,loyalty_ledger_retention_days=30,loyalty_backup_retention_days=30)
    monkeypatch.setattr(module,'settings',settings)
    app=FastAPI();app.add_middleware(SessionMiddleware,secret_key='test-session-'+'x'*40)
    @app.get('/test-login')
    def login(request:Request):request.session['user_id']=uid;return {'ok':True}
    app.include_router(module.router)
    with TestClient(app) as client:
        yield client,store,uid,settings

def login(c):
    c.get('/test-login');r=c.get('/loyalty');assert r.status_code==200
    import re
    return re.search(r'name="csrf_token" value="([^"]+)"',r.text).group(1)

def test_lookup_auth_and_id_only(ops):
    c,store,uid,_=ops
    assert c.get('/loyalty').status_code==401
    login(c)
    response=c.get('/loyalty',params={'customer_id':'3'})
    assert response.status_code==200 and '20 points' in response.text
    assert c.get('/loyalty',params={'customer_id':'email@example.test'}).status_code==400
    with Session(store.engine) as s:
        s.add(RolePermission(role='admin',resource_key='ops.loyalty.view',is_allowed=False));s.commit()
    assert c.get('/loyalty').status_code==403

@pytest.mark.parametrize('role',['manager','employee','viewer','reviewer'])
def test_nonadmin_requires_explicit_matrix_grant(ops,role):
    c,store,uid,_=ops;c.get('/test-login')
    with Session(store.engine) as s:
        u=s.get(User,uid);u.role=role;s.add(u);s.commit()
    assert c.get('/loyalty').status_code==403
    with Session(store.engine) as s:
        s.add(RolePermission(role=role,resource_key='ops.loyalty.view',is_allowed=True));s.commit()
    assert c.get('/loyalty').status_code==200

def test_inactive_and_revoked_sessions(ops):
    c,store,uid,_=ops;login(c)
    with Session(store.engine) as s:
        u=s.get(User,uid);u.session_invalidated_at=SALE.replace(tzinfo=None);s.add(u);s.commit()
    assert c.get('/loyalty').status_code==401
    with Session(store.engine) as s:
        u=s.get(User,uid);u.is_active=False;s.add(u);s.commit()
    c.get('/test-login');assert c.get('/loyalty').status_code==401

def test_csrf_bounded_replay_and_audit(ops):
    c,store,uid,_=ops;token=login(c)
    data={'ids':'1','request_key':'replay-test','csrf_token':token}
    assert c.post('/loyalty/replay',data={'ids':'1'}).status_code==403
    assert c.post('/loyalty/replay',data=data,follow_redirects=False).status_code==303
    assert c.post('/loyalty/replay',data=data,follow_redirects=False).status_code==303
    with Session(store.engine) as s:
        assert len(s.exec(select(AuditLog).where(AuditLog.action=='loyalty.replay')).all())==1
        assert len(s.exec(select(LoyaltyInbox)).all())==2
    data['ids']=','.join(str(x) for x in range(26))
    assert c.post('/loyalty/replay',data=data).status_code==400

def test_correction_idempotency_revision_bounds_and_hold(ops):
    c,store,uid,settings=ops;token=login(c)
    data={'entitlement_id':'1','expected_revision':'1','target_points':'19','request_key':'correction-test','reason':'refund_review','reference':'ticket-123','csrf_token':token}
    assert c.post('/loyalty/correct',data=data,follow_redirects=False).status_code==303
    assert c.post('/loyalty/correct',data=data,follow_redirects=False).status_code==303
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==19
    with Session(store.engine) as s:
        assert len(s.exec(select(AuditLog).where(AuditLog.action=='loyalty.correction')).all())==1
        assert s.exec(select(LoyaltyEntitlement)).one().reason=='manual_correction_hold'
    data.update(request_key='correction-2',target_points='20')
    assert c.post('/loyalty/correct',data=data).status_code==409
    data.update(expected_revision='2',target_points='1000')
    assert c.post('/loyalty/correct',data=data).status_code==400
    settings.loyalty_posting_enabled=False
    data['target_points']='18'
    assert c.post('/loyalty/correct',data=data).status_code==503

def test_explicit_admin_correction_and_replay_denies(ops):
    c,store,uid,_=ops;token=login(c)
    with Session(store.engine) as s:
        for resource in ('admin.loyalty.correct','admin.loyalty.reconcile'):
            s.add(RolePermission(role='admin',resource_key=resource,is_allowed=False))
        s.commit()
    assert c.post('/loyalty/replay',data={'ids':'1','request_key':'denied','csrf_token':token}).status_code==403
    assert c.post('/loyalty/correct',data={'entitlement_id':1,'expected_revision':1,'target_points':19,'request_key':'denied','reason':'refund_review','reference':'ticket-1','csrf_token':token}).status_code==403

def test_replay_request_key_is_scoped_to_exact_id_set(ops):
    c,store,uid,_=ops;token=login(c)
    assert c.post('/loyalty/replay',data={'ids':'1','request_key':'fixed','csrf_token':token},follow_redirects=False).status_code==303
    assert c.post('/loyalty/replay',data={'ids':'2','request_key':'fixed','csrf_token':token}).status_code==400

def test_permission_seed_honors_denies_and_runs_without_portal(ops):
    _,store,_,_=ops
    from app.loyalty.access import seed_loyalty_permissions
    from app.permissions import grouped_resource_keys
    with Session(store.engine) as s:
        s.add(RolePermission(role='admin',resource_key='ops.loyalty.view',is_allowed=False));s.commit()
        seed_loyalty_permissions(s);seed_loyalty_permissions(s)
        rows=s.exec(select(RolePermission).where(RolePermission.resource_key.like('%loyalty%'))).all()
        assert len(rows)==15
        assert not any(r.is_allowed for r in rows if r.role!='admin' or r.resource_key=='ops.loyalty.view')
    assert {'ops.loyalty.view','admin.loyalty.correct','admin.loyalty.reconcile'} <= {k for _,_,keys in grouped_resource_keys() for k in keys}


def test_verified_attachment_before_account_queues_fresh_canonical_and_audits(ops,monkeypatch):
    from app.loyalty.domain import fingerprint
    from app.loyalty import ops as actions
    c,store,uid,settings=ops;token=login(c)
    late=SALE+timedelta(days=8)
    real_naive=actions.naive
    monkeypatch.setattr(actions,'naive',lambda value=None:real_naive(value if value is not None else late))
    o=order(id='gid://shopify/Order/88',customer_id='gid://shopify/Customer/88')
    store.receive(POLICY.shop_domain,'late-attached','orders/updated',{'order_id':o['id']},'8'*64,late)
    assert finish(store,claim(store,late),o,now=late)==0
    with Session(store.engine) as s:
        row=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.order_id==o['id'])).one()
        assert not row.account_id and not row.basis_hash
        data={'entitlement_id':row.id,'expected_revision':row.revision,'evidence_hash':fingerprint(json.loads(row.observed_evidence_json)),
              'attached_at':(SALE+timedelta(days=6)).isoformat(),'reference':'case-88','request_key':'verify-88','csrf_token':token}
    # Verification is allowed while posting is OFF, and cannot post on its own.
    settings.loyalty_posting_enabled=False
    assert c.post('/loyalty/verify-attachment',data={k:v for k,v in data.items() if k!='csrf_token'}).status_code==403
    assert c.post('/loyalty/verify-attachment',data=data,follow_redirects=False).status_code==303
    assert c.post('/loyalty/verify-attachment',data=data,follow_redirects=False).status_code==303
    assert store.balance(POLICY.shop_domain,o['customer_id'])==0
    with Session(store.engine) as s:
        assert len(s.exec(select(AuditLog).where(AuditLog.action=='loyalty.verify_attachment')).all())==1
    assert finish(store,claim(store,late),o,now=late,posting=False)==0
    store.receive(POLICY.shop_domain,'activated-later','orders/updated',{'order_id':o['id']},'9'*64,late)
    assert finish(store,claim(store,late),o,now=late)==20


@pytest.mark.parametrize('change',['channel','cutoff','customer','late','stale','busy'])
def test_verified_attachment_cannot_bypass_evidence_or_order_lock(ops,monkeypatch,change):
    from app.loyalty.domain import fingerprint
    from app.loyalty import ops as actions
    c,store,uid,_=ops;token=login(c);late=SALE+timedelta(days=8)
    real_naive=actions.naive
    monkeypatch.setattr(actions,'naive',lambda value=None:real_naive(value if value is not None else late))
    o=order(id='gid://shopify/Order/88',customer_id='gid://shopify/Customer/88')
    store.receive(POLICY.shop_domain,'late','orders/updated',{'order_id':o['id']},'8'*64,late)
    finish(store,claim(store,late),o,now=late)
    with Session(store.engine) as s:
        row=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.order_id==o['id'])).one()
        evidence=json.loads(row.observed_evidence_json)
        if change=='channel':evidence['source']='tiktok'
        if change=='cutoff':evidence['created_at']=(POLICY.launch_at-timedelta(days=1)).isoformat()
        if change=='customer':evidence['customer_id']='gid://shopify/Customer/99'
        row.observed_evidence_json=json.dumps(evidence)
        if change=='busy':row.lease_until=(late+timedelta(minutes=1)).replace(tzinfo=None)
        s.add(row);s.commit()
        data={'entitlement_id':row.id,'expected_revision':row.revision,'evidence_hash':fingerprint(evidence),
              'attached_at':(late if change=='late' else SALE).isoformat(),'reference':'case-88','request_key':'verify-88','csrf_token':token}
    if change=='stale':data['evidence_hash']='0'*64
    assert c.post('/loyalty/verify-attachment',data=data).status_code==409
    assert store.balance(POLICY.shop_domain,o['customer_id'])==0


def test_cashier_page_and_sidebar_are_useful_without_admin_diagnostics(ops):
    c,store,uid,_=ops;c.get('/test-login')
    with Session(store.engine) as s:
        u=s.get(User,uid);u.role='cashier';s.add(u)
        s.add(RolePermission(role='cashier',resource_key='ops.loyalty.view',is_allowed=True));s.commit()
    html=c.get('/loyalty?customer_id=3').text
    assert '$19.99' in html and 'Order #10' in html and '20 points' in html
    assert 'href="/loyalty" class="linear-sidebar-item' in html
    for hidden in ('Administrator details','original_cents','revision','Receiving:','Posting:','USD cents'):
        assert hidden not in html


def test_navigation_lookup_failure_cannot_break_shared_pages(ops,monkeypatch):
    from app.loyalty import access
    from sqlalchemy.exc import OperationalError
    c,_,_,_=ops;login(c)
    def broken(*a):raise OperationalError('synthetic',{},Exception())
    monkeypatch.setattr(access,'has_permission',broken)
    assert c.get('/loyalty').status_code==200
