"""Synthetic POS reads: no Ops identities, cookies, providers or accounting writes."""
import importlib.util
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlmodel import Session, create_engine

from app.config import Settings
from app.loyalty.models import LoyaltyAccount, LoyaltyEntitlement, LoyaltyLedger
from app.loyalty.schema import install_schema
from test_loyalty_service import store

SHOP = 'synthetic-pos.myshopify.com'
SECRET = 'synthetic-pos-signing-key-' + 'x' * 40
CLIENT = 'synthetic-pos-client'
PATH = '/api/loyalty/pos/customers/101'


def configuration(**changes):
    values = dict(loyalty_shop_domain=SHOP, loyalty_shop_id='gid://shopify/Shop/1',
                  loyalty_pos_read_enabled=True, loyalty_pos_all_staff_enabled=True,
                  loyalty_pos_client_id=CLIENT, loyalty_pos_client_secret=SECRET,
                  loyalty_pos_session_requests=60, loyalty_pos_total_requests=600)
    values.update(changes)
    return Settings(**{Settings.model_fields[k].alias or k:v for k,v in values.items()})


def token(**changes):
    now = int(time.time())
    claims = dict(iss=f'https://{SHOP}/admin', dest=f'https://{SHOP}', aud=CLIENT,
                  sub='42', sid='synthetic-session', iat=now, nbf=now, exp=now + 60)
    claims.update(changes)
    return jwt.encode(claims, SECRET, algorithm='HS256')


def add_entry(engine, delta, *, customer='101', shop=SHOP, hold=False, ledger_id=None):
    with Session(engine) as session:
        from sqlmodel import select
        account = session.exec(select(LoyaltyAccount).where(
            LoyaltyAccount.shop == shop, LoyaltyAccount.customer_id == f'gid://shopify/Customer/{customer}')).first()
        if not account:
            account = LoyaltyAccount(shop=shop, customer_id=f'gid://shopify/Customer/{customer}', identity_hold=hold)
            session.add(account); session.flush()
        count = session.exec(select(LoyaltyLedger)).all()
        number = len(count) + 1
        row = LoyaltyEntitlement(shop=shop, order_id=f'gid://shopify/Order/{number}', account_id=account.id,
                                 customer_id=account.customer_id, status='eligible', checked_at=datetime(2026, 9, 10))
        session.add(row); session.flush()
        session.add(LoyaltyLedger(id=ledger_id, shop=shop, account_id=account.id, entitlement_id=row.id, revision=1,
                                 business_key=f'synthetic-{number}', delta=delta, resulting_points=max(delta, 0),
                                 reason='refund' if delta < 0 else 'earned', rule_version='test', evidence_hash='a'*64,
                                 evidence_json='{"private":"must not appear"}'))
        session.commit()


@pytest.fixture
def engine(store):
    result = store.engine
    add_entry(result, 20); add_entry(result, -1)
    add_entry(result, 999, shop='other.myshopify.com')
    yield result
    result.dispose()


def client(engine, config=None):
    app = FastAPI()
    # Before implementation the real requested route is absent: acceptance fails 404.
    if importlib.util.find_spec('app.routers.loyalty_pos'):
        from app.routers.loyalty_pos import build_router
        app.include_router(build_router(engine, config or configuration()))
    return TestClient(app)


def read(c, path=PATH, bearer=None):
    return c.get(path, headers={'Authorization':'Bearer ' + (bearer or token())})


def test_valid_session_without_ops_binding_returns_exact_scoped_read_only_history(engine):
    mutations = []
    def track(conn, cursor, statement, params, context, many):
        if statement.lstrip().split()[0].upper() in ('INSERT','UPDATE','DELETE','CREATE','ALTER'):
            mutations.append(statement)
    event.listen(engine, 'before_cursor_execute', track)
    response = read(client(engine))
    assert response.status_code == 200
    body = response.json()
    assert body['balance_points'] == '19'
    assert [row['delta_points'] for row in body['history']] == ['-1','20']
    assert body['customer_id'] == '101'
    assert body['posting_paused'] is True
    assert response.headers['cache-control'] == 'no-store'
    assert 'private' not in response.text and 'gid://' not in response.text
    assert 'staff' not in response.text and 'actor' not in response.text
    assert mutations == []


@pytest.mark.parametrize('override', [
    {'loyalty_pos_read_enabled':False}, {'loyalty_pos_all_staff_enabled':False},
    {'loyalty_pos_client_id':''}, {'loyalty_pos_client_secret':''},
    {'loyalty_shop_domain':''}, {'loyalty_shop_id':''},
])
def test_unapproved_or_missing_configuration_fails_closed(engine, override):
    response = read(client(engine, configuration(**override)))
    assert response.status_code == 503
    assert 'balance_points' not in response.text


def test_defaults_deny_even_valid_bearer_and_secret_not_in_settings_repr(engine):
    assert read(client(engine, Settings())).status_code == 503
    assert SECRET not in repr(configuration())
    from app.loyalty.pos_auth import POSAccess
    assert SECRET not in repr(POSAccess.from_settings(configuration()))


@pytest.mark.parametrize('change', [
    {'aud':'another-client'}, {'aud':[CLIENT]}, {'dest':'https://other.myshopify.com'},
    {'iss':f'https://{SHOP}/admin/evil'}, {'exp':1}, {'nbf':int(time.time())+1000},
    {'iat':int(time.time())+1000}, {'sub':''}, {'sub':42}, {'sid':''},
    {'exp':True}, {'iat':'1'}, {'exp':int(time.time())+10000},
])
def test_invalid_claims_never_return_customer_data(engine, change):
    response = read(client(engine), bearer=token(**change))
    assert response.status_code == 401
    assert response.json() == {'error':'authentication_required'}


def test_signature_algorithm_missing_claim_and_cookie_fallback_rejected(engine):
    c = client(engine)
    wrong = jwt.encode(jwt.decode(token(), SECRET, algorithms=['HS256'], audience=CLIENT), 'z'*40, algorithm='HS256')
    none = jwt.encode({'sub':'42'}, '', algorithm='none')
    for invalid in (wrong, none, 'garbage', token(sid=None)):
        assert read(c, bearer=invalid).status_code == 401
    for claim in ('aud','iss','dest','sub','sid','iat','nbf','exp'):
        data = jwt.decode(token(), SECRET, algorithms=['HS256'], audience=CLIENT); del data[claim]
        assert read(c, bearer=jwt.encode(data, SECRET, algorithm='HS256')).status_code == 401
    assert c.get(PATH, headers={'Cookie':'degen_session=synthetic'}).status_code == 401
    assert c.get(PATH, headers=[('Authorization','Bearer '+token()),('Authorization','Bearer '+token())]).status_code == 401


def test_request_bounds_and_no_bulk_or_contact_queries(engine):
    c = client(engine)
    for suffix in ('?email=test@example.invalid','?staff_id=1','?customer_id=102','?limit=26','?limit=0','?limit=1&limit=2'):
        assert read(c, PATH+suffix).status_code == 400
    for customer in ('0','-1','1.5','9007199254740992','01'):
        assert read(c, '/api/loyalty/pos/customers/'+customer).status_code == 400
    assert c.get(PATH, headers={'Authorization':'Bearer '+'x'*5000}).status_code == 431
    assert c.request('GET', PATH, content=b'x', headers={'Authorization':'Bearer '+token()}).status_code == 400
    assert c.post(PATH, headers={'Authorization':'Bearer '+token()}).status_code == 405
    assert c.get('/api/loyalty/pos/customers', headers={'Authorization':'Bearer '+token()}).status_code == 404


def test_signed_cursor_keeps_snapshot_after_new_posting_and_rejects_cross_customer(engine):
    c = client(engine)
    first = read(c, PATH+'?limit=1').json()
    assert first['balance_points'] == '19'
    assert [e['delta_points'] for e in first['history']] == ['-1']
    cursor = first['next_cursor']
    add_entry(engine, 5)
    second = read(c, PATH+'?limit=1&cursor='+cursor)
    assert second.status_code == 200
    assert second.json()['balance_points'] == '19'
    assert [e['delta_points'] for e in second.json()['history']] == ['20']
    assert second.json()['next_cursor'] is None
    assert read(c).json()['balance_points'] == '24'
    assert read(c, '/api/loyalty/pos/customers/102?cursor='+cursor).status_code == 400
    assert read(c, PATH+'?cursor='+cursor[:-5]+'xxxxx').status_code == 400
    assert read(c, bearer=cursor).status_code == 401


def test_unknown_customer_is_empty_without_account_creation(engine):
    from sqlmodel import select
    c = client(engine)
    body = read(c, '/api/loyalty/pos/customers/555').json()
    assert body['balance_points'] == '0' and body['history'] == []
    assert body['has_history'] is False
    with Session(engine) as session:
        assert len(session.exec(select(LoyaltyAccount)).all()) == 2


def test_exact_points_above_javascript_safe_integer_and_identity_hold(engine):
    add_entry(engine, 9007199254740993, customer='202', hold=True)
    body = read(client(engine), '/api/loyalty/pos/customers/202').json()
    assert body['balance_points'] == '9007199254740993'
    assert body['needs_review'] is True


def test_schema_not_ready_and_database_failure_are_sanitized(engine):
    empty = create_engine('sqlite:///:memory:')
    assert read(client(empty)).status_code == 503
    c = client(engine)
    with engine.begin() as conn:conn.execute(text('DROP TABLE loyalty_ledger'))
    response = read(c)
    assert response.status_code == 503 and 'SQL' not in response.text


def test_session_limit_cannot_be_bypassed_with_new_tokens_and_global_limit_bounds_sessions(engine):
    c = client(engine, configuration(loyalty_pos_session_requests=2, loyalty_pos_total_requests=4))
    assert read(c).status_code == 200
    assert read(c, bearer=token(jti='changed-token')).status_code == 200
    response = read(c)
    assert response.status_code == 429 and response.headers['retry-after']
    assert read(c, bearer=token(sid='other-session')).status_code == 200
    assert read(c, bearer=token(sid='third-session')).status_code == 429


def test_concurrent_rate_checks_do_not_exceed_session_limit(engine):
    c = client(engine, configuration(loyalty_pos_session_requests=3))
    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(lambda _:read(c).status_code, range(8)))
    assert codes.count(200) == 3 and codes.count(429) == 5


def test_snapshot_is_one_statement_even_when_an_award_commits_during_read(engine):
    if engine.dialect.name == 'sqlite':
        with engine.connect() as c:c.execute(text('PRAGMA journal_mode=WAL'))
    observed=[]
    def interleave(conn,cursor,statement,params,context,many):
        if statement.startswith('WITH customer_ledger') and not observed:
            observed.append(True)
            add_entry(engine,5)
    event.listen(engine,'after_cursor_execute',interleave)
    try:
        response=read(client(engine)).json()
    finally:event.remove(engine,'after_cursor_execute',interleave)
    assert observed
    assert response['balance_points']=='19'
    assert sum(int(e['delta_points']) for e in response['history'])==19
    assert read(client(engine)).json()['balance_points']=='24'


def test_late_lower_id_commit_requires_new_snapshot(engine):
    # PostgreSQL sequences allocate IDs before commit. A lower reserved ID may
    # become visible after a larger ID; a watermark alone is insufficient.
    add_entry(engine,3,ledger_id=10)
    c=client(engine)
    first=read(c,PATH+'?limit=1').json()
    add_entry(engine,2,ledger_id=8)
    response=read(c,PATH+'?cursor='+first['next_cursor'])
    assert response.status_code==409
    assert response.json()=={'error':'refresh_required'}


def test_expired_cursor_and_untrusted_pin_headers(engine):
    from app.loyalty.pos_auth import POSAccess
    access=POSAccess.from_settings(configuration())
    c=client(engine)
    first=read(c,PATH+'?limit=1').json()
    claims=access.decode_cursor(first['next_cursor'],'101');claims['exp']=1
    expired=jwt.encode(claims,access._cursor_secret(),algorithm='HS256')
    assert read(c,PATH+'?cursor='+expired).status_code==400
    assert c.get(PATH,headers={'X-Shopify-Staff-Id':'42'}).status_code==401
    assert c.get(PATH,headers={'Authorization':'Bearer '+token(),'X-Shopify-Staff-Id':'999'}).status_code==200


def test_shopify_cors_preflight_does_not_replace_bearer_auth(engine):
    c=client(engine)
    headers={'Origin':'https://extensions.shopifycdn.com','Access-Control-Request-Method':'GET','Access-Control-Request-Headers':'authorization'}
    preflight=c.options(PATH,headers=headers)
    assert preflight.status_code==204
    assert preflight.headers['access-control-allow-origin']==headers['Origin']
    assert 'access-control-allow-credentials' not in preflight.headers
    denied=c.get(PATH,headers={'Origin':headers['Origin']})
    assert denied.status_code==401
    assert denied.headers['access-control-allow-origin']==headers['Origin']
    allowed=c.get(PATH,headers={'Origin':'https://cdn.shopify.com','Authorization':'Bearer '+token()})
    assert allowed.status_code==200
    assert allowed.headers['access-control-allow-origin']=='https://cdn.shopify.com'
    for overrides in ({'Origin':'https://evil.invalid'},{'Access-Control-Request-Method':'POST'},{'Access-Control-Request-Headers':'authorization,x-staff-id'}):
        assert c.options(PATH,headers=dict(headers,**overrides)).status_code==403
    assert client(engine,configuration(loyalty_pos_all_staff_enabled=False)).options(PATH,headers=headers).status_code==503
