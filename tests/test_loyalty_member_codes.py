"""Member codes and the register lookup: synthetic data, no providers."""
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.loyalty import member_codes
from app.loyalty.member_codes import MemberCodeError, PATTERN, gates, issue, resolve, revoke
from app.routers.loyalty_member import PATH, build_router
from test_loyalty_pos import SHOP, configuration, token
from test_loyalty_service import store

KEY = 'synthetic-member-code-key-' + 'k' * 40


def enabled(**changes):
    return configuration(**{'loyalty_member_scan_enabled': True, 'loyalty_member_code_secret': KEY, **changes})


def client(store, settings=None):
    app = FastAPI(); app.include_router(build_router(store.engine, settings or enabled()))
    return TestClient(app)


def post(c, code, **headers):
    return c.post(PATH, headers={'Authorization': 'Bearer ' + token(), **headers}, json={'code': code})


def test_off_by_default_and_gated_on_secret_and_register_access():
    assert gates(configuration()) == ['member_scan_disabled']
    assert gates(configuration(loyalty_member_scan_enabled=True)) == ['member_code_secret_unset']
    assert gates(enabled(loyalty_pos_read_enabled=False)) == ['pos_read_unavailable']
    assert gates(enabled()) == []


def test_code_is_opaque_stable_and_stored_only_as_a_hash(store):
    settings = enabled()
    code = issue(store.engine, settings, SHOP, '101')
    assert PATTERN.fullmatch(code) and '101' not in code
    assert issue(store.engine, settings, SHOP, '101') == code
    assert issue(store.engine, settings, SHOP, '102') != code
    with store.engine.connect() as connection:
        rows = connection.execute(text('SELECT customer_id, generation, code_hash FROM loyalty_member_code ORDER BY id')).all()
    assert [(r[0], r[1]) for r in rows] == [('gid://shopify/Customer/101', 1), ('gid://shopify/Customer/102', 1)]
    assert all(code not in str(r) for r in rows)
    assert resolve(store.engine, settings, SHOP, code) == '101'


def test_codes_do_not_cross_shops_or_secrets(store):
    settings = enabled()
    code = issue(store.engine, settings, SHOP, '101')
    assert resolve(store.engine, settings, 'other-shop.myshopify.com', code) is None
    assert resolve(store.engine, enabled(loyalty_member_code_secret='z' * 40), SHOP, code) is None


def test_revoked_code_stops_working_and_reissue_gives_a_new_one(store):
    settings = enabled()
    old = issue(store.engine, settings, SHOP, '101')
    assert revoke(store.engine, SHOP, '101') is True
    assert revoke(store.engine, SHOP, '101') is False
    assert resolve(store.engine, settings, SHOP, old) is None
    new = issue(store.engine, settings, SHOP, '101')
    assert new != old and resolve(store.engine, settings, SHOP, new) == '101'
    assert resolve(store.engine, settings, SHOP, old) is None


def test_concurrent_issue_yields_one_code(store):
    settings = enabled()
    with ThreadPoolExecutor(4) as pool:
        codes = set(pool.map(lambda _: issue(store.engine, settings, SHOP, '101'), range(8)))
    assert len(codes) == 1
    with store.engine.connect() as connection:
        assert connection.execute(text('SELECT COUNT(*) FROM loyalty_member_code')).scalar() == 1


@pytest.mark.parametrize('customer', ['0', '-1', 'abc', '1 OR 1=1', '9007199254740992', ''])
def test_issue_rejects_bad_customer_ids(store, customer):
    with pytest.raises(MemberCodeError):
        issue(store.engine, enabled(), SHOP, customer)


def test_disabled_issue_and_resolve_refuse(store):
    with pytest.raises(MemberCodeError):
        issue(store.engine, configuration(), SHOP, '101')
    with pytest.raises(MemberCodeError):
        resolve(store.engine, configuration(), SHOP, 'DGR1' + 'A' * 26)


def test_lookup_returns_only_the_customer_id(store):
    code = issue(store.engine, enabled(), SHOP, '101')
    result = post(client(store), code)
    assert result.status_code == 200 and result.json() == {'customer_id': '101'}
    assert result.headers['cache-control'] == 'no-store'


def test_lookup_requires_a_register_token(store):
    code = issue(store.engine, enabled(), SHOP, '101')
    c = client(store)
    assert c.post(PATH, json={'code': code}).status_code == 401
    assert c.post(PATH, headers={'Authorization': 'Bearer ' + token(aud='other')}, json={'code': code}).status_code == 401
    assert c.post(PATH, headers={'Authorization': 'Bearer ' + token(iss='https://evil.myshopify.com/admin')},
                  json={'code': code}).status_code == 401


def test_lookup_off_returns_unavailable_even_with_a_valid_token(store):
    code = issue(store.engine, enabled(), SHOP, '101')
    off = client(store, configuration())
    assert post(off, code).status_code == 503
    assert off.options(PATH, headers={'Origin': 'https://cdn.shopify.com', 'Access-Control-Request-Method': 'POST'}).status_code == 503


def test_lookup_rejects_unknown_malformed_and_oversized_requests(store):
    issue(store.engine, enabled(), SHOP, '101')
    c = client(store); headers = {'Authorization': 'Bearer ' + token()}
    assert post(c, 'DGR1' + 'A' * 26).status_code == 404
    for bad in ('', '101', 'gid://shopify/Customer/101', 'dgr1' + 'a' * 26, 'DGR1' + 'A' * 27, 101, None, ['x']):
        assert post(c, bad).status_code == 400, bad
    assert c.post(PATH, headers=headers, json={'code': 'DGR1' + 'A' * 26, 'customer_id': '101'}).status_code == 400
    assert c.post(PATH, headers=headers, json=['DGR1' + 'A' * 26]).status_code == 400
    assert c.post(PATH, headers=headers, content='x' * 2000).status_code == 400
    assert c.post(PATH + '?code=x', headers=headers, json={'code': 'DGR1' + 'A' * 26}).status_code == 400
    assert c.post(PATH, headers=dict(headers, **{'Content-Type': 'application/json'}), content='{' + ' ' * 2000 + '}').status_code == 400
    assert post(c, 'DGR1' + 'A' * 26, Origin='https://evil.example').status_code == 403
    assert c.get(PATH, headers=headers).status_code == 405


def test_lookup_guessing_is_rate_limited_per_register_session(store):
    c = client(store, enabled(loyalty_pos_session_requests=3))
    assert [post(c, 'DGR1' + 'A' * 26).status_code for _ in range(4)] == [404, 404, 404, 429]


def test_cors_allows_only_shopify_post_and_never_cookies(store):
    c = client(store)
    ok = c.options(PATH, headers={'Origin': 'https://cdn.shopify.com', 'Access-Control-Request-Method': 'POST',
                                  'Access-Control-Request-Headers': 'authorization,content-type'})
    assert ok.status_code == 204 and ok.headers['Access-Control-Allow-Methods'] == 'POST'
    assert 'access-control-allow-credentials' not in ok.headers
    assert c.options(PATH, headers={'Origin': 'https://evil.example', 'Access-Control-Request-Method': 'POST'}).status_code == 403
    code = issue(store.engine, enabled(), SHOP, '101')
    assert post(c, code, Origin='https://cdn.shopify.com').headers['access-control-allow-origin'] == 'https://cdn.shopify.com'


def test_tampered_row_cannot_redirect_a_code_to_another_customer(store):
    settings = enabled()
    code = issue(store.engine, settings, SHOP, '101')
    with store.engine.begin() as connection:
        connection.execute(text("UPDATE loyalty_member_code SET customer_id='gid://shopify/Customer/999'"))
    assert resolve(store.engine, settings, SHOP, code) is None
    assert member_codes.PREFIX == 'DGR1'
