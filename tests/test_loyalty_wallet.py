"""Rewards-card sign-up links, the register action and the public page."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.loyalty.models import now_utc
from app.loyalty.wallet import LIFETIME, TOKEN, WalletError, consume, create, gates, link, qr_png, valid
from app.routers import loyalty_wallet
from app.routers.loyalty_wallet import build_router
from test_loyalty_pos import SHOP, configuration, token as staff_token
from test_loyalty_service import store

KEY = 'synthetic-member-code-key-' + 'k' * 40
BASE = 'https://cards.example.test'
POS = '/api/loyalty/pos/customers/101/wallet-card'
WHO = 's' * 64


def enabled(**changes):
    return configuration(**{'loyalty_member_scan_enabled': True, 'loyalty_member_code_secret': KEY,
                            'loyalty_wallet_enrollment_enabled': True, 'loyalty_card_base_url': BASE, **changes})


def client(store, settings=None):
    app = FastAPI(); app.include_router(build_router(store.engine, settings or enabled()))
    return TestClient(app)


def auth(**headers):
    return {'Authorization': 'Bearer ' + staff_token(), **headers}


def path(c):
    return c.post(POS, headers=auth()).json()['url'][len(BASE):]


def test_off_by_default_and_needs_scan_settings_and_an_https_address():
    assert gates(configuration()) == ['wallet_enrollment_disabled']
    assert gates(enabled(loyalty_member_scan_enabled=False)) == ['member_scan_disabled']
    for bad in ('', 'http://cards.example.test', 'https://cards.example.test/', 'https://cards.example.test/x',
                'https://user@cards.example.test', 'javascript:alert(1)', 'https://CARDS.example.test'):
        assert gates(enabled(loyalty_card_base_url=bad)) == ['card_base_url_unset'], bad
    assert gates(enabled()) == []
    assert gates(enabled(loyalty_card_base_url='https://cards.example.test:8443')) == []


def test_link_is_random_hashed_at_rest_and_single_use(store):
    settings = enabled()
    token, expires = create(store.engine, settings, SHOP, '101', WHO)
    assert TOKEN.fullmatch(token)
    assert link(settings, token) == f'{BASE}/rewards/card/{token}'
    with store.engine.connect() as connection:
        stored = connection.execute(text('SELECT token_hash, customer_id FROM loyalty_wallet_enrollment')).all()
    assert len(stored) == 1 and token not in str(stored) and stored[0][1] == 'gid://shopify/Customer/101'
    assert valid(store.engine, settings, token)
    assert valid(store.engine, settings, token)                  # looking does not use it up
    assert consume(store.engine, settings, token, 'apple') == '101'
    assert consume(store.engine, settings, token, 'google') is None
    assert not valid(store.engine, settings, token)


def test_link_expires_after_ten_minutes(store):
    settings = enabled(); start = now_utc()
    token, expires = create(store.engine, settings, SHOP, '101', WHO, now=start)
    assert expires - start == LIFETIME == timedelta(minutes=10)
    assert valid(store.engine, settings, token, now=start + LIFETIME - timedelta(seconds=1))
    assert not valid(store.engine, settings, token, now=start + LIFETIME)
    assert consume(store.engine, settings, token, 'apple', now=start + LIFETIME) is None


def test_a_new_link_for_the_same_customer_retires_the_old_one_only(store):
    settings = enabled()
    first, _ = create(store.engine, settings, SHOP, '101', WHO)
    other, _ = create(store.engine, settings, SHOP, '102', WHO)
    second, _ = create(store.engine, settings, SHOP, '101', WHO)
    assert not valid(store.engine, settings, first)
    assert consume(store.engine, settings, first, 'apple') is None
    assert valid(store.engine, settings, second) and valid(store.engine, settings, other)
    assert consume(store.engine, settings, other, 'google') == '102'


def test_two_phones_racing_one_link_get_one_card(store):
    settings = enabled()
    token, _ = create(store.engine, settings, SHOP, '101', WHO)
    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(lambda _: consume(store.engine, settings, token, 'apple'), range(8)))
    assert sorted(r for r in results if r) == ['101']


def test_links_do_not_cross_shops_and_bad_input_is_refused(store):
    settings = enabled()
    token, _ = create(store.engine, settings, 'other-shop.myshopify.com', '101', WHO)
    assert not valid(store.engine, settings, token)
    assert consume(store.engine, settings, token, 'apple') is None
    for bad in (None, '', 'x', token + 'x', 'x OR 1=1 --', 'a' * 500):
        assert not valid(store.engine, settings, bad)
        assert consume(store.engine, settings, bad, 'apple') is None
    with pytest.raises(WalletError):
        consume(store.engine, settings, token, 'windows')
    with pytest.raises(ValueError):
        create(store.engine, settings, SHOP, '0', WHO)
    with pytest.raises(WalletError):
        create(store.engine, configuration(), SHOP, '101', WHO)


def test_old_links_are_deleted_after_a_day(store):
    settings = enabled(); start = now_utc()
    create(store.engine, settings, SHOP, '101', WHO, now=start - timedelta(days=2))
    create(store.engine, settings, SHOP, '102', WHO, now=start)
    with store.engine.connect() as connection:
        rows = connection.execute(text('SELECT customer_id FROM loyalty_wallet_enrollment')).scalars().all()
    assert rows == ['gid://shopify/Customer/102']


def test_register_gets_a_link_and_a_qr_address(store):
    result = client(store).post(POS, headers=auth())
    assert result.status_code == 200, result.text
    body = result.json()
    assert set(body) == {'customer_id', 'url', 'qr_url', 'expires_at'} and body['customer_id'] == '101'
    assert body['url'].startswith(BASE + '/rewards/card/') and body['qr_url'] == body['url'] + '/qr.png'
    assert body['expires_at'].endswith('Z')
    assert result.headers['cache-control'] == 'no-store'


def test_register_action_requires_staff_token_and_valid_request(store):
    c = client(store)
    assert c.post(POS).status_code == 401
    assert c.post(POS, headers={'Authorization': 'Bearer ' + staff_token(aud='other')}).status_code == 401
    assert c.post(POS, headers=auth(Origin='https://evil.example')).status_code == 403
    assert c.post(POS + '?x=1', headers=auth()).status_code == 400
    assert c.post(POS.replace('101', '0'), headers=auth()).status_code == 400
    assert c.post(POS, headers=auth(), content='x' * 50).status_code == 400
    assert c.get(POS, headers=auth()).status_code == 405
    assert client(store, configuration()).post(POS, headers=auth()).status_code == 503
    with store.engine.connect() as connection:
        assert connection.execute(text('SELECT COUNT(*) FROM loyalty_wallet_enrollment')).scalar() == 0


def test_register_action_is_rate_limited_per_session(store):
    c = client(store, enabled(loyalty_pos_session_requests=2))
    assert [c.post(POS, headers=auth()).status_code for _ in range(3)] == [200, 200, 429]


def test_cors_allows_only_shopify_post(store):
    c = client(store)
    ok = c.options(POS, headers={'Origin': 'https://cdn.shopify.com', 'Access-Control-Request-Method': 'POST',
                                 'Access-Control-Request-Headers': 'authorization'})
    assert ok.status_code == 204 and 'access-control-allow-credentials' not in ok.headers
    assert c.options(POS, headers={'Origin': 'https://evil.example', 'Access-Control-Request-Method': 'POST'}).status_code == 403


def test_public_page_shows_no_customer_data_and_does_not_use_the_link(store):
    c = client(store); page = path(c)
    for _ in range(2):
        result = c.get(page)
        assert result.status_code == 200
        assert 'Degen Rewards' in result.text and 'Customer' not in result.text
    assert result.headers['cache-control'] == 'no-store'
    assert result.headers['referrer-policy'] == 'no-referrer'
    assert 'noindex' in result.headers['x-robots-tag']
    assert "default-src 'none'" in result.headers['content-security-policy']
    assert result.headers['x-frame-options'] == 'DENY'
    assert 'set-cookie' not in result.headers
    # No wallet platform is wired up yet, so the page says so instead of offering a dead button.
    assert 'not available yet' in result.text and '<form' not in result.text


def test_public_page_offers_buttons_once_a_platform_exists(store, monkeypatch):
    monkeypatch.setattr(loyalty_wallet, 'platforms', lambda settings: ['apple', 'google'])
    c = client(store); page = path(c)
    html = c.get(page).text
    assert f'action="{page}/apple"' in html and f'action="{page}/google"' in html
    assert 'Add to Apple Wallet' in html and 'Add to Google Wallet' in html


def test_qr_image_is_a_png_of_the_link(store):
    c = client(store); page = path(c)
    result = c.get(page + '/qr.png')
    assert result.status_code == 200 and result.headers['content-type'] == 'image/png'
    assert result.content[:4] == b'\x89PNG' and result.headers['cache-control'] == 'no-store'
    cv2 = pytest.importorskip('cv2'); numpy = pytest.importorskip('numpy')
    image = cv2.imdecode(numpy.frombuffer(result.content, numpy.uint8), cv2.IMREAD_COLOR)
    decoded, _, _ = cv2.QRCodeDetector().detectAndDecode(image)
    assert decoded == BASE + page


def test_expired_used_unknown_and_malformed_links_all_look_the_same(store):
    settings = enabled(); c = client(store, settings)
    used = path(c); consume(store.engine, settings, used.rsplit('/', 1)[1], 'apple')
    old, _ = create(store.engine, settings, SHOP, '103', WHO, now=now_utc() - timedelta(minutes=11))
    replies = [c.get(p) for p in (used, '/rewards/card/' + old, '/rewards/card/' + 'A' * 43, '/rewards/card/short',
                                  '/rewards/card/%3Cscript%3Ealert(1)%3Cscript%3E')]
    assert {r.status_code for r in replies} == {404}
    assert len({r.text for r in replies}) == 1 and 'expired' in replies[0].text and '<script' not in replies[0].text
    assert c.get(used + '/qr.png').status_code == 404
    assert c.get('/rewards/card/' + 'A' * 43 + '/qr.png').status_code == 404


def test_public_routes_are_closed_when_the_feature_is_off_and_are_rate_limited(store):
    c = client(store); page = path(c)
    off = client(store, configuration())
    assert off.get(page).status_code == 404 and off.get(page + '/qr.png').status_code == 404
    limited = client(store, enabled(loyalty_pos_total_requests=2))
    assert [limited.get(page).status_code for _ in range(3)] == [200, 200, 404]


def test_qr_png_helper_makes_an_image():
    assert qr_png('https://cards.example.test/rewards/card/x')[:4] == b'\x89PNG'
