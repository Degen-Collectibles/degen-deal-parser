import io
import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from PIL import Image
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select
from starlette.middleware.sessions import SessionMiddleware

from app.csrf import issue_token
from app.db import get_session
from app.models import AppSetting, AuditLog, TikTokProduct, User
from app.routers import tiktok_listing_assistant as routes
from app.tiktok import listing_assistant as service


def png():
    out = io.BytesIO()
    Image.new('RGB', (300, 450), '#d83544').save(out, 'PNG')
    return out.getvalue()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv('TIKTOK_LISTING_ASSET_DIR', str(tmp_path / 'images'))
    monkeypatch.setenv('TIKTOK_LISTING_ASSISTANT_ENABLED', 'true')
    monkeypatch.setenv('TIKTOK_LISTING_STOCK_TRANSFERS_ENABLED', 'true')
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine, tables=[User.__table__, AppSetting.__table__, AuditLog.__table__, TikTokProduct.__table__])
    with Session(engine) as session:
        session.add(User(id=1, username='listing-test', password_hash='unused', role='admin'))
        session.commit()
    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key='test-only-listing-key')

    @app.middleware('http')
    async def actor(request, call_next):
        request.state.current_user = SimpleNamespace(id=1, role=request.headers.get('x-role', 'admin'), is_active=request.headers.get('x-active', 'true') == 'true')
        return await call_next(request)

    @app.get('/csrf')
    def csrf(request: Request):
        return {'token': issue_token(request)}

    def session_override():
        with Session(engine) as session:
            yield session
    app.dependency_overrides[get_session] = session_override
    app.include_router(routes.router)
    with TestClient(app) as test_client:
        token = test_client.get('/csrf').json()['token']
        test_client.headers['X-CSRF-Token'] = token
        yield test_client
    engine.dispose()


BASE = '/tiktok/products/assistant'


@pytest.mark.parametrize('choice,label,ids', [
    ('Riftbound', 'Riftbound', ('89',)),
    ('Dragon Ball Super: Fusion World', 'Dragon Ball Super: Fusion World', ('80',)),
    ('catalog:86', 'Gundam Card Game', ('86',)),
])
def test_listing_search_uses_exact_game_catalog_and_persists_choice(client, choice, label, ids):
    from unittest.mock import AsyncMock
    from app.tiktok import listing_games
    d = create(client)
    discovered = [{'id': 'catalog:86', 'name': 'Gundam Card Game', 'category_ids': ['86']}]
    with patch.object(listing_games, 'catalog', return_value=(discovered, '')), patch(
        'app.inventory.routes._search_sealed_products', new=AsyncMock(return_value=([], ''))
    ) as lookup:
        d = write(client, d, 'search', {'query': 'Origins booster pack', 'game': choice})
    lookup.assert_awaited_once_with('Origins booster pack', game=label, limit=8, catalog_category_ids=ids)
    saved = client.get(f"{BASE}/drafts/{d['id']}").json()
    assert saved['search_game'] == {'id': choice, 'name': label, 'category_ids': list(ids)}
    assert saved['search_query'] == 'Origins booster pack'


@pytest.mark.parametrize('choice', ['', 'Other', 'Dragon Ball', 'Dragon Ball Super', 'Imaginary Card Game', 'catalog:99999'])
def test_unknown_or_ambiguous_game_never_searches_pokemon(client, choice):
    from unittest.mock import AsyncMock
    from app.tiktok import listing_games
    d = create(client)
    with patch.object(listing_games, 'catalog', return_value=([], '')), patch(
        'app.inventory.routes._search_sealed_products', new=AsyncMock()
    ) as lookup:
        response = client.post(f"{BASE}/drafts/{d['id']}/search", json={'version': d['version'], 'query': 'booster pack', 'game': choice})
    assert response.status_code == 422
    lookup.assert_not_awaited()


def test_game_discovery_filters_results_and_keeps_role_gate(client):
    from app.tiktok import listing_games
    catalog = [{'id': 'catalog:86', 'name': 'Gundam Card Game', 'category_ids': ['86']},
               {'id': 'catalog:80', 'name': 'Dragon Ball Super: Fusion World', 'category_ids': ['80']}]
    with patch.object(listing_games, 'catalog', return_value=(catalog, '')):
        response = client.get(BASE + '/games?q=gundam')
        assert response.status_code == 200
        assert response.json()['games'] == catalog[:1]
        assert client.get(BASE + '/games?q=gundam', headers={'x-role': 'employee'}).status_code == 403


def test_new_games_remain_available_during_game_discovery_outage(client):
    from unittest.mock import AsyncMock
    from app.tiktok import listing_games
    with patch.object(listing_games, 'catalog', return_value=([], 'Game catalog is unavailable.')):
        response = client.get(BASE + '/games?q=unknown')
        assert response.json() == {'games': [], 'warning': 'Game catalog is unavailable.'}
        d = create(client)
        with patch('app.inventory.routes._search_sealed_products', new=AsyncMock(return_value=([], ''))):
            d = write(client, d, 'search', {'query': 'booster pack', 'game': 'Dragon Ball Super: Fusion World'})
        assert d['search_game']['category_ids'] == ['80']


@pytest.mark.parametrize('game,matching,wrong', [
    ('Dragon Ball Super: Fusion World', 'Dragon Ball Super Fusion World Origins Booster Pack English Live Rip', 'Dragon Ball Super Masters Origins Booster Pack English Live Rip'),
    ('Gundam Card Game', 'Gundam Card Game Origins Booster Pack English Live Rip', 'Pokemon Origins Booster Pack English Live Rip'),
    ('Riftbound', 'Riftbound Origins Booster Pack English Live Rip', 'Pokemon Origins Booster Pack English Live Rip'),
])
def test_new_game_defaults_do_not_cross_game_boundaries(game, matching, wrong):
    from app.tiktok import listing_defaults
    product = {'game': game, 'name': 'Origins Booster Pack'}
    fields = {'language': 'English', 'fulfillment_mode': 'rip'}
    assert listing_defaults.comparable(product, fields, {'title': matching, 'status': 'ACTIVATE'})
    assert not listing_defaults.comparable(product, fields, {'title': wrong, 'status': 'ACTIVATE'})


@pytest.mark.parametrize('sku_languages,expected', [(['EN'], 'English'), (['zh-Hans'], 'Simplified Chinese'), (['ZH-HANT'], 'Traditional Chinese'), (['EN', 'JP'], ''), ([''], ''), ([], '')])
def test_new_game_market_language_requires_exact_sku_evidence(sku_languages, expected):
    from app.tiktok import listing_defaults
    product = {'external_id': '123', 'category_id': '80', 'set_id': '9', 'name': 'Test Booster Pack'}
    payloads = [
        {'products': [{'id': 123, 'name': 'Test Booster Pack', 'ext_data': {'Description': 'Contains 12 cards.'}}]},
        {'prices': {}},
        {'products': {'123': {str(i): {'lng': lang} for i, lang in enumerate(sku_languages)}}},
    ]
    with patch.object(listing_defaults.httpx, 'Client') as http, patch('app.inventory.routes._tcgtracking_market_price', return_value=5.0):
        http.return_value.__enter__.return_value.get.side_effect = [httpx.Response(200, json=p, request=httpx.Request('GET', 'https://example.test')) for p in payloads]
        details = listing_defaults.catalog_details(product)
    assert listing_defaults.language(product) == expected
    assert details['description'] == 'Contains 12 cards.'



@pytest.mark.parametrize('query,expected', [
    ('Pokemon Scarlet Violet Paldean Fates Quaquaval ex Premium Collection', 'Paldean Fates Quaquaval ex Premium Collection'),
    ('Pokemon Scarlet Violet Surging Sparks Elite Trainer Box', 'Surging Sparks Elite Trainer Box'),
    ('Pokemon Scarlet & Violet Elite Trainer Box [Koraidon]', 'Scarlet & Violet Elite Trainer Box [Koraidon]'),
    ('Wings of the Captain Booster Box', 'Wings of the Captain Booster Box'),
])
def test_specific_catalog_query(query, expected):
    assert service.product_search_query(query, 'Pokemon') == expected


def test_provider_image_rejection_preserves_photo_and_clears_old_identity(client):
    from openai import BadRequestError
    d = create(client)
    error = BadRequestError('content_policy_violation', response=httpx.Response(400, request=httpx.Request('POST','https://example.test')), body={})
    with patch.object(service, 'has_ai_key', return_value=True), patch.object(service, 'get_ai_client') as ai:
        ai.return_value.with_options.return_value.chat.completions.create.side_effect = error
        response = client.post(f"{BASE}/drafts/{d['id']}/photo", data={'version':d['version']}, files={'file':('photo.png',png(),'image/png')})
    assert response.status_code == 422
    assert 'continue manually' in response.json()['detail']
    saved = client.get(f"{BASE}/drafts/{d['id']}").json()
    assert saved['assets']['photo']
    assert not saved.get('identification')
    assert saved['fields']['product_confirmed'] is False


def create(client):
    result = client.post(BASE + '/drafts', json={})
    assert result.status_code == 200, result.text
    return result.json()


def write(client, draft, suffix, body):
    result = client.post(f"{BASE}/drafts/{draft['id']}/{suffix}", json={'version': draft['version'], **body})
    assert result.status_code == (202 if suffix == 'design' else 200), result.text
    if suffix == 'design':
        return client.get(f"{BASE}/drafts/{draft['id']}").json()
    return result.json()


def prepared(client):
    d = create(client)
    result = client.post(f"{BASE}/drafts/{d['id']}/source", data={'version': d['version']}, files={'file': ('photo.png', png(), 'image/png')})
    assert result.status_code == 200, result.text
    d = result.json()
    fields = dict(fulfillment_mode='sealed', product_name='Destined Rivals Elite Trainer Box', image_heading='DESTINED RIVALS', language='English',
                  title='Destined Rivals Elite Trainer Box — Shipped Sealed', description='One English Elite Trainer Box.',
                  price='59.99', quantity='4', weight='1.5', length='8', width='7', height='5', theme='blue',
                  category_id='123', warehouse_id='456', product_confirmed=True, image_confirmed=True,
                  shipping_confirmed=True, review_confirmed=False, attributes={})
    d = write(client, d, 'save', {'fields': fields})
    with patch('app.tiktok.listing_images.generate', return_value=png()):
        d = write(client, d, 'design', {})
    d = write(client, d, 'save', {'fields': {**d['fields'], 'review_confirmed': True}})
    return d


@pytest.mark.parametrize('role,allowed', [('admin', True), ('manager', True), ('reviewer', False), ('viewer', False), ('employee', False), ('owner', False)])
def test_explicit_role_gate(client, role, allowed):
    r = client.post(BASE + '/drafts', json={}, headers={'x-role': role})
    assert r.status_code == (200 if allowed else 403)


def test_inactive_and_csrf_are_rejected(client):
    assert client.post(BASE + '/drafts', json={}, headers={'x-active': 'false'}).status_code == 403
    assert client.post(BASE + '/drafts', json={}, headers={'X-CSRF-Token': 'bad'}).status_code == 403


def test_flag_and_private_images(client, monkeypatch):
    d = prepared(client)
    url = d['assets']['designed']
    assert client.get(url).status_code == 200
    assert client.get(url, headers={'x-role': 'viewer'}).status_code == 403
    monkeypatch.setenv('TIKTOK_LISTING_ASSISTANT_ENABLED', 'false')
    assert client.get(url).status_code == 404


def test_stale_edit_does_not_overwrite(client):
    d = create(client)
    changed = write(client, d, 'save', {'fields': {'title': 'First edit'}})
    stale = client.post(f"{BASE}/drafts/{d['id']}/save", json={'version': d['version'], 'fields': {'title': 'Stale edit'}})
    assert stale.status_code == 409
    assert client.get(f"{BASE}/drafts/{d['id']}").json()['fields']['title'] == changed['fields']['title']


def test_photo_survives_ai_error(client):
    d = create(client)
    with patch.object(service, 'identify', side_effect=ValueError('AI unavailable')):
        r = client.post(f"{BASE}/drafts/{d['id']}/photo", data={'version': d['version']}, files={'file': ('photo.png', png(), 'image/png')})
    assert r.status_code == 422
    saved = client.get(f"{BASE}/drafts/{d['id']}").json()
    assert client.get(saved['assets']['photo']).status_code == 200


def test_changed_copy_invalidates_review_and_changed_design_invalidates_image(client):
    d = prepared(client)
    assert d['fields']['review_confirmed']
    d = write(client, d, 'save', {'fields': {**d['fields'], 'price': '60.00'}})
    assert not d['fields']['review_confirmed']
    assert 'designed' in d['assets']
    d = write(client, d, 'save', {'fields': {**d['fields'], 'language': 'Japanese'}})
    assert 'designed' not in d['assets']


@pytest.mark.parametrize('key,value', [('price','NaN'), ('price','-1'), ('quantity','1.5'), ('weight','0'), ('length','1.2'), ('review_confirmed',False)])
def test_invalid_financial_or_shipping_details_cannot_submit(client, key, value):
    d = prepared(client)
    d['fields'][key] = value
    # Unit test exact payload boundary independent of UI validation.
    d['assets']['designed'] = 'unused.png'
    with pytest.raises(ValueError):
        service.build_payload(d, 'LISTING', ['uri'])


def test_payload_escapes_copy_and_contains_real_shipping(client):
    d = prepared(client)
    d['fields']['description'] = '<script>alert(1)</script>'
    body = service.build_payload(d, 'AS_DRAFT', ['designed', 'source'])
    assert '<script>' not in body['description']
    assert body['package_weight'] == {'value':'1.5', 'unit':'POUND'}
    assert body['skus'][0]['inventory'] == [{'warehouse_id':'456', 'quantity':4}]
    assert body['save_mode'] == 'AS_DRAFT'
    assert body['category_version'] == 'v2'
    assert body['idempotency_key'] == d['id']
    assert body['skus'][0]['seller_sku'] == 'DGN-LST-' + d['id']


def metadata(*args, **kwargs):
    return {'categories':[{'id':'123'}], 'warehouses':[{'id':'456'}], 'attributes':[]}


@pytest.mark.parametrize('fulfillment_mode', ['sealed', 'rip', 'both'])
def test_submit_once_and_readback(client, fulfillment_mode):
    d = prepared(client)
    if fulfillment_mode != 'sealed':
        d = write(client, d, 'save', {'fields':{**d['fields'], 'fulfillment_mode':fulfillment_mode, 'title':'Test product', 'description':'One English box.', 'quantity':'10'}})
        d = write(client, d, 'save', {'fields':{**d['fields'], 'weight':'1.5', 'length':'8', 'width':'7', 'height':'5', 'shipping_confirmed':True}})
        with patch('app.tiktok.listing_images.generate', return_value=png()):
            d = write(client, d, 'design', {})
        d = write(client, d, 'save', {'fields':{**d['fields'], 'review_confirmed':True}})
    sent = {}
    def call(ctx, path, body=None):
        if path.endswith('/listing_check'):
            return {'check_result':'PASS'}
        if body is not None:
            assert body['main_images'] == [{'uri':'image-uri'}, {'uri':'image-uri'}]
            sent.update(body)
            return {'product_id':'789'}
        return {**sent, 'id':'789', 'status':'DRAFT', 'audit':{'status':'PENDING'}}
    with patch.object(routes, 'context', return_value={'shop_id':'shop', 'shop_cipher':'cipher'}), patch.object(routes, 'shop_fields', side_effect=metadata), patch.object(routes, 'shop_call', side_effect=call) as api, patch('scripts.tiktok_backfill.upload_tiktok_product_image', return_value='image-uri'), patch('scripts.tiktok_backfill.upsert_tiktok_product_row'):
        result = write(client, d, 'submit', {'mode':'AS_DRAFT'})
        assert result['verification'] == 'verified'
        assert len(sent['skus']) == (2 if fulfillment_mode == 'both' else 1)
        repeated = write(client, d, 'submit', {'mode':'AS_DRAFT'})
        assert repeated['product_id'] == '789'
        assert sum(c.args[1] == '/product/202309/products' for c in api.call_args_list) == 1


def test_uncertain_create_is_locked_and_cannot_retry(client):
    d = prepared(client)
    with patch.object(routes, 'validate_listing', return_value={'check_result':'PASS'}), patch.object(routes, 'context', return_value={}), patch.object(routes, 'shop_fields', side_effect=metadata), patch.object(routes, 'shop_call', side_effect=httpx.ReadTimeout('uncertain')), patch('scripts.tiktok_backfill.upload_tiktok_product_image', return_value='uri'):
        r = client.post(f"{BASE}/drafts/{d['id']}/submit", json={'version':d['version'], 'mode':'LISTING'})
        assert r.status_code == 502
        saved = client.get(f"{BASE}/drafts/{d['id']}").json()
        assert saved['status'] == 'unknown'
        retry = client.post(f"{BASE}/drafts/{d['id']}/submit", json={'version':saved['version'], 'mode':'LISTING'})
        assert retry.status_code == 409


def test_definite_rejection_keeps_draft_editable(client):
    d = prepared(client)
    with patch.object(routes, 'context', return_value={}), patch.object(routes, 'shop_fields', side_effect=metadata), patch.object(routes, 'shop_call', side_effect=routes.TikTokRejected('Missing certification')), patch('scripts.tiktok_backfill.upload_tiktok_product_image', return_value='uri'):
        r = client.post(f"{BASE}/drafts/{d['id']}/submit", json={'version':d['version'], 'mode':'LISTING'})
        assert r.status_code == 422
        assert client.get(f"{BASE}/drafts/{d['id']}").json()['status'] == 'draft'


@pytest.mark.parametrize('url', ['http://product-images.tcgplayer.com/a.png', 'https://localhost/a', 'https://169.254.169.254/a', 'https://product-images.tcgplayer.com.evil.test/a', 'https://user:pass@product-images.tcgplayer.com/a'])
def test_remote_image_sources_are_restricted(url):
    with pytest.raises(ValueError):
        service.fetch_product_image(url)


def test_compose_preserves_product_pixels():
    raw = png()
    result = Image.open(io.BytesIO(service.compose(raw, {'image_heading':'TEST PRODUCT', 'language':'English'})))
    assert result.size == (1200,1200)
    assert result.getpixel((600,600)) == (216,53,68)


def test_packaging_validates_and_persists(client):
    r = client.post(BASE + '/packaging', json={'name':'Measured ETB', 'weight':'1.5', 'length':'8', 'width':'7', 'height':'5'})
    assert r.status_code == 200
    assert client.get(BASE + '/packaging').json()['presets'][0]['name'] == 'Measured ETB'
    assert client.post(BASE + '/packaging', json={'name':'bad', 'weight':'NaN'}).status_code == 422


def test_readback_flags_changed_price_and_shipping(client):
    d = prepared(client)
    expected = service.build_payload(d, 'LISTING', ['uri'])
    actual = json.loads(json.dumps(expected))
    actual['skus'][0]['price']['amount'] = '1.00'
    actual['package_weight']['value'] = '0.25'
    assert set(service.verification_issues(expected, actual)) == {'price', 'package_weight'}


def test_image_upload_sends_the_required_main_image_use_case():
    from unittest.mock import MagicMock
    from scripts.tiktok_backfill import upload_tiktok_product_image
    client = MagicMock()
    client.post.return_value.json.return_value = {'code':0, 'data':{'uri':'image-uri'}}
    result = upload_tiktok_product_image(client, base_url='https://example.test', app_key='test', app_secret='test',
                                        access_token='test', shop_id='test', shop_cipher='test', image_data=png())
    assert result == 'image-uri'
    assert client.post.call_args.kwargs['data'] == {'use_case':'MAIN_IMAGE'}
    from urllib.parse import urlsplit, parse_qs
    assert set(parse_qs(urlsplit(client.post.call_args.args[0]).query)) == {'app_key','timestamp','sign'}


@pytest.mark.parametrize('path', ['/product/202309/categories', '/product/202309/categories/123/attributes', '/product/202309/brands'])
def test_shop_metadata_requests_use_v2_categories(path):
    with patch('scripts.tiktok_backfill.build_tiktok_request', return_value=('https://example.test','',{})) as build, patch.object(routes.httpx, 'Client') as client:
        client.return_value.__enter__.return_value.request.return_value.json.return_value = {'code':0,'data':{}}
        routes.shop_call({}, path)
    assert build.call_args.kwargs['extra_query']['category_version'] == 'v2'


def test_live_category_names_and_permission_statuses_are_normalized():
    rows = [dict(id='1',local_name='Trading Cards',is_leaf=False,parent_id='0'),
            dict(id='2',local_name='Boxes and Packs',is_leaf=True,parent_id='1',permission_statuses=['AVAILABLE']),
            dict(id='3',local_name='Restricted',is_leaf=True,parent_id='1',permission_statuses=['INVITE_ONLY'])]
    assert routes.available_categories(rows) == [{'id':'2','name':'Trading Cards > Boxes and Packs','is_leaf':True}]


def test_live_required_attribute_spelling_is_normalized():
    with patch.object(routes,'context',return_value={}), patch.object(routes,'shop_call',side_effect=[{'categories':[]},{'warehouses':[]},{'attributes':[{'id':'1','name':'Language','is_requried':True,'type':'PRODUCT_PROPERTY'}]}]):
        result = routes.shop_fields('123',SimpleNamespace(id=1),None)
    assert result['attributes'][0]['is_required'] is True


def test_image_generation_uses_reference_edit_without_retries():
    import base64
    from app.tiktok import listing_images
    with patch.object(listing_images, 'has_ai_key', return_value=True), patch.object(listing_images, 'get_provider', return_value='nvidia'), patch.object(listing_images, 'get_ai_client') as ai:
        ai.return_value.with_options.return_value.images.edit.return_value = SimpleNamespace(data=[SimpleNamespace(b64_json=base64.b64encode(png()).decode())])
        assert listing_images.generate(png(), {'product_name':'Test'})
        args = ai.return_value.with_options.return_value.images.edit.call_args.kwargs
        assert args['model'] == 'us/openai/openai/eccn-gpt-image-2'
        assert len(args['image']) == 3
        assert ai.return_value.with_options.call_args.kwargs['max_retries'] == 0


def test_failed_regeneration_preserves_previous_image(client):
    d = prepared(client)
    with patch('app.tiktok.listing_images.generate', side_effect=ValueError('Provider blocked generation')):
        response = client.post(f"{BASE}/drafts/{d['id']}/design", json={'version':d['version']})
    assert response.status_code == 202
    saved = client.get(f"{BASE}/drafts/{d['id']}").json()
    assert saved['image_job']['status'] == 'failed'
    assert saved['assets']['designed'] == d['assets']['designed']
    assert saved['version'] == d['version'] + 2


@pytest.mark.parametrize('status,message,expected,calls', [
    (429, 'rate limit', 'image_temporary', 2),
    (503, 'unavailable', 'image_temporary', 2),
    (500, 'moderation_blocked', 'image_blocked', 1),
    (400, 'content_policy_violation', 'image_blocked', 1),
    (429, 'insufficient_quota', 'image_access', 1),
    (401, 'unauthorized', 'image_access', 1),
])
def test_image_retry_classification(status, message, expected, calls):
    import httpx
    from unittest.mock import Mock
    from openai import APIStatusError
    from app.tiktok.listing_images import _edit, ImageGenerationError
    ai = Mock()
    ai.images.edit.side_effect = APIStatusError(message, response=httpx.Response(status, request=httpx.Request('POST', 'https://example.com')), body=None)
    with patch('app.tiktok.listing_images.time.sleep') as delay, pytest.raises(ImageGenerationError) as error:
        _edit(ai, prompt='unchanged')
    assert error.value.code == expected
    assert ai.images.edit.call_count == calls
    assert delay.call_count == calls - 1


def test_image_transient_retry_succeeds_with_same_inputs():
    import httpx
    from unittest.mock import Mock
    from openai import APIStatusError
    from app.tiktok.listing_images import _edit
    ai = Mock()
    ai.images.edit.side_effect = [APIStatusError('busy', response=httpx.Response(503, request=httpx.Request('POST', 'https://example.com')), body=None), 'image']
    with patch('app.tiktok.listing_images.time.sleep'):
        assert _edit(ai, prompt='unchanged') == 'image'
    assert ai.images.edit.call_args_list[0] == ai.images.edit.call_args_list[1]


def test_image_timeout_never_retries():
    import httpx
    from unittest.mock import Mock
    from openai import APITimeoutError
    from app.tiktok.listing_images import _edit, ImageGenerationError
    ai = Mock()
    ai.images.edit.side_effect = APITimeoutError(request=httpx.Request('POST', 'https://example.com'))
    with patch('app.tiktok.listing_images.time.sleep') as delay, pytest.raises(ImageGenerationError) as error:
        _edit(ai)
    assert error.value.code == 'image_timeout'
    assert ai.images.edit.call_count == 1
    delay.assert_not_called()


def test_image_block_returns_recovery_code_and_preserves_draft(client):
    from app.tiktok.listing_images import ImageGenerationError
    d = prepared(client)
    with patch('app.tiktok.listing_images.generate', side_effect=ImageGenerationError('image_blocked', 'Blocked; draft saved.')):
        response = client.post(f"{BASE}/drafts/{d['id']}/design", json={'version': d['version']})
    assert response.status_code == 202
    saved = client.get(f"{BASE}/drafts/{d['id']}").json()
    assert saved['image_job']['code'] == 'image_blocked'
    assert saved['assets'] == d['assets']
    assert saved['version'] == d['version'] + 2


@pytest.mark.parametrize('total,sealed,rip', [(0,0,0),(1,1,0),(9,1,8),(10,1,9),(11,2,9),(100,10,90),(999999,100000,899999)])
def test_default_stock_split_preserves_total(total, sealed, rip):
    assert service.stock_allocation({'fulfillment_mode':'both','quantity':str(total)}) == {'sealed':sealed,'rip':rip}


@pytest.mark.parametrize('sealed', ['-1','11','1.5','abc'])
def test_reject_invalid_stock_split(sealed):
    with pytest.raises(ValueError):
        service.stock_allocation({'fulfillment_mode':'both','quantity':'10','sealed_quantity':sealed})


def test_manual_stock_split_moves_units_without_adding_stock():
    assert service.stock_allocation({'fulfillment_mode':'both','quantity':'10','sealed_quantity':'0'}) == {'sealed':0,'rip':10}


@pytest.mark.parametrize('mode,count', [('sealed',1),('rip',1),('both',2)])
def test_fulfillment_payload(client, mode, count):
    d = prepared(client)
    d['fields'].update(fulfillment_mode=mode, title='Test product', description='One English box.', quantity='10', rip_price='55.00')
    payload = service.build_payload(d, 'AS_DRAFT', ['test-image'])
    assert len(payload['skus']) == count
    assert sum(s['inventory'][0]['quantity'] for s in payload['skus']) == 10
    if mode == 'both':
        assert [s['inventory'][0]['quantity'] for s in payload['skus']] == [1,9]
        assert [s['sales_attributes'][0]['value_name'] for s in payload['skus']] == ['Shipped Sealed','Live Rip']
        assert payload['skus'][1]['price']['amount'] == '55.00'
    if mode != 'sealed':
        assert 'Bulk cards' in payload['description']


def test_switch_fulfillment_invalidates_generated_art(client):
    d = prepared(client)
    d = write(client, d, 'save', {'fields':{**d['fields'],'fulfillment_mode':'both'}})
    assert 'designed' not in d['assets']
    assert not d['fields']['review_confirmed']


@pytest.mark.parametrize('mode,footer', [('sealed','SHIPPED SEALED • UNOPENED'),('rip','LIVE RIP'),('both','CHOOSE SEALED OR LIVE RIP')])
def test_image_prompt_uses_selected_fulfillment(mode, footer):
    import base64
    from app.tiktok import listing_images
    with patch.object(listing_images,'has_ai_key',return_value=True), patch.object(listing_images,'get_ai_client') as ai:
        ai.return_value.with_options.return_value.images.edit.return_value = SimpleNamespace(data=[SimpleNamespace(b64_json=base64.b64encode(png()).decode())])
        listing_images.generate(png(), {'fulfillment_mode':mode})
        prompt = ai.return_value.with_options.return_value.images.edit.call_args.kwargs['prompt']
        assert 'Footer EXACTLY: '+footer+'.' in prompt


def test_readback_detects_wrong_fulfillment_variant(client):
    import copy
    d = prepared(client)
    d['fields'].update(fulfillment_mode='both',title='Test product',quantity='10')
    payload = service.build_payload(d,'AS_DRAFT',['test-image'])
    actual = copy.deepcopy(payload)
    actual['skus'][1]['sales_attributes'][0]['value_name'] = 'Shipped Sealed'
    assert 'fulfillment option' in service.verification_issues(payload,actual)


def test_failed_live_validation_prevents_creation(client):
    d = prepared(client)
    with patch.object(routes,'context',return_value={}), patch.object(routes,'shop_fields',side_effect=metadata), patch('scripts.tiktok_backfill.upload_tiktok_product_image',return_value='uri'), patch.object(routes,'shop_call',return_value={'check_result':'FAILED','fail_reasons':[{'message':'Invalid variants'}]}) as api:
        result = client.post(f"{BASE}/drafts/{d['id']}/submit",json={'version':d['version'],'mode':'AS_DRAFT'})
    assert result.status_code == 422
    assert api.call_count == 1
    assert api.call_args.args[1].endswith('/listing_check')
    assert client.get(f"{BASE}/drafts/{d['id']}").json()['status'] == 'draft'


@pytest.fixture
def stock_case(client):
    import copy
    d = prepared(client)
    d = write(client,d,'save',{'fields':{**d['fields'],'fulfillment_mode':'both','quantity':'10','sealed_quantity':'3','title':'Test product','description':'One box.'}})
    d = write(client,d,'save',{'fields':{**d['fields'],'weight':'1.5','length':'8','width':'7','height':'5','shipping_confirmed':True}})
    with patch('app.tiktok.listing_images.generate',return_value=png()):
        d = write(client,d,'design',{})
    d = write(client,d,'save',{'fields':{**d['fields'],'review_confirmed':True}})
    state = {'writes':0}
    def call(ctx,path,body=None):
        if path.endswith('/listing_check'):
            return {'check_result':'PASS'}
        if path.endswith('/inventory/update'):
            state['writes'] += 1
            if state.get('timeout'):
                raise httpx.ReadTimeout('uncertain')
            if state.get('partial'):
                return {'errors':[{'code':12052990,'message':'One SKU failed'}]}
            for item in body['skus']:
                next(s for s in state['product']['skus'] if s['id']==item['id'])['inventory'] = copy.deepcopy(item['inventory'])
            return {}
        if body is not None:
            state['product'] = {**copy.deepcopy(body),'id':'789','status':'DRAFT'}
            for i,sku in enumerate(state['product']['skus']):
                sku['id'] = str(i+100)
            return {'product_id':'789'}
        return copy.deepcopy(state['product'])
    with patch.object(routes,'context',return_value={'shop_id':'shop','shop_cipher':'cipher'}), patch.object(routes,'shop_fields',side_effect=metadata), patch.object(routes,'shop_call',side_effect=call), patch('scripts.tiktok_backfill.upload_tiktok_product_image',return_value='uri'), patch('scripts.tiktok_backfill.upsert_tiktok_product_row'):
        d = write(client,d,'submit',{'mode':'AS_DRAFT'})
        yield d,state


def test_stock_transfer_verified_once_and_audited(client,stock_case):
    d,state = stock_case
    d = write(client,d,'stock/preview',{'amount':2})
    proposal = d['stock_transfer']
    assert proposal['before']=={'sealed':3,'rip':7}
    assert proposal['after']=={'sealed':1,'rip':9}
    body={'version':d['version'],'transfer_id':proposal['id']}
    result=client.post(f"{BASE}/drafts/{d['id']}/stock/confirm",json=body).json()
    assert result['stock_transfer']['status']=='completed'
    assert client.post(f"{BASE}/drafts/{d['id']}/stock/confirm",json=body).json()['stock_transfer']['status']=='completed'
    assert state['writes']==1
    session_generator=client.app.dependency_overrides[get_session]()
    session=next(session_generator)
    from sqlmodel import select
    logs=session.exec(select(AuditLog).where(AuditLog.action=='tiktok.listing.stock_transfer_completed')).all()
    assert json.loads(logs[0].details_json)['stock_transfer']['after']=={'sealed':1,'rip':9}
    session_generator.close()


def test_stock_change_since_preview_rejects_transfer(client,stock_case):
    d,state=stock_case
    d=write(client,d,'stock/preview',{'amount':1})
    state['product']['skus'][0]['inventory'][0]['quantity']=2
    result=client.post(f"{BASE}/drafts/{d['id']}/stock/confirm",json={'version':d['version'],'transfer_id':d['stock_transfer']['id']})
    assert result.status_code==409
    assert state['writes']==0


def test_active_listing_cannot_transfer(client,stock_case):
    d,state=stock_case
    state['product']['status']='ACTIVATE'
    result=client.post(f"{BASE}/drafts/{d['id']}/stock/preview",json={'version':d['version'],'amount':1})
    assert result.status_code==422
    assert 'Pause' in result.json()['detail']
    assert state['writes']==0


@pytest.mark.parametrize('fault',['timeout','partial'])
def test_uncertain_transfer_locks_retries(client,stock_case,fault):
    d,state=stock_case
    d=write(client,d,'stock/preview',{'amount':1})
    state[fault]=True
    result=write(client,d,'stock/confirm',{'transfer_id':d['stock_transfer']['id']})
    assert result['stock_transfer']['status']=='needs_review'
    retry=client.post(f"{BASE}/drafts/{d['id']}/stock/preview",json={'version':result['version'],'amount':1})
    assert retry.status_code==422
    assert state['writes']==1


def test_expired_transfer_preview_rejected(client,stock_case):
    import time
    d,state=stock_case
    d=write(client,d,'stock/preview',{'amount':1})
    with patch('time.time',return_value=time.time()+180):
        result=client.post(f"{BASE}/drafts/{d['id']}/stock/confirm",json={'version':d['version'],'transfer_id':d['stock_transfer']['id']})
    assert result.status_code==422
    assert state['writes']==0


def test_transfer_insufficient_stock_rejected(client,stock_case):
    d,state=stock_case
    result=client.post(f"{BASE}/drafts/{d['id']}/stock/preview",json={'version':d['version'],'amount':4})
    assert result.status_code==422
    assert state['writes']==0


def test_stock_transfer_preserves_other_warehouses(client,stock_case):
    d,state=stock_case
    for sku in state['product']['skus']:
        sku['inventory'].append({'warehouse_id':'other','quantity':12})
    d=write(client,d,'stock/preview',{'amount':1})
    d=write(client,d,'stock/confirm',{'transfer_id':d['stock_transfer']['id']})
    assert d['stock_transfer']['status']=='completed'
    assert [s['inventory'][1]['quantity'] for s in state['product']['skus']]==[12,12]


def test_unknown_transfer_can_be_reconciled_without_repeating_write(client,stock_case):
    d,state=stock_case
    d=write(client,d,'stock/preview',{'amount':1})
    state['timeout']=True
    d=write(client,d,'stock/confirm',{'transfer_id':d['stock_transfer']['id']})
    state['product']['skus'][0]['inventory'][0]['quantity']=2
    state['product']['skus'][1]['inventory'][0]['quantity']=8
    d=write(client,d,'stock/reconcile',{})
    assert d['stock_transfer']['status']=='completed'
    assert state['writes']==1


def test_stock_transfer_role_gate(client,stock_case):
    d,state=stock_case
    response=client.post(f"{BASE}/drafts/{d['id']}/stock/preview",json={'version':d['version'],'amount':1},headers={'x-role':'viewer'})
    assert response.status_code==403
    assert state['writes']==0


def test_real_tiktok_readback_category_and_price_shape(client):
    import copy
    d=prepared(client)
    expected=service.build_payload(d,'AS_DRAFT',['uri'])
    actual=copy.deepcopy(expected)
    actual['category_chains']=[{'id':actual.pop('category_id'),'is_leaf':True}]
    for sku in actual['skus']:
        sku['price']['tax_exclusive_price']=sku['price'].pop('amount')
    assert not service.verification_issues(expected,actual)


def test_original_photo_fallback_preserves_pixels_and_requires_review(client):
    d=prepared(client)
    d=write(client,d,'use-source-image',{})
    assert d['image_generator']=='original-photo'
    assert not d['fields']['review_confirmed']
    assert client.get(d['assets']['source']).content == client.get(d['assets']['designed']).content


def test_original_photo_fallback_requires_confirmed_source(client):
    d=create(client)
    response=client.post(f"{BASE}/drafts/{d['id']}/use-source-image",json={'version':d['version']})
    assert response.status_code==422


def test_readback_allows_tiktok_ampersand_normalization_but_not_content_changes(client):
    import copy
    d=prepared(client)
    d['fields']['description']='Scarlet & Violet. One pack.'
    expected=service.build_payload(d,'AS_DRAFT',['uri'])
    actual=copy.deepcopy(expected)
    actual['description']=actual['description'].replace('&amp;','&')
    assert not service.verification_issues(expected,actual)
    actual['description']=actual['description'].replace('One pack.','Two packs.')
    assert 'description' in service.verification_issues(expected,actual)


@pytest.mark.parametrize('endpoint', ['preview', 'confirm'])
def test_stock_mutations_disabled_by_default(client, stock_case, monkeypatch, endpoint):
    d, state = stock_case
    monkeypatch.delenv('TIKTOK_LISTING_STOCK_TRANSFERS_ENABLED', raising=False)
    with patch.object(routes, 'shop_call') as call:
        result = client.post(f"{BASE}/drafts/{d['id']}/stock/{endpoint}", json={'version': d['version'], 'amount': 1})
    assert result.status_code == 403
    call.assert_not_called()


def test_style_references_exist():
    from app.tiktok.listing_images import STYLE, LOGO
    for path in (STYLE, LOGO):
        with Image.open(path) as picture:
            assert min(picture.size) >= 100


def test_running_image_job_blocks_duplicate_generation_and_edits(client):
    from starlette.background import BackgroundTasks
    d = prepared(client)
    with patch.object(BackgroundTasks, 'add_task') as enqueue:
        result = client.post(f"{BASE}/drafts/{d['id']}/design", json={'version': d['version']})
        assert result.status_code == 202
        running = result.json()
        assert running['image_job']['status'] == 'running'
        duplicate = client.post(f"{BASE}/drafts/{d['id']}/design", json={'version': running['version']})
        assert duplicate.status_code == 409
        edit = client.post(f"{BASE}/drafts/{d['id']}/save", json={'version': running['version'], 'fields': {'title':'Changed'}})
        assert edit.status_code == 409
        assert enqueue.call_count == 1


def test_interrupted_image_job_expires_visibly_without_retry(client):
    from starlette.background import BackgroundTasks
    d = prepared(client)
    with patch.object(BackgroundTasks, 'add_task'):
        running = client.post(f"{BASE}/drafts/{d['id']}/design", json={'version': d['version']}).json()
    with patch('app.tiktok.listing_jobs.time.time', return_value=running['image_job']['started_at'] + 601), patch('app.tiktok.listing_images.generate') as generate:
        saved = client.get(f"{BASE}/drafts/{d['id']}").json()
    assert saved['image_job']['status'] == 'failed'
    assert saved['image_job']['code'] == 'image_timeout'
    assert saved['assets'] == d['assets']
    generate.assert_not_called()
    client.headers['X-CSRF-Token'] = client.get('/csrf').json()['token']
    assert write(client, saved, 'save', {'fields': {'title':'Editable again'}})['fields']['title'] == 'Editable again'


# Autofill and revision regressions: exact catalog identity, safe defaults and recovery.
from app.tiktok import listing_defaults as defaults


def catalog_product(**overrides):
    return {"name":"Destined Rivals Booster Box", "set_name":"SV10: Destined Rivals", "set_id":"24269",
            "kind":"Booster Box", "game":"Pokemon", "category_id":"3", "external_id":"624679",
            "image_url":"https://cdn.tcgtracking.com/product/624679_400w.jpg", "source_name":"TCGTracking",
            "market_price":434.63, "market_price_source":"TCGPlayer Market", "price_checked_at":"2026-09-10T12:00:00+00:00", **overrides}


def shop_template(**overrides):
    return {"id":"987", "title":"Pokemon TCG Evolving Skies Booster Box English Live Rip Only", "status":"ACTIVATE",
            "category_chains":[{"id":"123","is_leaf":True}], "brand":{"id":"789"},
            "product_attributes":[{"id":"type","name":"Type","values":[{"name":"Box"}]}],
            "package_weight":{"value":"0.1","unit":"POUND"},
            "package_dimensions":{"length":"6","width":"6","height":"1","unit":"INCH"},
            "skus":[{"inventory":[{"warehouse_id":"456","quantity":10}]}], **overrides}


def autofill_metadata(*a, **kw):
    return {"categories":[{"id":"123","name":"Boxes and Packs"}],"warehouses":[{"id":"456","name":"Main"}],
            "attributes":[{"id":"language","name":"Language","is_required":True}]}


def selected(client, product=None):
    from unittest.mock import AsyncMock
    d=create(client)
    with patch('app.inventory.routes._search_sealed_products', new=AsyncMock(return_value=([product or catalog_product()], ''))):
        d=write(client,d,'search',{'query':'Destined Rivals Booster Box','game':'Pokemon'})
    with patch.object(service,'fetch_product_image',return_value=png()):
        return write(client,d,'select',{'index':0})


def fill(client, d, template=None):
    template=template or shop_template()
    gen=client.app.dependency_overrides[get_session]()
    session=next(gen)
    existing=session.exec(select(TikTokProduct).where(TikTokProduct.tiktok_product_id==template['id'])).first()
    if not existing:
        session.add(TikTokProduct(tiktok_product_id=template['id'],title=template['title'],status=template['status'],raw_payload=json.dumps(template)))
        session.commit()
    gen.close()
    with patch.object(defaults,'catalog_details',return_value={'description':'Contains 36 booster packs.','language_options':['English','French']}), patch.object(routes,'context',return_value={}), patch.object(routes,'shop_call',return_value=template), patch.object(routes,'shop_fields',side_effect=autofill_metadata):
        return write(client,d,'autofill',{})


def test_new_drafts_default_to_live_rip_but_old_drafts_keep_sealed(client):
    assert create(client)['fields']['fulfillment_mode']=='rip'
    assert service.fulfillment({})=='sealed'


@pytest.mark.parametrize('category,expected', [('3','English'),('85','Japanese'),('1','')])
def test_language_uses_catalog_identity(category,expected):
    assert defaults.language(catalog_product(category_id=category))==expected


def test_autofill_populates_market_copy_and_shop_settings(client):
    d=fill(client,selected(client))
    f=d['fields']
    assert f['fulfillment_mode']=='rip' and f['language']=='English'
    assert f['price']=='434.63' and f['category_id']=='123' and f['warehouse_id']=='456'
    assert f['weight']=='0.1' and f['attributes']['language']=='English'
    assert '36 booster packs' in f['description'] and 'Evolving Skies' not in f['description']
    assert d['missing_defaults']==['Quantity']
    assert d['defaults']['sources']['shop']['id']=='987'
    assert d['defaults']['sources']['price']['language']=='English'
    assert d['fields']['review_confirmed'] is False


def test_new_catalog_language_is_available_on_first_shop_default_lookup(client):
    d = selected(client, catalog_product(category_id='80', game='Dragon Ball Super: Fusion World'))
    assert not d['fields']['language']

    def refresh(product):
        product['language'] = 'English'
        return {'language_options': ['English']}

    with patch.object(defaults, 'catalog_details', side_effect=refresh), patch.object(
        defaults, 'rank_listings', return_value=[]
    ) as rank, patch.object(routes, 'context', return_value={}), patch.object(
        routes, 'shop_fields', side_effect=autofill_metadata
    ):
        result = write(client, d, 'autofill', {})
    assert rank.call_args.args[1]['language'] == 'English'
    assert result['fields']['language'] == 'English'


def test_autofill_refresh_preserves_staff_overrides(client):
    d=fill(client,selected(client))
    d=write(client,d,'save',{'fields':{**d['fields'],'price':'449.00','description':'Our edited product facts.','weight':'0.4','quantity':'10'}})
    d=fill(client,d)
    assert d['fields']['price']=='449.00' and d['fields']['weight']=='0.4'
    assert d['fields']['description']=='Our edited product facts.'
    assert d['missing_defaults']==[]


@pytest.mark.parametrize('name,lang,mode', [
    ('Pokemon TCG Evolving Skies Booster Pack English Live Rip Only','English','rip'),
    ('Pokemon TCG Evolving Skies Half Booster Box English Live Rip Only','English','rip'),
    ('Pokemon TCG Evolving Skies Booster Box Case English Live Rip Only','English','rip'),
    ('Pokemon TCG Black Bolt Booster Box Japanese Live Rip Only','English','rip'),
    ('Pokemon TCG Evolving Skies Booster Box English Shipped Sealed','English','rip'),
    ('Pokemon TCG Evolving Skies Booster Box English Live Rip Only','English','both'),
])
def test_incompatible_listings_never_supply_packaging(name,lang,mode):
    product=catalog_product()
    f={'language':lang,'fulfillment_mode':mode}
    template=shop_template(title=name)
    assert defaults.rank_listings(product,f,[template])==[]
    values,_=defaults.suggestions(product,f,{},template,autofill_metadata())
    assert 'weight' not in values and 'category_id' not in values


@pytest.mark.parametrize('source,price', [('TCGPlayer Low',12),('TCGPlayer Market',None),('TCGPlayer Market',0),('TCGPlayer Market','NaN')])
def test_missing_or_non_market_price_is_never_defaulted(source,price):
    values,sources=defaults.suggestions(catalog_product(market_price=price,market_price_source=source),{'language':'English','fulfillment_mode':'rip'},{},None,autofill_metadata())
    assert values['price']=='' and 'price' not in sources


def test_language_change_invalidates_price_photo_and_package(client):
    d=fill(client,selected(client))
    d=write(client,d,'save',{'fields':{**d['fields'],'language':'French'}})
    assert d['fields']['price']=='' and d['fields']['weight']==''
    assert not d['assets'].get('source') and not d['fields']['image_confirmed']
    assert 'price' not in d['defaults']['sources']
    assert 'French' in d['fields']['title'] and 'Language: French' in d['fields']['description']


def test_new_product_clears_stale_details_and_art(client):
    d=fill(client,selected(client))
    with patch.object(service,'fetch_product_image',return_value=png()):
        d=write(client,d,'select',{'index':0})
    assert not d.get('defaults') and not d['fields'].get('price')
    assert not d.get('language_options') and not d.get('missing_defaults')
    assert not d['fields'].get('category_id') and not d['fields'].get('weight')


def test_autofill_unavailable_preserves_draft_and_exposes_missing_fields(client):
    d=selected(client)
    with patch.object(defaults,'catalog_details',side_effect=httpx.ReadTimeout('unavailable')), patch.object(routes,'context',side_effect=ValueError('not connected')):
        d=write(client,d,'autofill',{})
    assert d['assets']['source'] and d['defaults']['warning']
    assert 'Category' in d['missing_defaults'] and 'Packed weight' in d['missing_defaults']


def test_catalog_lookup_returns_actual_supported_languages_and_contents():
    from unittest.mock import MagicMock
    raw={'id':624679,'name':'Destined Rivals Booster Box','ext_data':{'CardText':'36 packs.<script>bad()</script>'},
         'cardtrader':[{'tcg_player_id':624679,'match_confidence':100,'languages':['en','fr','de']},
                      {'tcg_player_id':999,'match_confidence':100,'languages':['ja']}]}
    responses=[]
    for data in ({'products':[raw]}, {'prices':{'624679':{'tcg':{'Normal':{'market':435.00}}}}}):
        r=MagicMock();r.json.return_value=data;responses.append(r)
    product=catalog_product()
    with patch.object(defaults.httpx,'Client') as http:
        http.return_value.__enter__.return_value.get.side_effect=responses
        result=defaults.catalog_details(product)
    assert result['language_options']==['English','French','German']
    assert result['description']=='36 packs.' and float(product['market_price'])==435


def test_image_revision_keeps_listing_fields_and_previous_version(client):
    d=prepared(client)
    before=dict(d['fields']);old_url=d['assets']['designed']
    with patch('app.tiktok.listing_images.generate',return_value=png()) as image_model:
        d=write(client,d,'design',{'revision':'Use green lighting and make the product larger.'})
    assert {**d['fields'],'review_confirmed':True}==before
    assert image_model.call_args.kwargs['revision'].startswith('Use green')
    assert image_model.call_args.kwargs['current']==png()
    assert len(d['image_history'])==1 and client.get(old_url).status_code==200
    key=d['image_history'][0]['key'];expected=d['assets'][key].split('?v=')[1]
    d=write(client,d,'restore-image',{'key':key})
    assert d['assets']['designed'].endswith(expected) and d['fields']['review_confirmed'] is False
    assert d['fields']['price']==before['price']


def test_blocked_revision_preserves_current_image_without_auto_retry(client):
    from app.tiktok.listing_images import ImageGenerationError
    d=prepared(client);old=d['assets']['designed'];fields=dict(d['fields'])
    with patch('app.tiktok.listing_images.generate',side_effect=ImageGenerationError('image_blocked','Blocked; previous image saved.')) as model:
        d=write(client,d,'design',{'revision':'Change the background'})
    assert model.call_count==1 and d['image_job']['status']=='failed'
    assert d['assets']['designed']==old and d['fields']==fields


def test_revision_length_and_cross_draft_restore_are_rejected(client):
    d=prepared(client)
    for suffix,body in [('design',{'revision':'x'*1501}),('restore-image',{'key':'history_other'})]:
        r=client.post(f"{BASE}/drafts/{d['id']}/{suffix}",json={'version':d['version'],**body})
        assert r.status_code==422


def test_image_revision_prompt_uses_four_references_and_ignores_stage_color():
    import base64
    from app.tiktok import listing_images
    with patch.object(listing_images,'has_ai_key',return_value=True), patch.object(listing_images,'get_ai_client') as ai:
        ai.return_value.with_options.return_value.images.edit.return_value=SimpleNamespace(data=[SimpleNamespace(b64_json=base64.b64encode(png()).decode())])
        listing_images.generate(png(),{'fulfillment_mode':'rip','theme':'hot-pink-manual'},current=png(),revision='Softer green lighting')
        request=ai.return_value.with_options.return_value.images.edit.call_args.kwargs
    assert len(request['image'])==4 and 'Softer green lighting' in request['prompt']
    assert 'hot-pink-manual' not in request['prompt'] and 'Footer EXACTLY: LIVE RIP' in request['prompt']


def test_pack_multipacks_do_not_inherit_single_pack_settings():
    assert defaults.unit_key('Destined Rivals Booster Pack Art Bundle [Set of 4]') != defaults.unit_key('Black Bolt Booster Pack')
    assert defaults.unit_key('Destined Rivals 3 Pack Blister') != defaults.unit_key('Black Bolt Booster Pack')


def test_source_replacement_removes_incompatible_restore_versions(client):
    d=prepared(client)
    with patch('app.tiktok.listing_images.generate',return_value=png()):
        d=write(client,d,'design',{'revision':'Darker background'})
    old_key=d['image_history'][0]['key']
    r=client.post(f"{BASE}/drafts/{d['id']}/source",data={'version':d['version']},files={'file':('new.png',png(),'image/png')})
    assert r.status_code==200
    d=r.json()
    assert not d.get('image_history') and old_key not in d['assets']
    r=client.post(f"{BASE}/drafts/{d['id']}/restore-image",json={'version':d['version'],'key':old_key})
    assert r.status_code==422


def test_catalog_identity_manual_edit_clears_price_package_and_source(client):
    d=fill(client,selected(client))
    d=write(client,d,'save',{'fields':{**d['fields'],'product_name':'Destined Rivals Booster Pack'}})
    assert not d['fields']['price'] and not d['fields']['weight'] and not d['assets'].get('source')
    assert not d.get('defaults')


def test_language_attribute_cannot_contradict_product_edition(client):
    d=fill(client,selected(client))
    d=write(client,d,'save',{'fields':{**d['fields'],'attributes':{'language':'Japanese'}}})
    assert d['fields']['language']=='English' and d['fields']['attributes']['language']=='English'


def test_unavailable_catalog_language_requires_another_product(client):
    d=fill(client,selected(client))
    r=client.post(f"{BASE}/drafts/{d['id']}/save",json={'version':d['version'],'fields':{**d['fields'],'language':'Japanese'}})
    assert r.status_code==422 and 'available language' in r.json()['detail']
    unchanged=client.get(f"{BASE}/drafts/{d['id']}").json()
    assert unchanged['fields']['language']=='English' and unchanged['assets']['source']

@pytest.mark.parametrize('key,value', [
    ('quantity','1000000'),('quantity','-1'),('quantity','1.5'),('quantity','1e2'),
    ('price','0'),('price','NaN'),('price','1000000'),('price','1.001'),
    ('weight','0'),('weight','151'),('weight','1.0001'),
    ('length','0'),('length','121'),('length','1.5'),('width','-1'),('height','Infinity'),
])
def test_invalid_numbers_never_start_paid_image_job(client,key,value):
    from starlette.background import BackgroundTasks
    d=prepared(client)
    d=write(client,d,'save',{'fields':{**d['fields'],key:value}})
    with patch.object(BackgroundTasks,'add_task') as enqueue:
        r=client.post(f"{BASE}/drafts/{d['id']}/design",json={'version':d['version']})
    assert r.status_code==422,r.text
    enqueue.assert_not_called()


@pytest.mark.parametrize('total,sealed,rip', [(0,0,0),(1,1,0),(2,1,1),(9,1,8),(10,1,9),(11,2,9),(99,10,89),(100,10,90),(999999,100000,899999)])
def test_both_allocation_conserves_total_at_boundaries(total,sealed,rip):
    assert service.stock_allocation({'fulfillment_mode':'both','quantity':str(total)})=={'sealed':sealed,'rip':rip}


@pytest.mark.parametrize('key,value',[('sealed_quantity','11'),('sealed_quantity','-1'),('sealed_quantity','1.5'),('rip_price','-1'),('rip_price','NaN')])
def test_invalid_variant_allocation_and_price_block_image_job(client,key,value):
    from starlette.background import BackgroundTasks
    d=prepared(client)
    f={**d['fields'],'fulfillment_mode':'both','quantity':'10',key:value}
    d=write(client,d,'save',{'fields':f})
    # Changing fulfillment deliberately invalidates packed dimensions.
    d=write(client,d,'save',{'fields':{**d['fields'],'weight':'2','length':'8','width':'6','height':'4'}})
    with patch.object(BackgroundTasks,'add_task') as enqueue:
        r=client.post(f"{BASE}/drafts/{d['id']}/design",json={'version':d['version']})
    assert r.status_code==422
    enqueue.assert_not_called()


def test_repeated_language_changes_update_generated_copy_and_both_prices(client):
    d=fill(client,selected(client))
    d=write(client,d,'save',{'fields':{**d['fields'],'rip_price':'400'}})
    d=write(client,d,'save',{'fields':{**d['fields'],'language':'French'}})
    assert d['fields']['rip_price']==''
    d=write(client,d,'save',{'fields':{**d['fields'],'language':'English'}})
    assert ' — English — ' in d['fields']['title']
    assert 'Language: English' in d['fields']['description']
    assert 'French' not in d['fields']['title']


def test_unknown_catalog_language_does_not_default_market_price():
    values,sources=defaults.suggestions(catalog_product(category_id='1',game='Magic'),{'fulfillment_mode':'rip'},{},None,autofill_metadata())
    assert values['language']=='' and values['price']=='' and 'price' not in sources


def test_first_manual_language_updates_generated_copy(client):
    d=fill(client,selected(client,catalog_product(category_id='1',game='Magic')))
    d=write(client,d,'save',{'fields':{**d['fields'],'language':'English'}})
    assert ' — English — ' in d['fields']['title'] and 'Language: English' in d['fields']['description']


def test_transient_lookup_failure_does_not_freeze_generated_packaging():
    d={'fields':{'weight':'0.1'},'defaults':{'values':{'weight':'0.1'}}}
    defaults.apply_defaults(d,{}, {},{},'Unavailable')
    defaults.apply_defaults(d,{'weight':'0.4'}, {},{})
    assert d['fields']['weight']=='0.4'
    d['fields']['weight']='0.8'
    defaults.apply_defaults(d,{}, {},{},'Unavailable')
    defaults.apply_defaults(d,{'weight':'0.6'}, {},{})
    assert d['fields']['weight']=='0.8'


def test_template_does_not_copy_neighboring_set_or_character_attributes():
    attrs=[{'id':'type','name':'Type'},{'id':'set','name':'Set'},{'id':'character','name':'Character'}]
    template=shop_template(product_attributes=[{**a,'values':[{'name':v}]} for a,v in zip(attrs,['Box','Evolving Skies','Rayquaza'])])
    meta={**autofill_metadata(),'attributes':attrs}
    values,_=defaults.suggestions(catalog_product(),{'language':'English','fulfillment_mode':'rip'},{},template,meta)
    assert values['attributes']=={'type':'Box'}


def test_manual_category_does_not_inherit_template_attributes_or_brand():
    meta={**autofill_metadata(),'category_id':'other','categories':[{'id':'123'},{'id':'other'}]}
    values,_=defaults.suggestions(catalog_product(),{'language':'English','fulfillment_mode':'rip','category_id':'other'},{},shop_template(),meta)
    assert 'category_id' not in values and 'brand_id' not in values
    assert values['attributes']=={'language':'English'}


def test_catalog_contents_excludes_unrelated_marketing_introduction():
    raw='<p>A Growing Storm of Stellar Strength!</p><p>Surging Sparks expansion!</p><p>Each Destined Rivals case includes 10 elite trainer boxes.</p><p>Each box contains 9 booster packs.</p>'
    text=defaults.product_contents(raw)
    assert text=='Each Destined Rivals case includes 10 elite trainer boxes.\nEach box contains 9 booster packs.'
    assert defaults.product_contents('36 packs.<script>bad()</script>')=='36 packs.'

@pytest.mark.parametrize('box,pack,game',[
    ('Modern Horizons 3 Collector Booster Box','Modern Horizons 3 Collector Booster Pack','Magic'),
    ('Modern Horizons 3 Collector Booster Display','Modern Horizons 3 Collector Booster Pack','Magic'),
    ('Destined Rivals Three Pack Blister','Destined Rivals Single Pack Blister','Pokemon'),
    ('Destined Rivals 3 Pack Blister','Destined Rivals Single Pack Blister','Pokemon'),
    ('Destined Rivals Booster Box Case','Destined Rivals Booster Box','Pokemon'),
    ('Prismatic Evolutions Mini Tin','Prismatic Evolutions Tin','Pokemon'),
])
def test_distinct_units_never_share_shipping(box,pack,game):
    p=catalog_product(name=box,game=game,language='English')
    template=shop_template(title=f'{game} {pack} English Shipped Sealed')
    assert not defaults.comparable(p,{'language':'English','fulfillment_mode':'sealed'},template)


def test_unapplied_suggestion_never_becomes_staff_override_baseline():
    d={'fields':{'price':'434.63'},'defaults':{'values':{'price':'434.63'}}}
    d['fields']['price']='450'
    defaults.apply_defaults(d,{'price':'450'}, {},{})
    defaults.apply_defaults(d,{'price':'475'}, {},{})
    assert d['fields']['price']=='450'
    assert d['defaults']['values']['price']=='434.63'


def test_changed_category_metadata_survives_save_and_reload(client):
    d=fill(client,selected(client))
    d=write(client,d,'save',{'fields':{**d['fields'],'category_id':'222'}})
    assert not d.get('shop_metadata') and not d['fields']['attributes']
    meta={'categories':[{'id':'123','name':'Old'},{'id':'222','name':'New'}],
          'warehouses':[{'id':'456','name':'Main'}],
          'attributes':[{'id':'new_attr','name':'New required field','is_required':True}]}
    with patch.object(routes,'shop_fields',return_value=meta):
        d=write(client,d,'shop-fields',{})
    d=write(client,d,'save',{'fields':{**d['fields'],'attributes':{'new_attr':'Filled'},'quantity':'5'}})
    loaded=client.get(f"{BASE}/drafts/{d['id']}").json()
    assert loaded['shop_metadata']['category_id']=='222'
    assert loaded['shop_metadata']['attributes'][0]['id']=='new_attr'
    assert loaded['fields']['attributes']=={'new_attr':'Filled'}
    assert not loaded['missing_defaults']


@pytest.mark.parametrize('endpoint',['autofill','shop-fields','restore-image','design'])
@pytest.mark.parametrize('role',['owner','reviewer','viewer','employee'])
def test_new_mutations_reject_non_admin_manager(client,endpoint,role):
    d=create(client)
    client.headers['x-role']=role
    r=client.post(f"{BASE}/drafts/{d['id']}/{endpoint}",json={'version':d['version']})
    assert r.status_code==403

@pytest.mark.parametrize('large,small',[
 ('Three Booster Pack Blister','Single Booster Pack Blister'),
 ('3 Booster Pack Blister','1 Booster Pack Blister'),
 ('Thirty Six Pack Bundle','Six Pack Bundle'),
 ('Thirty-Six Pack Bundle','Six Pack Bundle'),
 ('2 Booster Box','1 Booster Box'),
 ('2x Booster Box','1x Booster Box'),
 ('Two Box Bundle','One Box Bundle'),
 ('2× Booster Box','1× Booster Box'),
])
def test_complete_multipack_count_is_part_of_shipping_identity(large,small):
    p=catalog_product(name='Destined Rivals '+large)
    t=shop_template(title='Pokemon Destined Rivals '+small+' English Live Rip Only')
    assert not defaults.comparable(p,{'language':'English','fulfillment_mode':'rip'},t)


def test_failed_refresh_retains_known_unanswered_category_requirements(client):
    from starlette.background import BackgroundTasks
    d=fill(client,selected(client))
    metadata={**autofill_metadata(),'attributes':[{'id':'required','name':'Required shop fact','is_required':True}]}
    with patch.object(routes,'shop_fields',return_value=metadata):
        d=write(client,d,'shop-fields',{})
    d=write(client,d,'save',{'fields':{**d['fields'],'quantity':'10'}})
    with patch.object(defaults,'catalog_details',return_value={}),patch.object(routes,'context',side_effect=ValueError('Unavailable')):
        d=write(client,d,'autofill',{})
    assert 'Required shop fact' in d['missing_defaults']
    with patch.object(BackgroundTasks,'add_task') as enqueue:
        r=client.post(f"{BASE}/drafts/{d['id']}/design",json={'version':d['version']})
    assert r.status_code==422 and 'Required shop fact' in r.text
    enqueue.assert_not_called()


def test_new_edition_manual_price_equal_to_old_default_survives_refresh(client):
    d=fill(client,selected(client))
    d=write(client,d,'save',{'fields':{**d['fields'],'language':'French'}})
    d=write(client,d,'save',{'fields':{**d['fields'],'price':'434.63'}})
    d=fill(client,d)
    assert d['fields']['language']=='French' and d['fields']['price']=='434.63'

@pytest.mark.parametrize('mode', ['rip', 'sealed', 'both'])
def test_dual_option_source_supplies_shared_defaults_and_matching_warehouse(mode):
    source = shop_template(title='Pokemon TCG Evolving Skies Booster Box English Live Rip / Ship Sealed')
    source['skus'] = [
        {'sales_attributes':[{'name':'Order option','value_name':'Live Rip'}],
         'inventory':[{'warehouse_id':'rip-wh','quantity':0}]},
        {'sales_attributes':[{'name':'Order option','value_name':'Ship Sealed'}],
         'inventory':[{'warehouse_id':'sealed-wh','quantity':10}]},
    ]
    metadata = autofill_metadata()
    metadata['warehouses'] = [{'id':'rip-wh'}, {'id':'sealed-wh'}]
    values, _ = defaults.suggestions(catalog_product(), {'language':'English','fulfillment_mode':mode}, {}, source, metadata)
    assert values['category_id'] == '123'
    assert values['weight'] == '0.1'
    if mode != 'both':
        assert values['warehouse_id'] == mode + '-wh'


def test_dual_source_unknown_variant_does_not_take_other_variant_warehouse():
    source = shop_template(title='Pokemon TCG Evolving Skies Booster Box English Live Rip / Ship Sealed')
    source['skus'] = [{'sales_attributes':[{'name':'Order option','value_name':'Ship Sealed'}],
                       'inventory':[{'warehouse_id':'456','quantity':10}]}]
    metadata = autofill_metadata()
    metadata['warehouses'].append({'id':'789'})
    values, _ = defaults.suggestions(catalog_product(), {'language':'English','fulfillment_mode':'rip'}, {}, source, metadata)
    assert 'warehouse_id' not in values


@pytest.mark.parametrize('first_result', ['incompatible', 'unavailable'])
def test_autofill_verifies_next_candidate_when_cached_first_is_stale(client, first_result):
    d = selected(client)
    gen = client.app.dependency_overrides[get_session]()
    session = next(gen)
    for ident in ('111', '222'):
        template = shop_template()
        session.add(TikTokProduct(tiktok_product_id=ident, title=template['title'], status=template['status'], raw_payload=json.dumps(template)))
    session.commit()
    gen.close()
    fresh = shop_template(title='Pokemon TCG Evolving Skies Booster Box English Live Rip / Ship Sealed')
    fresh['id'] = '222'
    fresh['skus'][0]['sales_attributes'] = [{'name':'Order option','value_name':'Live Rip'}]
    bad = shop_template(title='Pokemon TCG Evolving Skies Booster Pack English Live Rip')
    first = ValueError('Source unavailable') if first_result == 'unavailable' else bad
    with patch.object(defaults,'catalog_details',return_value={'language_options':['English']}), patch.object(routes,'context',return_value={}), patch.object(routes,'shop_call',side_effect=[first,fresh]) as lookup, patch.object(routes,'shop_fields',side_effect=autofill_metadata):
        result = write(client,d,'autofill',{})
    assert lookup.call_count == 2
    assert result['defaults']['sources']['shop']['id'] == '222'
    assert result['fields']['category_id'] == '123'
    assert result['fields']['warehouse_id'] == '456'
    assert result['missing_defaults'] == ['Quantity']

@pytest.mark.parametrize('suffix', ['Shipped Sealed', 'Live Rip / Ship Sealed', ''])
def test_stale_cached_modes_can_be_shortlisted_but_require_live_verification(suffix):
    source = shop_template(title='Pokemon TCG Evolving Skies Booster Box English ' + suffix)
    fields = {'language':'English','fulfillment_mode':'rip'}
    assert defaults.rank_listings(catalog_product(), fields, [source], allow_stale_mode=True) == [source]
    assert defaults.comparable(catalog_product(), fields, source) == ('Live Rip' in suffix)


@pytest.mark.parametrize('name', [
    'Pokemon TCG Evolving Skies Booster Pack English Live Rip / Ship Sealed',
    'Pokemon TCG Black Bolt Booster Box Japanese Live Rip / Ship Sealed',
    'Pokemon TCG Evolving Skies Booster Box Case English Live Rip / Ship Sealed',
])
def test_dual_sources_still_require_same_language_and_product_unit(name):
    assert not defaults.comparable(catalog_product(), {'language':'English','fulfillment_mode':'rip'}, shop_template(title=name))


@pytest.mark.parametrize('revision', ['', 'Softer lighting'])
@pytest.mark.parametrize('mode,footer', [('rip','LIVE RIP'), ('sealed','SHIPPED SEALED • UNOPENED'), ('both','CHOOSE SEALED OR LIVE RIP')])
def test_full_logo_and_white_flare_references_are_authoritative(mode, footer, revision):
    import base64
    import hashlib
    from app.tiktok import listing_images
    raw = png()
    with patch.object(listing_images, 'has_ai_key', return_value=True), patch.object(listing_images, 'get_ai_client') as ai:
        ai.return_value.with_options.return_value.images.edit.return_value = SimpleNamespace(data=[SimpleNamespace(b64_json=base64.b64encode(raw).decode())])
        listing_images.generate(raw, {'fulfillment_mode': mode, 'language': 'English'}, current=raw, revision=revision)
        request = ai.return_value.with_options.return_value.images.edit.call_args.kwargs
    assert request['image'][0][1] == raw
    assert request['image'][1] == ('style.jpg', listing_images.STYLE.read_bytes(), 'image/jpeg')
    assert hashlib.sha256(request['image'][2][1]).hexdigest() == 'f0420343ae82811db997d2a17b2ec020f75a450bb913fcd21f87070567ee4ebb'
    assert len(request['image']) == (4 if revision else 3)
    prompt = request['prompt']
    for text in ['Charizard, Umbreon, Gengar', '17-20%', 'Do not redraw', 'Never invent side panels', 'Keep the original viewing angle', 'Footer EXACTLY: ' + footer]:
        assert text in prompt
    if revision:
        assert 'Reference 4 never overrides the full-logo requirement' in prompt
    ai.return_value.with_options.assert_called_once_with(timeout=240, max_retries=0)


@pytest.mark.parametrize('price,mode', [('434.63','custom'), ('450.00','custom'), ('','custom'), ('456.36','5'), ('434.63','0')])
def test_price_choice_survives_market_refresh_save_and_reload(client, price, mode):
    d = fill(client, selected(client))
    assert d['fields']['price_mode'] == '0'
    d = write(client, d, 'save', {'fields': {**d['fields'], 'price': price, 'price_mode': mode, 'rip_price': '400'}})
    def refresh(product):
        product['market_price'] = '500.00'
        return {}
    with patch.object(defaults, 'catalog_details', side_effect=refresh), patch.object(routes, 'context', side_effect=ValueError('offline')):
        d = write(client, d, 'autofill', {})
    loaded = client.get(f"{BASE}/drafts/{d['id']}").json()
    assert loaded['fields']['price'] == price
    assert loaded['fields']['price_mode'] == mode
    assert loaded['fields']['rip_price'] == '400'
    assert loaded['defaults']['sources']['price']['amount'] == '500.00'


def test_legacy_saved_price_equal_to_default_is_preserved():
    d = {'fields': {'price': '100.00'}, 'defaults': {'values': {'price': '100.00'}}}
    defaults.apply_defaults(d, {'price': '120.00'}, {}, {})
    assert d['fields']['price'] == '100.00'


@pytest.mark.parametrize('rip_price,expected', [('', '456.36'), ('400.00','400.00')])
def test_adjusted_base_price_keeps_both_variant_semantics(client, rip_price, expected):
    d = prepared(client)
    d['fields'].update(fulfillment_mode='both', quantity='10', price='456.36', price_mode='5', rip_price=rip_price)
    payload = service.build_payload(d, 'AS_DRAFT', ['uploaded-image'])
    assert [s['price']['amount'] for s in payload['skus']] == ['456.36', expected]
