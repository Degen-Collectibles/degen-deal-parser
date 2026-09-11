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
from sqlmodel import Session, SQLModel, create_engine
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
    fields = dict(product_name='Destined Rivals Elite Trainer Box', image_heading='DESTINED RIVALS', language='English',
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
