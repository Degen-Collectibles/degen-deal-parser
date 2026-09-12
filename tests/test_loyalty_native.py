"""Native launcher safety using synthetic keys/IDs only; no Shopify access."""
import os
import time

import jwt
from fastapi.testclient import TestClient
from pydantic import SecretStr
import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from scripts.loyalty_native import DEV_CLIENT, DEV_SHOP, create_native, private_secret, preview_host, samples

KEY = 'synthetic-native-test-key-' + 'x' * 40
SHOP_ID = 'gid://shopify/Shop/999999999'  # Test fixture, never a launch value.
PATH = '/api/loyalty/pos/customers/101'


def bearer(**changes):
    now = int(time.time())
    claims = dict(iss=f'https://{DEV_SHOP}/admin', dest=f'https://{DEV_SHOP}', aud=DEV_CLIENT,
                  sub='42', sid='synthetic-native-session', iat=now, nbf=now, exp=now + 60)
    claims.update(changes)
    return {'Authorization': 'Bearer ' + jwt.encode(claims, KEY, algorithm='HS256')}


@pytest.fixture
def native(tmp_path):
    app = create_native(tmp_path, shop_id=SHOP_ID, secret=SecretStr(KEY),
                        sample_customers={'101': 'earned', '102': 'refund', '103': 'pending', '104': 'review'})
    with TestClient(app, base_url='http://127.0.0.1:8768') as client:
        yield app, client


def test_real_route_samples_and_sqlite_cannot_write(native):
    app, client = native
    for customer, balance in [('101', '20'), ('102', '19'), ('103', '0'), ('104', '12')]:
        response = client.get(f'/api/loyalty/pos/customers/{customer}', headers=bearer())
        assert response.status_code == 200
        assert response.json()['balance_points'] == balance
        assert response.json()['posting_paused'] is True
        assert response.headers['cache-control'] == 'no-store'
    refund = client.get('/api/loyalty/pos/customers/102', headers=bearer()).json()
    assert [item['delta_points'] for item in refund['history']] == ['-1', '20']
    with app.state.engine.connect() as connection, pytest.raises(OperationalError, match='readonly'):
        connection.execute(text('CREATE TABLE forbidden_write (id INTEGER)'))


def test_no_token_mint_ops_docs_writes_or_cookie_fallback(native):
    _, client = native
    assert client.get('/health').json() == {'status': 'ok', 'mode': 'synthetic-native-preview'}
    for path in ['/demo/token', '/loyalty', '/admin', '/docs', '/redoc', '/openapi.json', '/']:
        assert client.get(path).status_code == 404
        assert client.post(path).status_code == 404
    assert client.get(PATH).status_code == 401
    assert client.get(PATH, headers={'Cookie': 'session=synthetic'}).status_code == 401
    assert client.post(PATH, headers=bearer()).status_code == 405
    assert client.get(PATH + '?email=synthetic@example.invalid', headers=bearer()).status_code == 400
    assert client.get(PATH + '?limit=26', headers=bearer()).status_code == 400


@pytest.mark.parametrize('change', [
    {'aud': 'production-app-excluded'}, {'dest': 'https://other.myshopify.com'},
    {'iss': 'https://other.myshopify.com/admin'}, {'exp': 1}, {'sub': None}, {'sid': None},
])
def test_native_denies_other_apps_shops_invalid_sessions(native, change):
    _, client = native
    response = client.get(PATH, headers=bearer(**change))
    assert response.status_code == 401
    assert response.json() == {'error': 'authentication_required'}
    assert KEY not in response.text


def test_host_cors_and_expiry(native, monkeypatch):
    _, client = native
    assert client.get('/health', headers={'Host': 'evil.invalid'}).status_code == 403
    assert client.options(PATH, headers={'Origin': 'https://evil.invalid',
                          'Access-Control-Request-Method': 'GET'}).status_code == 403
    assert client.options(PATH, headers={'Origin': 'https://cdn.shopify.com',
                          'Access-Control-Request-Method': 'GET',
                          'Access-Control-Request-Headers': 'authorization'}).status_code == 204
    start = time.monotonic()
    monkeypatch.setattr('scripts.loyalty_native.time.monotonic', lambda: start + 3601)
    assert client.get(PATH, headers=bearer()).status_code == 503


def test_missing_shop_secret_and_existing_database_refused(tmp_path):
    for shop, secret in [('', SecretStr(KEY)), (SHOP_ID, SecretStr('short')), (SHOP_ID, KEY)]:
        with pytest.raises(ValueError):
            create_native(tmp_path, shop_id=shop, secret=secret)
        assert not (tmp_path / 'native.db').exists()
    (tmp_path / 'native.db').write_bytes(b'preserve existing')
    with pytest.raises(FileExistsError):
        create_native(tmp_path, shop_id=SHOP_ID, secret=SecretStr(KEY))
    assert (tmp_path / 'native.db').read_bytes() == b'preserve existing'


@pytest.mark.skipif(os.name != 'posix', reason='WSL native launcher requires POSIX owner-only secret handoff')
def test_secret_permissions_symlink_size_and_redaction(tmp_path):
    secret = tmp_path / 'dev-secret'
    secret.write_text(KEY + '\n'); secret.chmod(0o600)
    assert private_secret(secret).get_secret_value() == KEY
    assert KEY not in repr(private_secret(secret))
    secret.chmod(0o644)
    with pytest.raises(ValueError): private_secret(secret)
    secret.chmod(0o600)
    link = tmp_path / 'link'; link.symlink_to(secret)
    with pytest.raises(ValueError): private_secret(link)
    for value in ['short', 'x' * 515, KEY + '\n\n\n', KEY + '\x00']:
        secret.write_text(value)
        with pytest.raises(ValueError): private_secret(secret)


def test_bounded_mapping_and_public_origin(tmp_path):
    assert samples(['101:refund']) == {'101': 'refund'}
    for value in [['101:earned', '101:review'], ['0:earned'], ['101:invented'], ['01:earned']]:
        with pytest.raises(ValueError): samples(value)
    assert preview_host('https://synthetic-preview.example.invalid') == 'synthetic-preview.example.invalid'
    for url in ['http://example.invalid', 'https://user:pass@example.invalid', 'https://example.invalid/path',
                'https://degen-loyalty-dev.myshopify.com', 'https://example.invalid:443']:
        with pytest.raises(ValueError): preview_host(url)
    with pytest.raises(ValueError):
        create_native(tmp_path, shop_id=SHOP_ID, secret=SecretStr(KEY), minutes=61)


def test_cli_adapter_binds_target_and_ignores_cli_secret(tmp_path):
    import json
    from scripts.loyalty_native_web import cli_arguments
    inputs = tmp_path / 'native-inputs.json'
    inputs.write_text(json.dumps({'shop_id': SHOP_ID, 'sample_customers': {'101': 'refund'}}))
    env = {'SHOPIFY_API_KEY': DEV_CLIENT, 'PORT': '8768',
           'APP_URL': 'https://synthetic-preview.example.invalid', 'SHOPIFY_API_SECRET': 'never-use-this'}
    args = cli_arguments(env, inputs)
    assert '101:refund' in args and SHOP_ID in args
    assert 'never-use-this' not in str(args)
    for changes in [{'SHOPIFY_API_KEY': 'wrong-app'}, {'PORT': '8767'}, {'APP_URL': 'https://example.com'},
                    {'APP_URL': 'http://insecure.invalid'}]:
        with pytest.raises(ValueError): cli_arguments(dict(env, **changes), inputs)
    inputs.write_text(json.dumps({'shop_id': SHOP_ID, 'contact': 'forbidden'}))
    with pytest.raises(ValueError): cli_arguments(env, inputs)


def test_parent_config_targets_and_no_scopes():
    import tomllib
    from scripts.loyalty_native import ROOT
    config = tomllib.loads((ROOT / 'shopify/loyalty-dev/shopify.app.toml').read_text())
    assert config['client_id'] == DEV_CLIENT
    assert config['build']['dev_store_url'] == DEV_SHOP
    assert config['access_scopes']['scopes'] == ''
    # Authorized dev-app linking pulled this version; shared Admin reader stays 2026-04.
    assert config['webhooks'] == {'api_version': '2026-07'}
    web = tomllib.loads((ROOT / 'shopify/loyalty-dev/web/shopify.web.toml').read_text())
    assert web['port'] == 8768 and 'loyalty_native_web.py' in web['commands']['dev']


def test_launch_check_only_uses_synthetic_secret_without_exposing_listener(tmp_path):
    import subprocess
    import sys
    from scripts.loyalty_native import ROOT
    secret = tmp_path / 'synthetic-secret'
    secret.write_text(KEY); secret.chmod(0o600)
    result = subprocess.run([sys.executable, str(ROOT / 'scripts/loyalty_native.py'),
        '--shop-id', SHOP_ID, '--secret-file', str(secret), '--check-only'],
        cwd=ROOT, env={'PATH': os.defpath}, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert 'deny checks passed; no listener or tunnel started' in result.stdout
    assert KEY not in result.stdout + result.stderr
