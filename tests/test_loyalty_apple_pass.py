"""Apple Wallet rewards card: the signed pass file and the public route that issues it."""
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import shutil
import subprocess
import zipfile

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs7
from cryptography.x509.oid import NameOID
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.loyalty import apple_pass
from app.loyalty.apple_pass import ApplePassError, build, gates
from app.loyalty.member_codes import resolve
from app.loyalty.wallet import platforms
from app.routers.loyalty_wallet import build_router
from test_loyalty_pos import SHOP, token as staff_token
from test_loyalty_service import store
from test_loyalty_wallet import BASE, POS, enabled

TEAM = 'ABCDE12345'
PASS_TYPE = 'pass.test.degen.rewards'
SERIAL = 'a' * 32
CODE = 'DGR1' + 'A' * 26


def _certificate(subject, issuer, key, signer, ca=False):
    now = datetime.now(timezone.utc)
    return (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=30))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
            .sign(signer, hashes.SHA256()))


def _pem(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())


@pytest.fixture(scope='module')
def files(tmp_path_factory):
    """Throwaway stand-ins for Apple's intermediate and a Pass Type ID certificate."""
    folder = tmp_path_factory.mktemp('apple')
    ca_key = rsa.generate_private_key(65537, 2048)
    key = rsa.generate_private_key(65537, 2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'Synthetic WWDR')])

    def leaf(pass_type=PASS_TYPE, team=TEAM):
        name = x509.Name([x509.NameAttribute(NameOID.USER_ID, pass_type),
                          x509.NameAttribute(NameOID.COMMON_NAME, 'Pass Type ID: ' + pass_type),
                          x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, team)])
        return _certificate(name, ca_name, key, ca_key).public_bytes(serialization.Encoding.PEM)
    written = {'ca.pem': _certificate(ca_name, ca_name, ca_key, ca_key, ca=True).public_bytes(serialization.Encoding.PEM),
               'cert.pem': leaf(), 'key.pem': _pem(key), 'other-type.pem': leaf(pass_type='pass.test.other'),
               'other-team.pem': leaf(team='ZZZZZ99999'), 'other-key.pem': _pem(rsa.generate_private_key(65537, 2048)),
               'junk.pem': b'not a certificate'}
    for name, data in written.items():
        (folder / name).write_bytes(data)
    return {name: str(folder / name) for name in written}


def apple(files, **changes):
    return enabled(**{'loyalty_apple_team_id': TEAM, 'loyalty_apple_pass_type_id': PASS_TYPE,
                      'loyalty_apple_pass_cert_path': files['cert.pem'], 'loyalty_apple_pass_key_path': files['key.pem'],
                      'loyalty_apple_wwdr_cert_path': files['ca.pem'], **changes})


def unpack(data):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def client(store, settings):
    app = FastAPI()
    app.include_router(build_router(store.engine, settings))
    return TestClient(app)


def page(c):
    return c.post(POS, headers={'Authorization': 'Bearer ' + staff_token()}).json()['url'][len(BASE):]


def test_unavailable_until_every_apple_setting_is_present(files):
    assert gates(enabled()) == ['apple_team_id_unset', 'apple_pass_type_id_unset', 'apple_certificates_unset']
    assert platforms(enabled()) == []
    assert gates(apple(files)) == [] and platforms(apple(files)) == ['apple']
    assert gates(apple(files, loyalty_apple_team_id='abc')) == ['apple_team_id_unset']
    assert gates(apple(files, loyalty_apple_pass_type_id='com.example')) == ['apple_pass_type_id_unset']
    assert gates(apple(files, loyalty_apple_pass_key_path=files['key.pem'] + '.missing')) == ['apple_certificates_unset']
    with pytest.raises(ApplePassError):
        build(enabled(), SERIAL, CODE, 5)


def test_pass_holds_the_member_code_and_points_and_nothing_about_the_customer(files):
    parts = unpack(build(apple(files), SERIAL, CODE, 19))
    assert set(parts) == {'pass.json', 'icon.png', 'icon@2x.png', 'icon@3x.png', 'manifest.json', 'signature'}
    card = json.loads(parts['pass.json'])
    assert card['passTypeIdentifier'] == PASS_TYPE and card['teamIdentifier'] == TEAM and card['serialNumber'] == SERIAL
    assert card['barcodes'] == [{'format': 'PKBarcodeFormatQR', 'message': CODE, 'messageEncoding': 'iso-8859-1'}]
    assert card['storeCard']['primaryFields'] == [{'key': 'points', 'label': 'POINTS', 'value': 19}]
    assert 'webServiceURL' not in card and 'authenticationToken' not in card   # balance updates are a later step
    for word in ('gid://', 'Customer', 'myshopify', '@', 'phone', 'email'):
        assert word not in parts['pass.json'].decode()
    assert all(parts[name][:4] == b'\x89PNG' for name in apple_pass.ICONS)


def test_manifest_lists_every_file_and_the_signature_carries_both_certificates(files):
    parts = unpack(build(apple(files), SERIAL, CODE, 0))
    manifest = json.loads(parts['manifest.json'])
    assert set(manifest) == set(parts) - {'manifest.json', 'signature'}
    assert all(manifest[name] == hashlib.sha1(parts[name]).hexdigest() for name in manifest)
    names = [c.subject.rfc4514_string() for c in pkcs7.load_der_pkcs7_certificates(parts['signature'])]
    assert len(names) == 2
    assert [name for name in names if 'Synthetic WWDR' in name]


def test_signature_verifies_against_the_manifest(files, tmp_path):
    openssl = shutil.which('openssl')
    if not openssl:
        pytest.skip('openssl not installed')
    parts = unpack(build(apple(files), SERIAL, CODE, 7))
    (tmp_path / 'signature').write_bytes(parts['signature'])

    def verify(content):
        (tmp_path / 'manifest.json').write_bytes(content)
        command = [openssl, 'smime', '-verify', '-binary', '-inform', 'DER', '-in', str(tmp_path / 'signature'),
                   '-content', str(tmp_path / 'manifest.json'), '-CAfile', files['ca.pem'], '-purpose', 'any',
                   '-out', str(tmp_path / 'out')]
        return subprocess.run(command, capture_output=True).returncode
    assert verify(parts['manifest.json']) == 0
    assert verify(parts['manifest.json'] + b' ') != 0


@pytest.mark.parametrize('change', [
    {'loyalty_apple_pass_cert_path': 'other-type.pem'}, {'loyalty_apple_pass_cert_path': 'other-team.pem'},
    {'loyalty_apple_pass_key_path': 'other-key.pem'}, {'loyalty_apple_pass_cert_path': 'junk.pem'},
    {'loyalty_apple_wwdr_cert_path': 'junk.pem'}])
def test_a_certificate_that_does_not_match_the_settings_is_refused(files, change):
    with pytest.raises(ApplePassError):
        build(apple(files, **{k: files[v] for k, v in change.items()}), SERIAL, CODE, 1)


@pytest.mark.parametrize('serial,code,points', [('short', CODE, 1), (SERIAL, 'has space', 1), (SERIAL, CODE, '1'),
                                                (SERIAL, CODE, True), (SERIAL, CODE, 1.5), (None, CODE, 1)])
def test_bad_card_details_are_refused(files, serial, code, points):
    with pytest.raises(ApplePassError):
        build(apple(files), serial, code, points)


def test_page_offers_apple_only_and_the_button_returns_a_card_once(store, files):
    settings = apple(files)
    c = client(store, settings)
    link = page(c)
    html = c.get(link).text
    assert f'action="{link}/apple"' in html and 'Add to Apple Wallet' in html and 'Google' not in html
    result = c.post(link + '/apple')
    assert result.status_code == 200 and result.headers['content-type'] == 'application/vnd.apple.pkpass'
    assert result.headers['cache-control'] == 'no-store' and 'degen-rewards.pkpass' in result.headers['content-disposition']
    card = json.loads(unpack(result.content)['pass.json'])
    assert card['storeCard']['primaryFields'][0]['value'] == 0          # no purchases yet
    assert resolve(store.engine, settings, SHOP, card['barcodes'][0]['message']) == '101'
    with store.engine.connect() as connection:
        rows = connection.execute(text('SELECT customer_id, platform, serial FROM loyalty_wallet_card')).all()
    assert rows == [('gid://shopify/Customer/101', 'apple', card['serialNumber'])]
    assert '101' not in card['serialNumber']
    again = c.post(link + '/apple')
    assert again.status_code == 404 and 'expired' in again.text
    assert c.get(link).status_code == 404


def test_two_cards_for_one_customer_share_the_code_but_not_the_serial(store, files):
    c = client(store, apple(files))
    cards = [json.loads(unpack(c.post(page(c) + '/apple').content)['pass.json']) for _ in range(2)]
    assert cards[0]['barcodes'] == cards[1]['barcodes'] and cards[0]['serialNumber'] != cards[1]['serialNumber']


def test_a_broken_certificate_fails_without_using_the_link_up(store, files):
    broken = client(store, apple(files, loyalty_apple_pass_cert_path=files['other-team.pem']))
    link = page(broken)
    result = broken.post(link + '/apple')
    assert result.status_code == 503 and 'could not be made' in result.text and 'mismatch' not in result.text
    assert client(store, apple(files)).post(link + '/apple').status_code == 200
    with store.engine.connect() as connection:
        assert connection.execute(text('SELECT COUNT(*) FROM loyalty_wallet_card')).scalar() == 1


def test_apple_route_is_closed_when_unset_off_unknown_or_oversized(store, files):
    c = client(store, apple(files))
    link = page(c)
    assert client(store, enabled()).post(link + '/apple').status_code == 404          # Apple not configured
    assert client(store, apple(files, loyalty_wallet_enrollment_enabled=False)).post(link + '/apple').status_code == 404
    assert c.post('/rewards/card/' + 'A' * 43 + '/apple').status_code == 404
    assert c.post(link + '/apple', content=b'x' * 2000).status_code == 404
    assert c.post(link + '/google').status_code in (404, 405)
    assert c.get(link + '/apple').status_code in (404, 405)
    assert c.post(link + '/apple').status_code == 200                                  # none of that used the link
