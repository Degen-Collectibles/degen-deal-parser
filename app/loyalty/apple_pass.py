"""Apple Wallet pass for a rewards card. Unavailable until Apple settings exist.

A pass is a zip of pass.json, the images, a manifest of SHA-1 hashes and a
detached PKCS#7 signature over that manifest made with the Pass Type ID
certificate. The pass carries the opaque member code as a QR code and the
points balance; it holds no name, phone number, email or Shopify ID.
"""
import hashlib
import io
import json
import os
import re
import zipfile

MIME = 'application/vnd.apple.pkpass'
TEAM = re.compile(r'[A-Z0-9]{10}')
PASS_TYPE = re.compile(r'pass\.[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+')
FILES = ('loyalty_apple_pass_cert_path', 'loyalty_apple_pass_key_path', 'loyalty_apple_wwdr_cert_path')
ORGANISATION = 'Degen Collectibles'
BACKGROUND = (17, 17, 17)
ICONS = {'icon.png': 29, 'icon@2x.png': 58, 'icon@3x.png': 87}


class ApplePassError(ValueError):
    pass


def gates(settings):
    """Reasons an Apple pass cannot be made; empty when it can."""
    reasons = []
    if not TEAM.fullmatch(getattr(settings, 'loyalty_apple_team_id', '') or ''):
        reasons.append('apple_team_id_unset')
    if not PASS_TYPE.fullmatch(getattr(settings, 'loyalty_apple_pass_type_id', '') or ''):
        reasons.append('apple_pass_type_id_unset')
    if not all(os.path.isfile(getattr(settings, name, '') or '') for name in FILES):
        reasons.append('apple_certificates_unset')
    return reasons


def _read(path):
    with open(path, 'rb') as handle:
        return handle.read()


def _credentials(settings):
    """Signing certificate, key and Apple's intermediate, checked against the settings."""
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.x509.oid import NameOID
    try:
        certificate = x509.load_pem_x509_certificate(_read(settings.loyalty_apple_pass_cert_path))
        intermediate = x509.load_pem_x509_certificate(_read(settings.loyalty_apple_wwdr_cert_path))
        password = settings.loyalty_apple_pass_key_password.get_secret_value().encode() or None
        key = load_pem_private_key(_read(settings.loyalty_apple_pass_key_path), password)
    except (OSError, ValueError, TypeError) as exc:
        raise ApplePassError('apple_certificates_unreadable') from exc

    def field(oid):
        found = certificate.subject.get_attributes_for_oid(oid)
        return found[0].value if found else None
    # A certificate for another pass type or team makes a pass iPhones silently refuse.
    if field(NameOID.USER_ID) != settings.loyalty_apple_pass_type_id:
        raise ApplePassError('apple_certificate_pass_type_mismatch')
    if field(NameOID.ORGANIZATIONAL_UNIT_NAME) != settings.loyalty_apple_team_id:
        raise ApplePassError('apple_certificate_team_mismatch')
    if certificate.public_key().public_numbers() != key.public_key().public_numbers():
        raise ApplePassError('apple_certificate_key_mismatch')
    return certificate, key, intermediate


def check(settings):
    """Raise unless the certificate files can sign a pass for these settings."""
    if gates(settings):
        raise ApplePassError('apple_pass_unavailable')
    _credentials(settings)


def _icon(size):
    """Placeholder artwork until the real logo is supplied."""
    from PIL import Image
    output = io.BytesIO()
    Image.new('RGB', (size, size), BACKGROUND).save(output, format='PNG')
    return output.getvalue()


def _sign(manifest, certificate, key, intermediate):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.serialization import Encoding, pkcs7
    return (pkcs7.PKCS7SignatureBuilder().set_data(manifest)
            .add_signer(certificate, key, hashes.SHA256()).add_certificate(intermediate)
            .sign(Encoding.DER, [pkcs7.PKCS7Options.DetachedSignature, pkcs7.PKCS7Options.Binary]))


def build(settings, serial, code, points):
    """Signed .pkpass bytes for one card."""
    if gates(settings):
        raise ApplePassError('apple_pass_unavailable')
    if not re.fullmatch(r'[0-9a-f]{32}', serial or '') or not re.fullmatch(r'[A-Z0-9]{8,64}', code or ''):
        raise ApplePassError('invalid_card')
    if isinstance(points, bool) or not isinstance(points, int):
        raise ApplePassError('invalid_points')
    certificate, key, intermediate = _credentials(settings)
    colour = 'rgb(%d, %d, %d)' % BACKGROUND
    card = {
        'formatVersion': 1,
        'passTypeIdentifier': settings.loyalty_apple_pass_type_id,
        'teamIdentifier': settings.loyalty_apple_team_id,
        'serialNumber': serial,
        'organizationName': ORGANISATION,
        'description': 'Degen Rewards card',
        'logoText': 'Degen Rewards',
        'backgroundColor': colour,
        'foregroundColor': 'rgb(244, 244, 244)',
        'labelColor': 'rgb(207, 207, 207)',
        'storeCard': {
            'primaryFields': [{'key': 'points', 'label': 'POINTS', 'value': points}],
            'backFields': [{'key': 'about', 'label': 'How it works',
                            'value': 'Show this card at the register to be added to the sale.'}],
        },
        'barcodes': [{'format': 'PKBarcodeFormatQR', 'message': code, 'messageEncoding': 'iso-8859-1'}],
    }
    files = {'pass.json': json.dumps(card, separators=(',', ':')).encode()}
    files.update({name: _icon(size) for name, size in ICONS.items()})
    manifest = json.dumps({name: hashlib.sha1(data).hexdigest() for name, data in files.items()},
                          separators=(',', ':')).encode()
    files['manifest.json'] = manifest
    files['signature'] = _sign(manifest, certificate, key, intermediate)
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return output.getvalue()
