"""Shopify ID-token authentication for the explicit all-staff POS read policy.

PIN staff/customer/location headers are never authorization evidence. No Ops
cookie fallback, identity mapping, Shopify request or persistent token storage.
"""
from dataclasses import dataclass, field
import hashlib
import hmac
import re
import time

import jwt


class AuthenticationRequired(ValueError):
    pass


@dataclass(frozen=True)
class POSAccess:
    shop: str
    client_id: str
    secret: str = field(repr=False)

    @classmethod
    def from_settings(cls, settings):
        secret = settings.loyalty_pos_client_secret.get_secret_value()
        if not (settings.loyalty_pos_read_enabled and settings.loyalty_pos_all_staff_enabled
                and re.fullmatch(r'[a-z0-9][a-z0-9-]*\.myshopify\.com', settings.loyalty_shop_domain)
                and re.fullmatch(r'gid://shopify/Shop/[1-9][0-9]*', settings.loyalty_shop_id)
                and re.fullmatch(r'[A-Za-z0-9_-]{8,128}', settings.loyalty_pos_client_id)
                and 32 <= len(secret.encode()) <= 512):
            raise ValueError('pos_read_unavailable')
        return cls(settings.loyalty_shop_domain, settings.loyalty_pos_client_id, secret)

    def authenticate(self, authorization):
        try:
            if not re.fullmatch(r'Bearer [A-Za-z0-9_.-]+', authorization or ''):
                raise AuthenticationRequired()
            claims = jwt.decode(
                authorization[7:], self.secret, algorithms=['HS256'],
                audience=self.client_id, issuer=f'https://{self.shop}/admin',
                options={'require':['iss','dest','aud','sub','sid','iat','nbf','exp'], 'strict_aud':True},
            )
            if claims['dest'] != f'https://{self.shop}':
                raise AuthenticationRequired()
            if any(type(claims[k]) is not int for k in ('iat','nbf','exp')):
                raise AuthenticationRequired()
            if not (0 < claims['exp'] - claims['iat'] <= 300 and claims['nbf'] <= claims['exp']):
                raise AuthenticationRequired()
            if not re.fullmatch(r'[1-9][0-9]{0,19}', claims['sub']):
                raise AuthenticationRequired()
            if not isinstance(claims['sid'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', claims['sid']):
                raise AuthenticationRequired()
            # Only an ephemeral, non-reversible session bucket leaves this method.
            return hashlib.sha256(f'{self.shop}:{claims["sub"]}:{claims["sid"]}'.encode()).hexdigest()
        except (jwt.InvalidTokenError, ValueError, TypeError, KeyError):
            raise AuthenticationRequired('authentication_required') from None

    def _cursor_secret(self):
        return hmac.digest(self.secret.encode(), b'degen-loyalty-pos-history-v1', 'sha256')

    def encode_cursor(self, payload):
        return jwt.encode(dict(payload, aud='loyalty-pos-history', app=self.client_id,
                               shop=self.shop, exp=int(time.time())+300), self._cursor_secret(), algorithm='HS256')

    def decode_cursor(self, cursor, customer):
        try:
            claims = jwt.decode(cursor, self._cursor_secret(), algorithms=['HS256'], audience='loyalty-pos-history',
                                options={'require':['exp','app','shop','customer','upper','before','balance','count','as_of'],
                                         'strict_aud':True})
            if claims['app'] != self.client_id or claims['shop'] != self.shop or claims['customer'] != customer:
                raise ValueError()
            if any(type(claims[k]) is not int or claims[k] < 0 for k in ('upper','before','count')):
                raise ValueError()
            if not 0 < claims['before'] <= claims['upper'] or claims['count'] < 1:
                raise ValueError()
            if not isinstance(claims['balance'],str) or not re.fullmatch(r'[0-9]{1,30}', claims['balance']):
                raise ValueError()
            if not isinstance(claims['as_of'],str) or len(claims['as_of']) > 40:
                raise ValueError()
            return claims
        except (jwt.InvalidTokenError, ValueError, TypeError, KeyError):
            raise ValueError('invalid_cursor') from None
