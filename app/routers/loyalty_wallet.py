"""Rewards-card sign-up: a register action plus the public page it links to.

The register action needs a Shopify staff token. The page and the QR image
are public, because the customer's phone and the register's image loader have
no session; the unguessable single-use link is the only credential, and
neither response contains anything about the customer.
"""
import asyncio
import logging
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from sqlalchemy.exc import SQLAlchemyError

from ..loyalty.pos_auth import AuthenticationRequired, POSAccess
from ..loyalty.schema import schema_ready
from ..loyalty import apple_pass
from ..loyalty.wallet import WalletError, create, gates, issue, link, platforms, qr_png, valid
from .loyalty_pos import POSRoute, ReadBudget, SHOPIFY_ORIGINS

log = logging.getLogger(__name__)

POS_PATH = '/api/loyalty/pos/customers/{customer_id}/wallet-card'
PAGE_PATH = '/rewards/card/{token}'
STATUS = {'wallet_enrollment_unavailable': 503, 'rate_limited': 429, 'invalid_origin': 403, 'request_too_large': 431}
PUBLIC_HEADERS = {'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer', 'X-Robots-Tag': 'noindex, nofollow',
                  'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY',
                  'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'"}

PAGE = '''<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Degen Rewards</title>
<style>
body{{margin:0;font:17px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:#111;color:#f4f4f4}}
main{{max-width:28rem;margin:0 auto;padding:3rem 1.25rem}}
h1{{font-size:1.6rem;margin:0 0 .75rem}}
p{{margin:0 0 1rem;color:#cfcfcf}}
button{{display:block;width:100%;margin:0 0 .75rem;padding:.9rem 1rem;border:0;border-radius:.6rem;background:#f4f4f4;color:#111;font:inherit;font-weight:600}}
</style></head><body><main>
<h1>Degen Rewards</h1>
{body}
</main></body></html>'''
EXPIRED = '<p>This link has expired or was already used.</p><p>Ask a team member at the register for a new code.</p>'
FAILED = '<p>The card could not be made.</p><p>Try again in a moment, or ask a team member at the register.</p>'
NOT_YET = '<p>Phone wallet cards are not available yet.</p><p>Your points are still tracked at the register.</p>'
LABELS = {'apple': 'Add to Apple Wallet', 'google': 'Add to Google Wallet'}


def build_router(engine, settings):
    router = APIRouter(route_class=POSRoute)
    staff = ReadBudget()
    public = ReadBudget()

    def response(value, status=200):
        headers = {'Cache-Control': 'no-store'}
        if status == 401: headers['WWW-Authenticate'] = 'Bearer'
        if status == 429: headers['Retry-After'] = '60'
        return JSONResponse(value, status_code=status, headers=headers)

    def error(code):
        return response({'error': code}, STATUS.get(code, 400))

    def page(body, status=200):
        return HTMLResponse(PAGE.format(body=body), status_code=status, headers=PUBLIC_HEADERS)

    @router.options(POS_PATH, include_in_schema=False)
    async def preflight(request: Request, customer_id: str):
        if gates(settings): return error('wallet_enrollment_unavailable')
        if not staff.allow(settings.loyalty_pos_total_requests): return error('rate_limited')
        requested = {h.strip().lower() for h in request.headers.get('access-control-request-headers', '').split(',') if h.strip()}
        if (request.headers.get('origin') not in SHOPIFY_ORIGINS
                or request.headers.get('access-control-request-method') != 'POST'
                or not requested <= {'authorization', 'content-type'}):
            return response({'error': 'invalid_preflight'}, 403)
        return Response(status_code=204, headers={'Access-Control-Allow-Methods': 'POST',
                        'Access-Control-Allow-Headers': 'Authorization, Content-Type', 'Cache-Control': 'no-store'})

    @router.post(POS_PATH, include_in_schema=False)
    async def issue_link(request: Request, customer_id: str):
        try:
            if gates(settings): raise WalletError('wallet_enrollment_unavailable')
            access = POSAccess.from_settings(settings)
            if not staff.allow(settings.loyalty_pos_total_requests): raise WalletError('rate_limited')
            if sum(len(k) + len(v) for k, v in request.scope['headers']) > 8192:
                raise WalletError('request_too_large')
            if len(request.headers.getlist('authorization')) != 1:
                raise AuthenticationRequired()
            session = access.authenticate(request.headers.get('authorization'))
            if not staff.allow(settings.loyalty_pos_session_requests, session): raise WalletError('rate_limited')
            origin = request.headers.get('origin')
            if origin and origin not in SHOPIFY_ORIGINS: raise WalletError('invalid_origin')
            if request.url.query or not re.fullmatch(r'[1-9][0-9]{0,15}', customer_id) or int(customer_id) > 9007199254740991:
                raise WalletError('invalid_request')
            size = 0
            async for part in request.stream():
                size += len(part)
                if size > 2: raise WalletError('invalid_request')   # at most an empty JSON object

            def work():
                if not schema_ready(engine): raise WalletError('wallet_enrollment_unavailable')
                return create(engine, settings, access.shop, customer_id, session)
            token, expires = await asyncio.to_thread(work)
            url = link(settings, token)
            return response({'customer_id': customer_id, 'url': url, 'qr_url': url + '/qr.png',
                             'expires_at': expires.isoformat() + 'Z'})
        except AuthenticationRequired:
            return response({'error': 'authentication_required'}, 401)
        except WalletError as exc:
            return error(str(exc))
        except SQLAlchemyError:
            log.warning('loyalty wallet link unavailable')
            return error('wallet_enrollment_unavailable')

    async def live(token):
        """True/False for the link, or None when the feature cannot answer."""
        if gates(settings) or not public.allow(settings.loyalty_pos_total_requests):
            return None
        try:
            return await asyncio.to_thread(lambda: schema_ready(engine) and valid(engine, settings, token))
        except (SQLAlchemyError, WalletError):
            log.warning('loyalty wallet page unavailable')
            return None

    @router.get(PAGE_PATH, include_in_schema=False)
    async def landing(token: str):
        # Opening the page never uses the link up: phone cameras and message
        # apps preview links before the customer taps anything.
        state = await live(token)
        if not state:
            return page(EXPIRED, 404)
        available = platforms(settings)
        if not available:
            return page(NOT_YET)
        buttons = ''.join(f'<form method="post" action="/rewards/card/{token}/{name}"><button type="submit">{LABELS[name]}</button></form>'
                          for name in available)
        return page('<p>Add your rewards card to your phone to see your points and get scanned at the register.</p>' + buttons)

    @router.post(PAGE_PATH + '/apple', include_in_schema=False)
    async def apple(request: Request, token: str):
        # The tap on the button is what uses the link up; the reply is the card file.
        if gates(settings) or not public.allow(settings.loyalty_pos_total_requests):
            return page(EXPIRED, 404)
        size = 0
        async for part in request.stream():
            size += len(part)
            if size > 1024: return page(EXPIRED, 404)

        def work():
            if not schema_ready(engine) or 'apple' not in platforms(settings):
                return None
            apple_pass.check(settings)                 # before the link is used up
            card = issue(engine, settings, token, 'apple')
            return card and apple_pass.build(settings, card['serial'], card['code'], card['points'])
        try:
            data = await asyncio.to_thread(work)
        except (SQLAlchemyError, ValueError):
            # Covers settings, certificate and member-code errors; never say which to the public.
            log.warning('loyalty apple card unavailable')
            return page(FAILED, 503)
        if not data:
            return page(EXPIRED, 404)
        return Response(data, media_type=apple_pass.MIME, headers={
            **PUBLIC_HEADERS, 'Content-Disposition': 'attachment; filename="degen-rewards.pkpass"'})

    @router.get(PAGE_PATH + '/qr.png', include_in_schema=False)
    async def qr(token: str):
        if not await live(token):
            return Response(status_code=404, headers=PUBLIC_HEADERS)
        image = await asyncio.to_thread(qr_png, link(settings, token))
        return Response(image, media_type='image/png', headers=PUBLIC_HEADERS)

    return router
