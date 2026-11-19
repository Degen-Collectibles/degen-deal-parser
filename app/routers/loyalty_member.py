"""Register lookup of a scanned rewards card. Shopify bearer only; off by default."""
import asyncio
import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy.exc import SQLAlchemyError

from ..loyalty.member_codes import MemberCodeError, gates, resolve
from ..loyalty.pos_auth import AuthenticationRequired, POSAccess
from ..loyalty.schema import schema_ready
from .loyalty_pos import POSRoute, ReadBudget, SHOPIFY_ORIGINS

log = logging.getLogger(__name__)

PATH = '/api/loyalty/pos/member-codes/resolve'
STATUS = {'member_scan_unavailable': 503, 'rate_limited': 429, 'invalid_origin': 403,
          'request_too_large': 431, 'unknown_code': 404}


def build_router(engine, settings):
    router = APIRouter(route_class=POSRoute)
    budget = ReadBudget()

    def response(value, status=200):
        headers = {'Cache-Control': 'no-store'}
        if status == 401: headers['WWW-Authenticate'] = 'Bearer'
        if status == 429: headers['Retry-After'] = '60'
        return JSONResponse(value, status_code=status, headers=headers)

    def error(code):
        return response({'error': code}, STATUS.get(code, 400))

    @router.options(PATH, include_in_schema=False)
    async def preflight(request: Request):
        if gates(settings): return error('member_scan_unavailable')
        if not budget.allow(settings.loyalty_pos_total_requests): return error('rate_limited')
        requested = {h.strip().lower() for h in request.headers.get('access-control-request-headers', '').split(',') if h.strip()}
        if (request.headers.get('origin') not in SHOPIFY_ORIGINS
                or request.headers.get('access-control-request-method') != 'POST'
                or not requested <= {'authorization', 'content-type'}):
            return response({'error': 'invalid_preflight'}, 403)
        return Response(status_code=204, headers={'Access-Control-Allow-Methods': 'POST',
                        'Access-Control-Allow-Headers': 'Authorization, Content-Type', 'Cache-Control': 'no-store'})

    @router.post(PATH, include_in_schema=False)
    async def lookup(request: Request):
        try:
            if gates(settings): raise MemberCodeError('member_scan_unavailable')
            access = POSAccess.from_settings(settings)
            if not budget.allow(settings.loyalty_pos_total_requests): raise MemberCodeError('rate_limited')
            if sum(len(k) + len(v) for k, v in request.scope['headers']) > 8192:
                raise MemberCodeError('request_too_large')
            if len(request.headers.getlist('authorization')) != 1:
                raise AuthenticationRequired()
            session = access.authenticate(request.headers.get('authorization'))
            # Wrong guesses spend the same per-session budget as real scans.
            if not budget.allow(settings.loyalty_pos_session_requests, session): raise MemberCodeError('rate_limited')
            origin = request.headers.get('origin')
            if origin and origin not in SHOPIFY_ORIGINS: raise MemberCodeError('invalid_origin')
            if request.url.query or request.headers.get('content-type', '').split(';')[0] != 'application/json':
                raise MemberCodeError('invalid_request')
            raw = bytearray()
            async for part in request.stream():
                raw.extend(part)
                if len(raw) > 1024: raise MemberCodeError('invalid_request')
            try: data = json.loads(raw)
            except (ValueError, UnicodeError): raise MemberCodeError('invalid_request') from None
            if not isinstance(data, dict) or set(data) != {'code'}: raise MemberCodeError('invalid_request')

            def query():
                if not schema_ready(engine): raise MemberCodeError('member_scan_unavailable')
                return resolve(engine, settings, access.shop, data['code'])
            customer = await asyncio.to_thread(query)
            if customer is None: raise MemberCodeError('unknown_code')
            return response({'customer_id': customer})
        except AuthenticationRequired:
            return response({'error': 'authentication_required'}, 401)
        except MemberCodeError as exc:
            return error(str(exc))
        except SQLAlchemyError:
            log.warning('loyalty member lookup unavailable')
            return error('member_scan_unavailable')

    return router
