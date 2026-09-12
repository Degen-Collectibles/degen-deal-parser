"""Read-only POS bearer endpoint; deliberately independent of Ops cookie auth."""
import logging
import re
import threading
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from sqlalchemy.exc import SQLAlchemyError

from ..loyalty.pos_auth import POSAccess, AuthenticationRequired
from ..loyalty.queries import ledger_snapshot, SnapshotChanged
from ..loyalty.schema import schema_ready

log = logging.getLogger(__name__)


SHOPIFY_ORIGINS = {'https://cdn.shopify.com', 'https://extensions.shopifycdn.com'}


class POSRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()
        async def handler(request):
            response = await original(request)
            origin = request.headers.get('origin')
            if origin in SHOPIFY_ORIGINS:
                response.headers['Access-Control-Allow-Origin'] = origin
                response.headers['Vary'] = 'Origin'
            return response
        return handler


class ReadBudget:
    """Bounded per-process global and session budgets; no DB/customer writes.

    Multi-process deployment also needs a reviewed aggregate ingress limit.
    Invalid tokens consume the global budget, not unbounded identity buckets.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.started = time.monotonic()
        self.total = 0
        self.sessions = {}

    def allow(self, maximum, session=None):
        with self.lock:
            now = time.monotonic()
            if now - self.started >= 60:
                self.started = now; self.total = 0; self.sessions.clear()
            if session is None:
                if self.total >= maximum:return False
                self.total += 1
            else:
                if self.sessions.get(session,0) >= maximum:return False
                if session not in self.sessions and len(self.sessions) >= 4096:return False
                self.sessions[session] = self.sessions.get(session,0)+1
            return True


def build_router(engine, settings):
    router = APIRouter(route_class=POSRoute)
    budget = ReadBudget()

    def error(code, status):
        headers = {'Cache-Control':'no-store'}
        if status == 401:headers['WWW-Authenticate'] = 'Bearer'
        if status == 429:headers['Retry-After'] = '60'
        return JSONResponse({'error':code}, status_code=status, headers=headers)

    @router.options('/api/loyalty/pos/customers/{customer_id}', include_in_schema=False)
    async def preflight(request: Request, customer_id: str):
        try:POSAccess.from_settings(settings)
        except ValueError:return error('pos_read_unavailable',503)
        if not budget.allow(settings.loyalty_pos_total_requests):return error('rate_limited',429)
        if sum(len(k)+len(v) for k,v in request.scope['headers']) > 8192 or len(request.url.path) > 200:
            return error('request_too_large',431)
        requested = {h.strip().lower() for h in request.headers.get('access-control-request-headers','').split(',') if h.strip()}
        if (request.headers.get('origin') not in SHOPIFY_ORIGINS
                or request.headers.get('access-control-request-method') != 'GET'
                or not requested <= {'authorization'}):
            return error('invalid_preflight',403)
        return Response(status_code=204,headers={'Access-Control-Allow-Methods':'GET',
                        'Access-Control-Allow-Headers':'Authorization','Cache-Control':'no-store'})

    @router.get('/api/loyalty/pos/customers/{customer_id}', include_in_schema=False)
    async def customer_points(request: Request, customer_id: str):
        try:
            access = POSAccess.from_settings(settings)
        except ValueError:
            return error('pos_read_unavailable',503)
        if not budget.allow(settings.loyalty_pos_total_requests):
            return error('rate_limited',429)
        headers = request.scope['headers']
        if sum(len(k)+len(v) for k,v in headers) > 8192 or len(request.headers.get('authorization','')) > 4096:
            return error('request_too_large',431)
        if len(request.scope.get('query_string',b'')) > 2048 or len(request.url.path) > 200:
            return error('invalid_request',400)
        if len(request.headers.getlist('authorization')) != 1:
            return error('authentication_required',401)
        try:
            session = access.authenticate(request.headers.get('authorization'))
        except AuthenticationRequired:
            return error('authentication_required',401)
        if not budget.allow(settings.loyalty_pos_session_requests, session):
            return error('rate_limited',429)
        # A GET must not carry a request body. Do not buffer attacker-controlled bodies.
        if request.headers.get('transfer-encoding') or request.headers.get('content-length','0') != '0':
            return error('invalid_request',400)
        async for chunk in request.stream():
            if chunk:return error('invalid_request',400)
        params = request.query_params
        if any(k not in ('cursor','limit') or len(params.getlist(k)) != 1 for k in params):
            return error('invalid_request',400)
        if not re.fullmatch(r'[1-9][0-9]{0,15}', customer_id) or int(customer_id) > 9007199254740991:
            return error('invalid_request',400)
        if not re.fullmatch(r'[1-9][0-9]?', params.get('limit','25')) or int(params.get('limit','25')) > 25:
            return error('invalid_request',400)
        try:
            cursor = access.decode_cursor(params['cursor'], customer_id) if 'cursor' in params else None
        except ValueError:
            return error('invalid_cursor',400)
        try:
            # SQL work is synchronous: keep the ASGI loop free for request bounds.
            from starlette.concurrency import run_in_threadpool
            def query():
                if not schema_ready(engine):return None
                return ledger_snapshot(engine, access.shop, customer_id, limit=int(params.get('limit','25')), cursor=cursor)
            snapshot = await run_in_threadpool(query)
            if snapshot is None:return error('pos_read_unavailable',503)
            payload, next_page = snapshot
        except SnapshotChanged:
            return error('refresh_required',409)
        except SQLAlchemyError:
            log.warning('loyalty POS read unavailable')
            return error('pos_read_unavailable',503)
        payload['next_cursor'] = access.encode_cursor(next_page) if next_page else None
        payload['posting_paused'] = not (settings.loyalty_processing_enabled and settings.loyalty_posting_enabled)
        return JSONResponse(payload, headers={'Cache-Control':'no-store'})

    return router
