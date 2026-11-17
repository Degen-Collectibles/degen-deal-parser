"""Shopify bearer-only, bounded reward actions. Disabled by default."""
import asyncio
import json
import re
import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from ..loyalty.pos_auth import POSAccess, AuthenticationRequired
from ..loyalty.reward_policy import Reward, redemption_gates, variant_ids
from ..loyalty.redemption import Redemptions, RedemptionError, prepare, reconcile
from ..loyalty.redemption_shopify import DiscountProvider
from ..loyalty.schema import schema_ready
from ..loyalty.service import Store
from ..loyalty.config import posting_gates, policy_from_settings
from ..inventory.shopify import resolve_shopify_access_token
from .loyalty_pos import POSRoute, ReadBudget, SHOPIFY_ORIGINS

PATH='/api/loyalty/pos/customers/{customer_id}/rewards'


def build_router(engine, settings, provider_factory=None):
    router=APIRouter(route_class=POSRoute); budget=ReadBudget(); redemptions=Redemptions(Store(engine), posting_allowed=lambda: not posting_gates(settings), policy_getter=lambda: policy_from_settings(settings))
    def response(value,status=200):
        return JSONResponse(value,status_code=status,headers={'Cache-Control':'no-store'})

    def authorize(request, customer_id):
        if redemption_gates(settings, issuing=False): raise RedemptionError('redemption_unavailable')
        if provider_factory is None and not resolve_shopify_access_token(settings):
            raise RedemptionError('redemption_unavailable')
        access=POSAccess.from_settings(settings)
        if not budget.allow(settings.loyalty_pos_total_requests): raise RedemptionError('rate_limited')
        if sum(len(k)+len(v) for k,v in request.scope['headers'])>8192:
            raise RedemptionError('invalid_request')
        if len(request.headers.getlist('authorization'))!=1:
            raise AuthenticationRequired()
        if request.url.query:
            raise RedemptionError('invalid_request')
        session=access.authenticate(request.headers.get('authorization'))
        if not budget.allow(settings.loyalty_pos_session_requests,session): raise RedemptionError('rate_limited')
        if not re.fullmatch(r'[1-9][0-9]{0,15}',customer_id) or int(customer_id)>9007199254740991:
            raise RedemptionError('invalid_request')
        origin=request.headers.get('origin')
        if origin and origin not in SHOPIFY_ORIGINS: raise RedemptionError('invalid_origin')
        if not schema_ready(engine): raise RedemptionError('redemption_unavailable')
        return access, session

    def failure(exc):
        if isinstance(exc,AuthenticationRequired): return response({'error':'authentication_required'},401)
        if isinstance(exc,RedemptionError):
            code=str(exc)
            return response({'error':code},503 if code=='redemption_unavailable' else 429 if code=='rate_limited' else 403 if code=='invalid_origin' else 400 if code in ('invalid_request','invalid_request_key','invalid_variants') else 409)
        return response({'error':'redemption_unavailable'},503)

    @router.options(PATH,include_in_schema=False)
    async def preflight(request:Request,customer_id:str):
        if redemption_gates(settings, issuing=False): return response({'error':'redemption_unavailable'},503)
        if (request.headers.get('origin') not in SHOPIFY_ORIGINS or
            request.headers.get('access-control-request-method') not in ('GET','POST') or
            not {x.strip().lower() for x in request.headers.get('access-control-request-headers','').split(',') if x.strip()} <= {'authorization','content-type'}):
            return response({'error':'invalid_preflight'},403)
        return Response(status_code=204,headers={'Access-Control-Allow-Methods':'GET, POST',
            'Access-Control-Allow-Headers':'Authorization, Content-Type','Cache-Control':'no-store'})

    @router.get(PATH,include_in_schema=False)
    async def status(request:Request,customer_id:str):
        try:
            access,session=authorize(request,customer_id)
            if request.headers.get('content-length','0')!='0' or request.headers.get('transfer-encoding'):
                raise RedemptionError('invalid_request')
            data=await asyncio.to_thread(redemptions.overview,access.shop,customer_id)
            pending=await asyncio.to_thread(redemptions.owned_pending,access.shop,customer_id,session)
            return response(dict(data,customer_id=customer_id,reward=Reward.from_settings(settings).public(),pending=pending,
                issuance_enabled=not bool(redemption_gates(settings)) and await asyncio.to_thread(redemptions.issuance_ready, access.shop)))
        except Exception as exc:return failure(exc)

    @router.post(PATH,include_in_schema=False)
    async def action(request:Request,customer_id:str):
        try:
            access,session=authorize(request,customer_id)
            if request.headers.get('content-type','').split(';')[0]!='application/json':
                raise RedemptionError('invalid_request')
            raw=bytearray()
            async for part in request.stream():
                raw.extend(part)
                if len(raw)>8192: raise RedemptionError('invalid_request')
            try: data=json.loads(raw)
            except (ValueError, UnicodeError): raise RedemptionError('invalid_request') from None
            if not isinstance(data,dict): raise RedemptionError('invalid_request')
            kind=data.get('action')
            if kind in ('eligibility','reserve'):
                if redemption_gates(settings) or not await asyncio.to_thread(redemptions.issuance_ready, access.shop):
                    raise RedemptionError('redemption_unavailable')
                expected={'action','variant_ids'} | ({'request_key'} if kind=='reserve' else set())
                custom = data.get('custom_sales') is True
                if custom: expected.add('custom_sales')
                if set(data)!=expected: raise RedemptionError('invalid_request')
                try: requested=variant_ids(data['variant_ids'], allow_empty=custom)
                except ValueError: raise RedemptionError('invalid_request') from None
            elif kind in ('cancel','refresh') and set(data)=={'action','redemption_id'} and type(data['redemption_id']) is int:
                row=await asyncio.to_thread(redemptions.get,access.shop,data['redemption_id'],customer_id,session)
                if kind=='cancel':
                    row=await asyncio.to_thread(redemptions.transition,access.shop,row.id,'cancelling')
            else: raise RedemptionError('invalid_request')
            async with httpx.AsyncClient(timeout=httpx.Timeout(15,connect=5),follow_redirects=False) as client:
                provider=provider_factory(client) if provider_factory else DiscountProvider(client,access.shop,
                    resolve_shopify_access_token(settings),settings.loyalty_shop_id,settings.loyalty_location_ids)
                if kind in ('eligibility','reserve'):
                    eligibility=await asyncio.wait_for(provider.eligibility(requested, **({'custom_sales':True} if custom else {})),timeout=15)
                    if kind=='eligibility':
                        return response({'eligible_variant_ids':eligibility['eligible'],'taxes_included':False,
                                         'custom_sales':eligibility.get('custom_sales',False)})
                    row=await asyncio.to_thread(redemptions.reserve,access.shop,customer_id,session,
                        data['request_key'],Reward.from_settings(settings),eligibility=eligibility)
                row=await asyncio.wait_for(prepare(redemptions,provider,row) if kind=='reserve'
                                           else reconcile(redemptions,provider,row, allow_prepare=not bool(redemption_gates(settings)),
                                                          cashier_cancel=kind=='cancel'),timeout=45)
            return response(dict(redemption_id=row.id,state=row.state,points=row.points,
                can_cancel=row.debit_ledger_id is None and row.state in ('reserved','ready','review','cancelling'),
                discount_cents=row.discount_cents,minimum_cents=row.minimum_cents,
                eligible_variant_ids=json.loads(row.eligibility_json).get('eligible',[]),
                custom_sales=json.loads(row.eligibility_json).get('custom_sales',False),
                code=row.code if row.state=='ready' else None))
        except Exception as exc:return failure(exc)
    return router
