"""DEV-only integrated earning/redemption test. No production imports or webhooks.

Uses a private persistent scratch ledger during one bounded preview session.
The only customer admitted is the verified Loyalty Test customer in the DEV shop.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime,timezone
import json,os,sys,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.loyalty_native import DEV_SHOP,DEV_CLIENT,private_secret,preview_host
SHOP_ID='gid://shopify/Shop/81339547671'
CUSTOMER='9588918190103'
LOCATION='104761688087'
INPUTS=Path('/tmp/degen-loyalty-native-handoff/checkout-inputs.json')


def seed(engine):
    from sqlmodel import Session,select
    from app.loyalty.models import LoyaltyAccount,LoyaltyEntitlement,LoyaltyLedger
    with Session(engine) as s:
        if s.exec(select(LoyaltyAccount)).first(): return
        account=LoyaltyAccount(shop=DEV_SHOP,customer_id='gid://shopify/Customer/'+CUSTOMER)
        s.add(account);s.flush()
        entry=LoyaltyEntitlement(shop=DEV_SHOP,order_id='gid://shopify/Order/1',account_id=account.id,
            customer_id=account.customer_id,posted_points=500,revision=1,status='eligible')
        s.add(entry);s.flush()
        s.add(LoyaltyLedger(shop=DEV_SHOP,account_id=account.id,entitlement_id=entry.id,revision=1,
            business_key='native-test-starting-balance',delta=500,resulting_points=500,reason='earned',
            rule_version='synthetic-dev-test-credit',evidence_hash='a'*64,evidence_json='{"synthetic":true}'))
        s.commit()


class IntegratedProvider:
    """Feed only this verified DEV customer's reward orders to the real worker."""
    def __init__(self, provider, store, settings):
        self.provider=provider; self.store=store; self.settings=settings

    async def eligibility(self, requested, **kwargs): return await self.provider.eligibility(requested, **kwargs)
    async def ensure_discount(self, row):
        self.provider.issuance_check = getattr(self, 'issuance_check', None)
        return await self.provider.ensure_discount(row)
    async def deactivate(self, row): return await self.provider.deactivate(row)
    async def confirmed_unused(self, row): return await self.provider.confirmed_unused(row)

    async def order_evidence(self, row):
        from app.loyalty.config import policy_from_settings, posting_gates
        from app.loyalty.domain import fingerprint
        from app.loyalty.worker import process_one
        self.provider.observation = getattr(self, 'observation', None)
        evidence=await self.provider.order_evidence(row)
        if evidence is None: return None
        canonical=evidence['canonical_order']
        if (row.shop!=DEV_SHOP or row.customer_id!='gid://shopify/Customer/'+CUSTOMER
            or canonical['customer_id']!=row.customer_id or canonical['shop_id']!=SHOP_ID
            or canonical['location_id']!='gid://shopify/Location/'+LOCATION
            or canonical['id']!=evidence['order_id'] or posting_gates(self.settings)):
            raise ValueError('DEV earning bounds failed')
        digest=fingerprint(canonical)
        observed=evidence['observed_at']
        await asyncio.to_thread(self.store.receive,DEV_SHOP,'dev-reward-'+digest,'orders/updated',
            dict(order_id=canonical['id'],customer_id=row.customer_id,updated_at=canonical['updated_at']),digest,observed)
        class BoundedReader:
            async def order(self, order_id, shop_id):
                if order_id!=canonical['id'] or shop_id!=SHOP_ID:
                    raise ValueError('DEV order outside current reward')
                return canonical
        await process_one(self.store,BoundedReader(),policy_from_settings(self.settings),self.settings)
        return evidence


def create_app(engine,secret,host,deadline,provider_factory):
    from fastapi import FastAPI,Request
    from fastapi.responses import JSONResponse
    from app.config import Settings
    from app.loyalty.redemption import Redemptions,reconcile
    from app.loyalty.service import Store
    from app.routers.loyalty_pos import build_router as reads
    from app.routers.loyalty_redemption import build_router as actions
    settings=Settings(_env_file=None,LOYALTY_SHOP_DOMAIN=DEV_SHOP,LOYALTY_SHOP_ID=SHOP_ID,
        LOYALTY_LOCATION_IDS=LOCATION,LOYALTY_POS_CLIENT_ID=DEV_CLIENT,LOYALTY_POS_CLIENT_SECRET=secret,
        LOYALTY_POS_READ_ENABLED=True,LOYALTY_POS_ALL_STAFF_ENABLED=True,
        LOYALTY_REDEMPTION_ENABLED=True,LOYALTY_REDEMPTION_ALL_STAFF_ENABLED=True,
        LOYALTY_REDEMPTION_PROVIDER_VERIFIED=True,LOYALTY_RECEIVING_ENABLED=False,
        LOYALTY_PROCESSING_ENABLED=True,LOYALTY_POSTING_ENABLED=True,
        SHOPIFY_STORE_DOMAIN=DEV_SHOP,LOYALTY_LAUNCH_AT='2026-09-01T00:00:00Z',
        LOYALTY_READ_ACCESS_VERIFIED=True,LOYALTY_EVIDENCE_RETENTION_DAYS=7,
        LOYALTY_BUDGET_CENTS=0,LOYALTY_EXCEPTION_OWNER='DEV test operator',
        LOYALTY_LEDGER_RETENTION_DAYS=7,LOYALTY_BACKUP_RETENTION_DAYS=7)
    from app.loyalty.config import posting_gates, policy_from_settings
    rewards=Redemptions(Store(engine), posting_allowed=lambda: not posting_gates(settings),
        policy_getter=lambda: policy_from_settings(settings));stop=asyncio.Event()
    raw_factory=provider_factory
    provider_factory=lambda client:IntegratedProvider(raw_factory(client),rewards.store,settings)
    async def poll():
        while not stop.is_set() and time.time()<deadline:
            try:
                for row in await asyncio.to_thread(rewards.pending,DEV_SHOP,5,history_seconds=30):
                    try:await asyncio.wait_for(reconcile(rewards,provider_factory(None),row),45)
                    except asyncio.TimeoutError as exc:
                        await asyncio.to_thread(rewards.failed,row.shop,row.id,exc)
                    except Exception:print('DEV reward reconciliation pending; reservation retained.',flush=True)
                    finally:await asyncio.to_thread(rewards.checked,row.shop,row.id)
            except Exception:print('DEV reward poll failed; ledger retained.',flush=True)
            try:await asyncio.wait_for(stop.wait(),5)
            except asyncio.TimeoutError:pass
    @asynccontextmanager
    async def lifespan(app):
        task=asyncio.create_task(poll())
        try:yield
        finally:
            stop.set();task.cancel()
            try:await task
            except asyncio.CancelledError:pass
            engine.dispose()
    app=FastAPI(docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)
    app.include_router(reads(engine,settings));app.include_router(actions(engine,settings,provider_factory))
    base='/api/loyalty/pos/customers/'+CUSTOMER
    @app.middleware('http')
    async def bounds(request:Request,call_next):
        if time.time()>=deadline:return JSONResponse({'error':'preview_expired'},503)
        if request.headers.get('host') not in (host,'127.0.0.1:8769','testserver'):
            return JSONResponse({'error':'invalid_host'},403)
        if request.url.path not in ('/health',base,base+'/rewards'):
            return JSONResponse({'error':'not_found'},404)
        response=await call_next(request);response.headers['Cache-Control']='no-store'
        return response
    @app.get('/health')
    async def health():return {'status':'ok','mode':'dev-redemption-checkout','earning_enabled':True}
    app.state.rewards=rewards
    app.state.loyalty_settings=settings
    return app


async def serve():
    url=os.environ.get('APP_URL') or os.environ.get('HOST')
    host=preview_host(url)
    if os.environ.get('SHOPIFY_API_KEY')!=DEV_CLIENT or os.environ.get('PORT')!='8769' or not host:
        raise ValueError('Exact DEV CLI context required')
    # CLI injects this app's secret into the bounded DEV backend process. Capture
    # it before synthetic isolation clears the environment; never print/store it.
    from pydantic import SecretStr
    cli_secret=os.environ.get('SHOPIFY_API_SECRET')
    if cli_secret is not None and (not 32<=len(cli_secret)<=512 or
            any(ord(c)<33 or ord(c)>126 for c in cli_secret)):
        raise ValueError('Invalid DEV CLI secret')
    secret=SecretStr(cli_secret) if cli_secret else private_secret('/tmp/degen-loyalty-native-handoff/dev-app-secret')
    manifest=json.loads(INPUTS.read_text())
    scratch=Path(manifest['scratch']).resolve()
    if scratch.parent!=Path('/tmp') or not scratch.name.startswith('loyalty-checkout-'):
        raise ValueError('Private test scratch required')
    if time.time()>=manifest['deadline']:raise ValueError('DEV test session expired')
    from scripts.loyalty_synthetic import prepare_environment
    prepare_environment('loyalty-checkout-process-')
    from sqlmodel import create_engine
    from app.loyalty.schema import install_schema
    from app.loyalty.redemption_shopify import DiscountProvider
    import httpx,uvicorn
    async def restrict(request):
        if request.url.scheme!='https' or request.url.host!=DEV_SHOP:
            raise ValueError('Only the DEV Shopify host is permitted')
    async with httpx.AsyncClient(timeout=20,follow_redirects=False,trust_env=False,
                                 event_hooks={'request':[restrict]}) as client:
        response=await client.post(f'https://{DEV_SHOP}/admin/oauth/access_token',data={
            'grant_type':'client_credentials','client_id':DEV_CLIENT,'client_secret':secret.get_secret_value()})
        response.raise_for_status();token=response.json()['access_token']
        factory=lambda _:DiscountProvider(client,DEV_SHOP,token,SHOP_ID,LOCATION)
        identity=await factory(None).query('{ shop { id myshopifyDomain } customer(id:"gid://shopify/Customer/'+CUSTOMER+'") { id displayName } }',{})
        if identity['shop']!={'id':SHOP_ID,'myshopifyDomain':DEV_SHOP} or identity['customer']!={'id':'gid://shopify/Customer/'+CUSTOMER,'displayName':'Loyalty Test'}:
            raise ValueError('DEV identity mismatch')
        engine=create_engine('sqlite:///'+str(scratch/'checkout.db'),connect_args={'check_same_thread':False,'timeout':10})
        install_schema(engine);seed(engine)
        app=create_app(engine,secret,host,manifest['deadline'],factory)
        print('Verified DEV checkout backend: Loyalty Test, 500 initial synthetic points; DEV scratch earning enabled.',flush=True)
        await uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=8769,access_log=False,loop='asyncio')).serve()

if __name__=='__main__':
    os.umask(0o077)
    try:asyncio.run(serve())
    except Exception:raise SystemExit('DEV checkout launch failed; private details suppressed.') from None
