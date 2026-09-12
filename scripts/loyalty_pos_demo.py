"""Loopback-only synthetic phase-two preview. Never import into the real app.

/tmp/degen-loyalty-test-venv/bin/python scripts/loyalty_pos_demo.py --port 8767
Build the local extension package first. No Shopify credentials or network.
"""
import argparse
from pathlib import Path
import secrets
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def create_demo():
    from scripts.loyalty_synthetic import prepare_environment, block_external_network
    scratch=prepare_environment('loyalty-pos-demo-')
    block_external_network()
    import jwt
    from fastapi import FastAPI, Request
    from fastapi.responses import FileResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
    from sqlmodel import Session, create_engine
    from app.config import Settings
    from app.loyalty.models import LoyaltyAccount, LoyaltyEntitlement, LoyaltyLedger
    from app.loyalty.schema import install_schema
    from app.routers.loyalty_pos import build_router
    shop='synthetic-pos.myshopify.com';client='synthetic-pos-client';key=secrets.token_urlsafe(48)
    engine=create_engine('sqlite:///'+str(scratch/'pos.db'),connect_args={'check_same_thread':False})
    install_schema(engine)
    with Session(engine) as session:
        number=0
        for customer,deltas,status in [(101,[20],'eligible'),(102,[20,-1],'eligible'),(103,[],'pending'),(104,[12],'review'),(105,[1]*15,'eligible')]:
            account=LoyaltyAccount(shop=shop,customer_id=f'gid://shopify/Customer/{customer}')
            session.add(account);session.flush()
            number+=1
            entitlement=LoyaltyEntitlement(shop=shop,order_id=f'gid://shopify/Order/{number}',account_id=account.id,customer_id=account.customer_id,status=status,posted_points=sum(deltas),revision=len(deltas))
            session.add(entitlement);session.flush()
            running=0
            for revision,delta in enumerate(deltas,1):
                running+=delta
                session.add(LoyaltyLedger(shop=shop,account_id=account.id,entitlement_id=entitlement.id,revision=revision,business_key=f'demo-{customer}-{revision}',delta=delta,resulting_points=running,reason='refund' if delta<0 else 'earned',rule_version='synthetic',evidence_hash='a'*64,evidence_json='{}'))
        session.commit()
    values=dict(loyalty_shop_domain=shop,loyalty_shop_id='gid://shopify/Shop/1',loyalty_pos_read_enabled=True,loyalty_pos_all_staff_enabled=True,loyalty_pos_client_id=client,loyalty_pos_client_secret=key)
    config=Settings(**{Settings.model_fields[k].alias or k:v for k,v in values.items()})
    app=FastAPI(docs_url=None,redoc_url=None,openapi_url=None)
    app.include_router(build_router(engine,config))
    @app.middleware('http')
    async def local_only(request:Request,call_next):
        # Host/Origin checks also prevent DNS rebinding into the synthetic token route.
        host=request.headers.get('host','')
        expected=f'http://{host}'
        if host.split(':')[0]!='127.0.0.1' or (request.headers.get('origin') not in (None,expected)):
            return JSONResponse({'error':'local_demo_only'},status_code=403)
        response=await call_next(request)
        response.headers['Cache-Control']='no-store'
        response.headers['Content-Security-Policy']="default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'"
        return response
    @app.post('/demo/token')
    async def demo_token(request:Request):
        if request.headers.get('x-loyalty-demo')!='synthetic':return JSONResponse({'error':'demo_header_required'},status_code=403)
        now=int(time.time())
        return {'token':jwt.encode(dict(iss=f'https://{shop}/admin',dest=f'https://{shop}',aud=client,sub='42',sid='synthetic-browser-session',iat=now,nbf=now,exp=now+60),key,algorithm='HS256')}
    @app.get('/')
    async def home():return FileResponse(ROOT/'extensions/loyalty-pos/preview.html')
    app.mount('/assets',StaticFiles(directory=ROOT/'extensions/loyalty-pos/dist'),name='assets')
    return app


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,default=8767)
    args=parser.parse_args()
    if not 1024<=args.port<=65535 or args.port==8766:parser.error('Choose an unprivileged port other than the existing demo port 8766')
    app=create_demo()
    import uvicorn
    print(f'Synthetic browser preview (not native POS): http://127.0.0.1:{args.port}',flush=True)
    uvicorn.run(app,host='127.0.0.1',port=args.port,access_log=False,log_level='warning')
