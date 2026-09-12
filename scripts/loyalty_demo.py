"""Local-only loyalty walkthrough. Never imported by the production application.

Run with the existing app interpreter: python scripts/loyalty_demo.py
Every launch uses a fresh synthetic database and home; no app lifespan or providers.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
import socket
import sys

ROOT = Path(__file__).resolve().parents[1]


def create_demo(scratch):
    """Build an isolated ASGI wrapper around the real loyalty router."""
    import os
    if not os.environ.get('LOYALTY_SYNTHETIC_ROOT'):
        raise RuntimeError('Use the synthetic launcher before importing the app')
    from fastapi import APIRouter, FastAPI, Form, HTTPException, Request
    from fastapi.responses import HTMLResponse, RedirectResponse
    from fastapi.staticfiles import StaticFiles
    from starlette.middleware.sessions import SessionMiddleware
    from starlette.middleware.trustedhost import TrustedHostMiddleware
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    from sqlmodel import SQLModel, Session, create_engine
    from app import shared
    from app.config import get_settings
    from app.csrf import CSRFProtectedRoute, issue_token, rotate_token
    from app.models import User, RolePermission, AuditLog
    from app.loyalty.domain import Policy, fingerprint
    from app.loyalty.service import Store
    from app.loyalty.schema import install_schema
    from app.loyalty.access import seed_loyalty_permissions
    from app.routers import loyalty

    scratch=Path(scratch);scratch.mkdir(parents=True,exist_ok=True)
    engine=create_engine('sqlite:///'+(scratch/'demo.db').as_posix(),connect_args={'check_same_thread':False})
    SQLModel.metadata.create_all(engine,tables=[User.__table__,RolePermission.__table__,AuditLog.__table__])
    install_schema(engine);store=Store(engine)
    now=datetime.now(timezone.utc).replace(microsecond=0)
    sale=now-timedelta(days=2)
    policy=Policy('demo.myshopify.com','gid://shopify/Shop/1',frozenset({'gid://shopify/Location/2'}),now-timedelta(days=30))
    users={}
    with Session(engine) as session:
        seed_loyalty_permissions(session)
        for role in ('cashier','admin'):
            user=User(username='demo-'+role,display_name='DEMO '+role.capitalize(),role=role,password_hash='no-password-login-in-demo')
            session.add(user);session.flush();users[role]=user.id
        session.add(RolePermission(role='cashier',resource_key='ops.loyalty.view',is_allowed=True));session.commit()

    def order(number,customer,paid=sale):
        return dict(id=f'gid://shopify/Order/{number}',shop_id=policy.shop_id,source='pos',location_id='gid://shopify/Location/2',
            created_at=paid.isoformat(),processed_at=paid.isoformat(),updated_at=paid.isoformat(),
            customer_id=f'gid://shopify/Customer/{customer}' if customer else None,test=False,edited=False,returns=False,exchanges=False,
            currency='USD',taxes_included=False,total_cents=1999,received_cents=1999,outstanding_cents=0,financial_status='PAID',
            payment_rounding_cents=0,refund_rounding_cents=0,
            lines=[dict(id=f'gid://shopify/LineItem/{number}',quantity=1,original_cents=1999,discounts_cents=[0],tax_cents=0,gift_card=False,merchandise=True)],
            payments=[dict(id=f'gid://shopify/OrderTransaction/{number}',kind='SALE',status='SUCCESS',cents=1999,rounding_cents=0,processed_at=paid.isoformat())],refunds=[])

    canonical={}
    def evaluate(evidence,observed):
        oid=evidence['id'];canonical[oid]=evidence
        store.receive(policy.shop_domain,secrets.token_hex(12),'orders/updated',{'order_id':oid},fingerprint(evidence),observed)
        for _ in range(25):
            lease=store.claim(policy.shop_domain,now=observed)
            if not lease:raise RuntimeError('Synthetic receipt was not claimable')
            store.finish(lease,canonical[lease.order_id],policy,posting=True,now=observed)
            if lease.order_id==oid:return
        raise RuntimeError('Process queued demo checks before adding another example')

    earned=order(1001,101);evaluate(earned,sale)
    refunded=order(1002,102);evaluate(refunded,sale)
    for number in (1,2):
        refunded['refunds'].append(dict(id=f'gid://shopify/Refund/{2000+number}',
            lines=[dict(id=f'gid://shopify/RefundLineItem/{2000+number}',line_id='gid://shopify/LineItem/1002',quantity=0,subtotal_cents=25,tax_cents=0)],
            shipping_cents=0,shipping_tax_cents=0,adjustments=False,duties=False,
            transactions=[dict(id=f'gid://shopify/OrderTransaction/{2000+number}',kind='REFUND',status='SUCCESS',cents=25,rounding_cents=0)]))
        refunded['updated_at']=(sale+timedelta(minutes=number)).isoformat();refunded['returns']=True
        evaluate(refunded,sale+timedelta(minutes=number))
    pending=order(1003,None);evaluate(pending,sale)
    review=order(1004,104);evaluate(review,sale)
    review['exchanges']=True;review['returns']=True;review['updated_at']=now.isoformat();evaluate(review,now)
    late=order(1005,105,now-timedelta(days=9));evaluate(late,now)
    samples={
        'earned':('Earned points','customer_id=101','Original $19.99 purchase earned 20 whole points.'),
        'refund':('Cumulative refund','customer_id=102','Two $0.25 refunds: 20 points after the first, 19 after the second. History shows +20 and −1.'),
        'pending':('Pending attachment','order_id=1003','Starts without a customer. Simulate attaching existing demo customer 103 within seven days.'),
        'review':('Exchange review','customer_id=104','A later exchange requires review. The existing 20 points remain attributable; no guessed adjustment.'),
        'late':('Attachment evidence review','customer_id=105','First observed after seven days. Admin may verify authoritative attachment evidence using the actual form. Demo case: attached '+(now-timedelta(days=8)).isoformat()),
        'all':('All demo orders','','All records on this server are synthetic.'),
    }
    previous=(loyalty.store,loyalty.settings,shared.managed_session)
    loyalty.store=store
    # Only this demo router's settings allow synthetic corrections. No real worker
    # is started and get_settings() still has all external/loyalty flags disabled.
    loyalty.settings=get_settings().model_copy(update=dict(loyalty_shop_domain=policy.shop_domain,shopify_store_domain=policy.shop_domain,
        loyalty_shop_id=policy.shop_id,loyalty_location_ids=','.join(policy.locations),loyalty_launch_at=policy.launch_at.isoformat(),
        loyalty_processing_enabled=True,loyalty_posting_enabled=True,loyalty_read_access_verified=True,
        loyalty_exception_owner='DEMO ONLY',loyalty_budget_cents=0,loyalty_evidence_retention_days=30,
        loyalty_ledger_retention_days=30,loyalty_backup_retention_days=30))
    @contextmanager
    def managed_session():
        with Session(engine) as session:yield session
    shared.managed_session=managed_session
    def restore():
        loyalty.store,loyalty.settings,shared.managed_session=previous
        engine.dispose()

    app=FastAPI(docs_url=None,redoc_url=None,openapi_url=None)
    app.state.demo_restore=restore;app.state.demo_store=store
    app.add_middleware(SessionMiddleware,secret_key=secrets.token_urlsafe(48),session_cookie='loyalty_demo_session',same_site='strict')
    app.add_middleware(TrustedHostMiddleware,allowed_hosts=['127.0.0.1'])
    @app.middleware('http')
    async def local_only(request,call_next):
        if not request.client or request.client.host!='127.0.0.1':return HTMLResponse('Local demo only',403)
        origin=request.headers.get('origin')
        if origin and origin!=str(request.base_url).rstrip('/'):return HTMLResponse('Local origin required',403)
        response=await call_next(request)
        response.headers['Cache-Control']='no-store'
        response.headers['Content-Security-Policy']="default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-src 'self'; frame-ancestors 'self'; form-action 'self'"
        return response
    demo=APIRouter(route_class=CSRFProtectedRoute)
    templates=Environment(loader=FileSystemLoader(str(ROOT/'scripts')),autoescape=select_autoescape(['html']))
    @demo.get('/')
    def index():return RedirectResponse('/demo',303)
    @demo.get('/demo',response_class=HTMLResponse)
    def home(request:Request,sample:str='earned'):
        if sample not in samples:raise HTTPException(400,'unknown_demo_sample')
        role=next((role for role,id in users.items() if id==request.session.get('user_id')),None)
        return templates.get_template('loyalty_demo.html').render(token=issue_token(request),role=role,sample=sample,samples=samples,
            target='/loyalty?'+samples[sample][1],description=samples[sample][2])
    @demo.post('/demo/select')
    def choose(request:Request,role:str=Form(),sample:str=Form()):
        if role not in users or sample not in samples:raise HTTPException(400,'unknown_demo_choice')
        request.session.clear();request.session['user_id']=users[role];rotate_token(request)
        return RedirectResponse('/demo?sample='+sample,303)
    @demo.post('/demo/attach')
    def attach(request:Request):
        loyalty.auth(request,'ops.loyalty.view')
        current=datetime.now(timezone.utc)
        pending['customer_id']='gid://shopify/Customer/103';pending['updated_at']=current.isoformat();evaluate(pending,current)
        return RedirectResponse('/demo?sample=pending',303)
    @demo.post('/demo/process')
    def process(request:Request,sample:str=Form(default='all')):
        loyalty.auth(request,'admin.loyalty.reconcile')
        if sample not in samples:raise HTTPException(400,'unknown_demo_sample')
        for _ in range(25):
            lease=store.claim(policy.shop_domain)
            if not lease:break
            store.finish(lease,canonical[lease.order_id],policy,posting=True)
        return RedirectResponse('/demo?sample='+sample,303)
    app.include_router(demo);app.include_router(loyalty.router)
    app.mount('/static',StaticFiles(directory=str(ROOT/'app/static')),name='static')
    return app


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,default=8766)
    args=parser.parse_args()
    if not 0<=args.port<=65535:parser.error('Port must be 0..65535')
    sys.path.insert(0,str(ROOT))
    from scripts.loyalty_synthetic import prepare_environment,block_external_network
    scratch=prepare_environment('degen-loyalty-demo-');block_external_network()
    app=create_demo(scratch)
    import uvicorn
    server_socket=socket.socket();server_socket.bind(('127.0.0.1',args.port));server_socket.listen(128)
    port=server_socket.getsockname()[1]
    print(f'DEMO ONLY — synthetic data at {scratch}',flush=True)
    print(f'Open http://127.0.0.1:{port}/demo — Ctrl+C stops this demo.',flush=True)
    try:
        uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=port,proxy_headers=False,log_level='warning')).run(sockets=[server_socket])
    finally:
        app.state.demo_restore();server_socket.close()


if __name__=='__main__':main()
