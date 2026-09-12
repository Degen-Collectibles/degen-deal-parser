"""Internal Ops balances/history/exceptions; no public or Shopify write surface."""
import json
import re
from uuid import uuid4
from fastapi import APIRouter,Form,HTTPException,Query,Request
from fastapi.responses import RedirectResponse
from sqlalchemy import func
from sqlmodel import Session,select
from ..auth import has_permission
from ..config import get_settings
from ..csrf import CSRFProtectedRoute
from ..db import engine
from ..models import User
from ..loyalty.config import posting_gates,processing_gates,policy_from_settings
from ..loyalty.domain import Review,gid,fingerprint
from ..loyalty.models import LoyaltyAccount,LoyaltyEntitlement,LoyaltyInbox,LoyaltyLedger,LoyaltyReconciliation
from ..loyalty.schema import schema_ready
from ..loyalty.service import Store
from ..loyalty import ops
from .. import shared

settings=get_settings()
store=Store(engine)
router=APIRouter(route_class=CSRFProtectedRoute)


def authorize(session,request,permission):
    # Resolve against the current DB row on EVERY request, including revoked or
    # stale cookie sessions; do not trust a cached role or legacy role ranking.
    data=request.scope.get('session') or {}
    uid=data.get('user_id')
    user=session.get(User,uid) if type(uid) is int else None
    if (not user or not user.is_active or shared._password_session_is_stale(user,data)
        or shared._session_invalidation_is_stale(user,data)):
        raise HTTPException(401,'authentication_required')
    if not has_permission(session,user,permission):raise HTTPException(403,'permission_denied')
    request.state.current_user=user
    return user


def auth(request,permission):
    with Session(store.engine) as session:return authorize(session,request,permission)


LABELS={
    'eligible':'Points posted','shadow':'Calculated · posting paused','pending':'Awaiting update',
    'review':'Needs review','excluded':'Not eligible','queued':'Waiting','processing':'Checking','done':'Checked',
    'calculated':'Eligible POS purchase','earned':'Purchase points','refund':'Refund adjustment',
    'customer_unattached':'Attach an existing customer in Shopify POS',
    'awaiting_canonical':'Waiting for Shopify verification',
    'attachment_outside_window':'Attachment timing needs verification',
    'attachment_timing_unverified':'Attachment timing needs verification',
    'verified_attachment_awaiting_canonical':'Attachment verified; waiting for Shopify check',
    'identity_reassignment':'Customer changed; administrator review required',
    'identity_merge_review':'Customer merge needs administrator review',
    'manual_correction_hold':'Administrator correction; further changes paused',
    'edit_or_exchange_review':'Edited order or exchange needs review',
    'payment_unverified':'Waiting for confirmed payment',
    'channel_excluded':'Online purchases are excluded','prelaunch':'Purchase predates launch',
    'location_excluded':'Store location is outside this program',
    'refund_allocation_unverified':'Refund details need administrator review',
}


def display_reason(value):
    return LABELS.get(value,(value or 'Awaiting update').replace('_',' ').capitalize())


@router.get('/loyalty')
def loyalty_page(request:Request,customer_id:str=Query(default='',max_length=100),order_id:str=Query(default='',max_length=100),page:int=Query(default=0,ge=0,le=100000)):
    user=auth(request,'ops.loyalty.view')
    try:
        customer=gid('Customer',customer_id) if customer_id else None
        order=gid('Order',order_id) if order_id else None
    except Review:raise HTTPException(400,'use_shopify_customer_or_order_id') from None
    ready=schema_ready(store.engine)
    history_orders={};rows=[];history=[];inbox=[];scan=None;balance=None;counts=[];can_correct=False;can_replay=False
    if ready:
        with Session(store.engine) as s:
            can_correct=has_permission(s,user,'admin.loyalty.correct');can_replay=has_permission(s,user,'admin.loyalty.reconcile')
            query=select(LoyaltyEntitlement).where(LoyaltyEntitlement.shop==settings.loyalty_shop_domain)
            if customer:query=query.where(LoyaltyEntitlement.customer_id==customer)
            if order:query=query.where(LoyaltyEntitlement.order_id==order)
            rows=s.exec(query.order_by(LoyaltyEntitlement.id.desc()).offset(page*50).limit(50)).all()
            ledger=select(LoyaltyLedger).where(LoyaltyLedger.shop==settings.loyalty_shop_domain)
            if customer:
                ledger=ledger.join(LoyaltyAccount,LoyaltyAccount.id==LoyaltyLedger.account_id).where(LoyaltyAccount.customer_id==customer)
                balance=store.balance(settings.loyalty_shop_domain,customer)
            if order:
                ledger=ledger.join(LoyaltyEntitlement,LoyaltyEntitlement.id==LoyaltyLedger.entitlement_id).where(LoyaltyEntitlement.order_id==order)
            history=s.exec(ledger.order_by(LoyaltyLedger.id.desc()).offset(page*50).limit(50)).all()
            history_orders=dict(s.exec(select(LoyaltyEntitlement.id,LoyaltyEntitlement.order_id).where(
                LoyaltyEntitlement.shop==settings.loyalty_shop_domain,LoyaltyEntitlement.id.in_([e.entitlement_id for e in history]))).all())
            counts=s.exec(select(LoyaltyEntitlement.status,func.count()).where(LoyaltyEntitlement.shop==settings.loyalty_shop_domain).group_by(LoyaltyEntitlement.status)).all()
            if can_replay:
                inbox=s.exec(select(LoyaltyInbox).where(LoyaltyInbox.shop==settings.loyalty_shop_domain,LoyaltyInbox.status.in_(['queued','processing','review'])).order_by(LoyaltyInbox.received_at).limit(50)).all()
                scan=s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.shop==settings.loyalty_shop_domain)).first()
    gates=list(dict.fromkeys(posting_gates(settings)+([] if ready else ['schema_not_ready'])))
    response=shared.templates.TemplateResponse(request=request,name='loyalty.html',context=dict(request=request,current_user=user,title='POS loyalty',
        rows=rows,history=history,history_orders=history_orders,inbox=inbox,scan=scan,balance=balance,counts=counts,customer_id=customer_id,order_id=order_id,
        page=page,gates=gates,receiving=settings.loyalty_receiving_enabled,processing=settings.loyalty_processing_enabled,posting=settings.loyalty_posting_enabled,
        can_correct=can_correct,can_replay=can_replay,diagnostics=can_correct or can_replay,
        evidence_hashes={row.id:fingerprint(json.loads(row.observed_evidence_json)) for row in rows} if can_correct else {},
        display_reason=display_reason,display_id=lambda value:value.rsplit('/',1)[-1] if value else 'Unattached',
        display_money=lambda value:f'${value//100:,}.{value%100:02d}',request_key=uuid4().hex,shop=settings.loyalty_shop_domain))
    response.headers['Cache-Control']='no-store';return response


@router.post('/loyalty/replay')
def replay_orders(request:Request,ids:str=Form(max_length=300),request_key:str=Form(max_length=80)):
    auth(request,'admin.loyalty.reconcile')
    if not schema_ready(store.engine):raise HTTPException(503,'loyalty_schema_not_ready')
    try:
        if not re.fullmatch(r'[0-9]+(?:,[0-9]+){0,24}',ids):raise ValueError()
        ops.replay(store,settings.loyalty_shop_domain,[int(v) for v in ids.split(',')],request_key,lambda s,p:authorize(s,request,p))
    except ValueError:raise HTTPException(400,'invalid_replay_request') from None
    return RedirectResponse('/loyalty',status_code=303)


@router.post('/loyalty/correct')
def correct_order(request:Request,entitlement_id:int=Form(gt=0),expected_revision:int=Form(ge=0),target_points:int=Form(ge=0),
                  request_key:str=Form(max_length=80),reason:str=Form(max_length=40),reference:str=Form(max_length=80)):
    auth(request,'admin.loyalty.correct')
    if posting_gates(settings) or not schema_ready(store.engine):raise HTTPException(503,'loyalty_posting_not_ready')
    try:
        ops.correct(store,settings.loyalty_shop_domain,entitlement_id,expected_revision,target_points,request_key,reason,reference,lambda s,p:authorize(s,request,p))
    except Review:raise HTTPException(409,'loyalty_correction_conflict') from None
    except ValueError:raise HTTPException(400,'invalid_correction_request') from None
    return RedirectResponse('/loyalty',status_code=303)


@router.post('/loyalty/rebuild-check')
def rebuild_check(request:Request):
    user=auth(request,'admin.loyalty.reconcile')
    if not schema_ready(store.engine):raise HTTPException(503,'loyalty_schema_not_ready')
    differences=store.rebuild_check(settings.loyalty_shop_domain)
    from ..models import AuditLog
    def record(s):
        authorize(s,request,'admin.loyalty.reconcile')
        s.add(AuditLog(actor_user_id=user.id,action='loyalty.rebuild_check',resource_key='admin.loyalty.reconcile',details_json=json.dumps({'mismatch_count':len(differences)})))
    store.transaction(record)
    return {'matches':not differences,'differences':differences}


@router.post('/loyalty/verify-attachment')
def verify_order_attachment(request:Request,entitlement_id:int=Form(gt=0),expected_revision:int=Form(ge=0),
                            evidence_hash:str=Form(min_length=64,max_length=64),attached_at:str=Form(max_length=40),
                            request_key:str=Form(max_length=80),reference:str=Form(max_length=80)):
    auth(request,'admin.loyalty.correct')
    if not schema_ready(store.engine):raise HTTPException(503,'loyalty_schema_not_ready')
    try:
        policy=policy_from_settings(settings)
        ops.verify_attachment(store,policy,entitlement_id,expected_revision,evidence_hash,attached_at,request_key,reference,
                              lambda s,p:authorize(s,request,p))
    except Review:raise HTTPException(409,'loyalty_attachment_conflict') from None
    except ValueError:raise HTTPException(400,'invalid_attachment_request') from None
    return RedirectResponse('/loyalty',status_code=303)
