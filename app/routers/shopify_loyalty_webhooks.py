"""Dedicated durable loyalty receiver; deliberately independent of inventory."""
import asyncio
import hashlib
import json
import logging
import re
from fastapi import APIRouter, HTTPException, Request
from ..config import get_settings
from ..db import engine
from ..inventory.shopify_ingest import validate_shopify_webhook
from ..loyalty.config import receiving_gates
from ..loyalty.domain import Review, gid, timestamp
from ..loyalty.schema import schema_ready
from ..loyalty.service import Store

router=APIRouter()
settings=get_settings()
store=Store(engine)
log=logging.getLogger(__name__)
TOPICS=frozenset({'orders/create','orders/paid','orders/updated','orders/cancelled','refunds/create','customers/merge'})


def unique_object(pairs):
    result={}
    for key,value in pairs:
        if key in result: raise ValueError('duplicate_json_key')
        result[key]=value
    return result


def minimize(payload,topic,triggered_at=None):
    if not isinstance(payload,dict): raise Review('invalid_payload')
    if topic=='customers/merge':
        ids=[gid('Customer',payload[k]) for k in ('admin_graphql_api_customer_kept_id','admin_graphql_api_customer_deleted_id')]
        status=payload.get('status')
        # All states, including failed/in-progress, are REVIEW, never an alias.
        if status not in ('completed','succeeded','failed','in_progress','pending'):
            status='unknown'
        return {'merge_ids':ids,'merge_status':status}
    order_id=gid('Order',payload.get('order_id') if topic=='refunds/create' else payload.get('id'))
    evidence={'order_id':order_id}
    if topic=='refunds/create':
        evidence['refund_id']=gid('Refund',payload.get('id'))
        return evidence
    if payload.get('admin_graphql_api_id') and gid('Order',payload['admin_graphql_api_id'])!=order_id:
        raise Review('order_identity_mismatch')
    if payload.get('updated_at'):
        evidence['updated_at']=timestamp(payload['updated_at']).isoformat()
    customer=payload.get('customer')
    if customer is not None:
        if not isinstance(customer,dict): raise Review('invalid_customer')
        evidence['customer_id']=gid('Customer',customer.get('id'))
        # Event headers are outside the body HMAC. No webhook is historical
        # attachment proof; processing observes the canonical association locally.
    return evidence


@router.post('/webhooks/shopify/loyalty')
async def receive_loyalty(request: Request):
    if receiving_gates(settings):
        raise HTTPException(503,'loyalty_receiving_not_ready')
    if not await asyncio.to_thread(schema_ready,store.engine):
        raise HTTPException(503,'loyalty_schema_not_ready')
    if request.headers.get('x-shopify-shop-domain')!=settings.loyalty_shop_domain:
        raise HTTPException(403,'shop_not_allowed')
    topic=request.headers.get('x-shopify-topic','')
    if topic not in TOPICS:
        raise HTTPException(400,'topic_not_allowed')
    delivery=request.headers.get('x-shopify-webhook-id','')
    event=request.headers.get('x-shopify-event-id','')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',delivery) or (event and not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',event)):
        raise HTTPException(400,'invalid_delivery_id')
    limit=settings.loyalty_body_limit_bytes
    length=request.headers.get('content-length')
    if length and (not length.isdigit() or int(length)>limit):
        raise HTTPException(413,'body_too_large')
    if request.headers.get('content-encoding','identity')!='identity':
        raise HTTPException(415,'unsupported_content_encoding')
    async def bounded_body():
        chunks=[];size=0
        async for chunk in request.stream():
            size+=len(chunk)
            if size>limit:raise HTTPException(413,'body_too_large')
            chunks.append(chunk)
        return b''.join(chunks)
    try: body=await asyncio.wait_for(bounded_body(),timeout=3)
    except asyncio.TimeoutError: raise HTTPException(408,'body_timeout') from None
    signature=request.headers.get('x-shopify-hmac-sha256','')
    if not re.fullmatch(r'[A-Za-z0-9+/]{43}=',signature) or not validate_shopify_webhook(raw_body=body,shared_secret=settings.shopify_webhook_secret,received_hmac=signature):
        raise HTTPException(401,'invalid_hmac')
    try:
        payload=json.loads(body,object_pairs_hook=unique_object,parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        evidence=minimize(payload,topic,request.headers.get('x-shopify-triggered-at'))
        if event:evidence['event_id']=event
    except (ValueError,KeyError,TypeError,UnicodeError,RecursionError):
        log.warning('loyalty receiver rejected invalid evidence topic=%s',topic)
        raise HTTPException(400,'invalid_loyalty_evidence') from None
    try:
        inbox_id=await asyncio.to_thread(store.receive,settings.loyalty_shop_domain,delivery,topic,evidence,hashlib.sha256(body).hexdigest())
    except ValueError:
        raise HTTPException(409,'delivery_conflict') from None
    except Exception:
        log.error('loyalty receipt commit failed topic=%s',topic)
        raise HTTPException(503,'loyalty_receipt_not_committed') from None
    return {'accepted':True,'receipt_id':inbox_id}
