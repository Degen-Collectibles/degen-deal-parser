"""Independent, bounded loyalty lifecycle. No inventory jobs or Shopify writes."""
import asyncio
from datetime import timedelta
import json
import logging
import httpx
from sqlmodel import Session
from .config import policy_from_settings, processing_gates, posting_gates
from .domain import Review
from .models import LoyaltyEntitlement
from .reconciliation import Reconciler
from .schema import schema_ready
from .service import LostLease, Store, naive
from .shopify_reader import Reader
from ..inventory.shopify import resolve_shopify_access_token

log=logging.getLogger(__name__)


async def process_one(store,reader,policy,settings,*,now=None):
    if not settings.loyalty_processing_enabled:return False
    lease=await asyncio.to_thread(store.claim,policy.shop_domain,now=now)
    if not lease:return False
    try:
        with Session(store.engine) as session:
            previous=session.get(LoyaltyEntitlement,lease.entitlement_id)
            created=json.loads(previous.evidence_json).get('created_at')
        if created and naive(created)<naive(now)-timedelta(days=60) and not getattr(settings,'loyalty_read_all_orders_verified',False):
            raise Review('older_order_access_unverified')
        evidence=await asyncio.wait_for(reader.order(lease.order_id,policy.shop_id),timeout=240)
        if not settings.loyalty_processing_enabled:
            raise Review('processing_paused')
        posting=settings.loyalty_posting_enabled and not posting_gates(settings)
        await asyncio.to_thread(store.finish,lease,evidence,policy,posting=posting,now=now)
    except LostLease:
        log.info('loyalty lease superseded order_id=%s',lease.order_id)
    except Exception as exc:
        reason=str(exc) if isinstance(exc,Review) else 'canonical_fetch_failed'
        try:await asyncio.to_thread(store.fail,lease,reason,now=now)
        except LostLease:log.info('loyalty failure from superseded worker order_id=%s',lease.order_id)
    return True


async def loyalty_loop(stop_event,settings,store):
    policy=policy_from_settings(settings)
    scanner=Reconciler(store,policy)
    async with httpx.AsyncClient(timeout=httpx.Timeout(20,connect=5)) as client:
        while not stop_event.is_set():
            if processing_gates(settings):
                log.warning('loyalty processing paused by configuration')
                return
            reader=Reader(client,policy.shop_domain,resolve_shopify_access_token(settings),request_limit=settings.loyalty_api_request_limit)
            try:
                # Recovery of known awards is independent of scanner access errors.
                await asyncio.to_thread(scanner.recover_known)
                if settings.loyalty_receipt_retention_days:
                    await asyncio.to_thread(store.minimize_old_receipts,policy.shop_domain,settings.loyalty_receipt_retention_days)
                for _ in range(10):
                    if stop_event.is_set() or reader.remaining<=10:break
                    if not await process_one(store,reader,policy,settings):break
                scan=await asyncio.to_thread(scanner.claim)
                if scan:
                    try:
                        if scan.start<naive()-timedelta(days=60) and not settings.loyalty_read_all_orders_verified:
                            raise Review('older_order_access_unverified')
                        start=scan.start.isoformat()+'Z';end=scan.end.isoformat()+'Z'
                        nodes,more,cursor=await reader.updated_page(start,end,scan.cursor)
                        await asyncio.to_thread(scanner.page,scan,nodes,more,cursor)
                        if more:
                            # One bounded page per loop; release lease without losing
                            # persisted cursor so a different process may continue.
                            await asyncio.to_thread(scanner.fail,scan,'page_checkpointed',checkpoint=True)
                    except LostLease:pass
                    except Exception as exc:
                        reason=str(exc) if isinstance(exc,Review) else 'reconciliation_fetch_failed'
                        try:await asyncio.to_thread(scanner.fail,scan,reason)
                        except LostLease:pass
                        log.warning('loyalty reconciliation paused reason=%s',reason)
            except Exception:
                # Fixed log text; provider exceptions may contain tokens or bodies.
                log.error('loyalty cycle failed; durable backlog retained')
            try:await asyncio.wait_for(stop_event.wait(),timeout=60)
            except asyncio.TimeoutError:pass


def start_loyalty_task(app,stop_event,background_tasks,settings):
    app.state.loyalty_task=None
    if not settings.loyalty_processing_enabled:return None
    from ..db import engine
    reasons=processing_gates(settings)
    if not resolve_shopify_access_token(settings):reasons.append('read_token_unset')
    if not schema_ready(engine):reasons.append('loyalty_schema_not_ready')
    if reasons:
        log.warning('loyalty worker blocked gates=%s',','.join(reasons));return None
    task=asyncio.create_task(loyalty_loop(stop_event,settings,Store(engine)),name='shopify-pos-loyalty')
    background_tasks.append(task);app.state.loyalty_task=task
    return task
