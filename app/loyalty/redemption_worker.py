"""Bounded reward reconciliation, independent of purchase-point earning."""
import asyncio
import logging
import httpx
from .redemption import Redemptions, reconcile
from .redemption_shopify import DiscountProvider
from .reward_policy import redemption_gates
from .schema import schema_ready
from .service import Store
from .config import posting_gates, policy_from_settings
from ..inventory.shopify import resolve_shopify_access_token

log = logging.getLogger(__name__)


async def redemption_loop(stop_event, settings, store):
    rewards = Redemptions(store, posting_allowed=lambda: not posting_gates(settings), policy_getter=lambda: policy_from_settings(settings))
    async with httpx.AsyncClient(timeout=httpx.Timeout(15, connect=5), follow_redirects=False) as client:
        while not stop_event.is_set():
            if redemption_gates(settings, issuing=False):
                return
            try:
                rows = await asyncio.to_thread(rewards.pending, settings.loyalty_shop_domain,
                    read_all_orders=getattr(settings, 'loyalty_read_all_orders_verified', False))
                for row in rows:
                    if stop_event.is_set(): break
                    try:
                        provider = DiscountProvider(client, settings.loyalty_shop_domain,
                            resolve_shopify_access_token(settings), settings.loyalty_shop_id,
                            settings.loyalty_location_ids)
                        await asyncio.wait_for(reconcile(rewards, provider, row, allow_prepare=bool(settings.loyalty_redemption_enabled)), timeout=45)
                    except asyncio.TimeoutError as exc:
                        await asyncio.to_thread(rewards.failed, row.shop, row.id, exc)
                    except Exception:
                        # Never include provider bodies, discount codes or customer IDs.
                        log.warning('Reward reconciliation deferred; reservation retained')
                    finally:
                        await asyncio.to_thread(rewards.checked, row.shop, row.id)
            except Exception:
                log.error('Reward reconciliation cycle failed; durable reservations retained')
            try: await asyncio.wait_for(stop_event.wait(), timeout=60)
            except asyncio.TimeoutError: pass


def start_redemption_task(app, stop_event, background_tasks, settings):
    app.state.loyalty_redemption_task = None
    if redemption_gates(settings, issuing=False): return None
    from ..db import engine
    if not resolve_shopify_access_token(settings) or not schema_ready(engine):
        log.warning('Reward worker blocked by token or schema readiness')
        return None
    task = asyncio.create_task(redemption_loop(stop_event, settings, Store(engine)), name='loyalty-redemptions')
    app.state.loyalty_redemption_task = task
    background_tasks.append(task)
    return task
