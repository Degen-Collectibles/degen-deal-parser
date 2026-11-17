import asyncio
from sqlmodel import Session
from types import SimpleNamespace
from app.loyalty import redemption_worker as worker
from app.loyalty.redemption import Redemptions
from test_loyalty_service import store
from test_loyalty_redemption import funded,reserve,SHOP,CUSTOMER

def test_unconfigured_staff_policy_blocks_worker_start():
    app=SimpleNamespace(state=SimpleNamespace());tasks=[]
    assert worker.start_redemption_task(app,None,tasks,SimpleNamespace(loyalty_redemption_enabled=False)) is None
    assert tasks==[]

def test_provider_failure_retains_reservation_and_rotates_work(store,monkeypatch):
    rewards=funded(store);row=reserve(rewards)
    async def run():
        stop=asyncio.Event()
        class Provider:
            def __init__(self,*a):pass
            async def order_evidence(self,row):
                stop.set();raise RuntimeError('sensitive-provider-body')
        monkeypatch.setattr(worker,'DiscountProvider',Provider)
        monkeypatch.setattr(worker,'redemption_gates',lambda _,**kw:[])
        monkeypatch.setattr(worker,'resolve_shopify_access_token',lambda _:'synthetic')
        await worker.redemption_loop(stop,SimpleNamespace(loyalty_shop_domain=SHOP,
            loyalty_shop_id='1',loyalty_location_ids='2',loyalty_redemption_enabled=True),store)
    asyncio.run(run())
    assert rewards.get(SHOP,row.id).checked_at is not None
    assert rewards.overview(SHOP,CUSTOMER)['reserved_points']==500


def test_issuance_pause_keeps_existing_settlement_but_posting_pause_does_not(store,monkeypatch):
    from test_loyalty_redemption import evidence
    from test_loyalty_domain import POLICY
    rewards=funded(store);row=reserve(rewards)
    async def run(paused):
        stop=asyncio.Event()
        class Provider:
            def __init__(self,*a):pass
            async def order_evidence(self,row):
                stop.set();return evidence(row)
        monkeypatch.setattr(worker,'DiscountProvider',Provider)
        monkeypatch.setattr(worker,'redemption_gates',lambda _,**kw:[])
        monkeypatch.setattr(worker,'resolve_shopify_access_token',lambda _:'synthetic')
        monkeypatch.setattr(worker,'posting_gates',lambda _:['posting_disabled'] if paused else [])
        monkeypatch.setattr(worker,'policy_from_settings',lambda _:POLICY)
        await worker.redemption_loop(stop,SimpleNamespace(loyalty_shop_domain=SHOP,
            loyalty_shop_id='1',loyalty_location_ids='2',loyalty_redemption_enabled=False),store)
    asyncio.run(run(True))
    assert rewards.get(SHOP,row.id).debit_ledger_id is None
    with Session(store.engine) as s:
        from app.loyalty.models import LoyaltyRedemption
        item=s.get(LoyaltyRedemption,row.id);item.checked_at=None;s.add(item);s.commit()
    asyncio.run(run(False))
    assert rewards.get(SHOP,row.id).debit_ledger_id is not None
    assert store.balance(SHOP,CUSTOMER)==500
