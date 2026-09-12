import asyncio
from datetime import timedelta
from types import SimpleNamespace
import pytest
from sqlmodel import Session,select
from app.loyalty.models import LoyaltyInbox,LoyaltyReconciliation,LoyaltyEntitlement
from app.loyalty.reconciliation import Reconciler
from app.loyalty.worker import process_one,start_loyalty_task
from test_loyalty_service import store,receipt,claim,finish
from test_loyalty_domain import POLICY,SALE,order


def test_scan_page_commit_before_cursor_and_recovery(store):
    scanner=Reconciler(store,POLICY)
    lease=scanner.claim(now=SALE)
    assert lease
    scanner.page(lease,[{'id':order()['id'],'updatedAt':(SALE-timedelta(minutes=1)).isoformat()}],True,'cursor-1',now=SALE)
    with Session(store.engine) as s:
        assert s.exec(select(LoyaltyInbox)).one().status=='queued'
        assert s.exec(select(LoyaltyReconciliation)).one().cursor=='cursor-1'
    scanner.page(lease,[],False,None,now=SALE)
    with Session(store.engine) as s:
        assert s.exec(select(LoyaltyReconciliation)).one().completed_through is not None
    assert scanner.recover_known(now=SALE+timedelta(days=1))==0  # existing queued work coalesces

def test_scan_lease_fences_crashes_and_overlaps(store):
    scanner=Reconciler(store,POLICY)
    first=scanner.claim(now=SALE)
    assert scanner.claim(now=SALE) is None
    replacement=scanner.claim(now=SALE+timedelta(minutes=6))
    assert replacement.start==first.start and replacement.end==first.end
    from app.loyalty.service import LostLease
    with pytest.raises(LostLease):scanner.page(first,[],False,None,now=SALE+timedelta(minutes=6))
    scanner.page(replacement,[],False,None,now=SALE+timedelta(minutes=6))
    later=scanner.claim(now=SALE+timedelta(hours=1))
    assert later.start < replacement.end

def test_reader_failure_retries_and_worker_disabled_no_calls(store):
    receipt(store)
    class BadReader:
        async def order(self,*a):raise RuntimeError('secret text')
    settings=SimpleNamespace(loyalty_processing_enabled=False)
    assert asyncio.run(process_one(store,BadReader(),POLICY,settings,now=SALE)) is False
    settings.loyalty_processing_enabled=True
    settings.loyalty_posting_enabled=False
    asyncio.run(process_one(store,BadReader(),POLICY,settings,now=SALE))
    with Session(store.engine) as s:
        row=s.exec(select(LoyaltyInbox)).one()
        assert row.status=='queued' and row.reason=='canonical_fetch_failed' and row.lease_token is None

def test_startup_off_has_no_loyalty_task():
    app=SimpleNamespace(state=SimpleNamespace())
    tasks=[]
    assert start_loyalty_task(app,asyncio.Event(),tasks,SimpleNamespace(loyalty_processing_enabled=False)) is None
    assert not tasks and app.state.loyalty_task is None

def test_recovery_includes_awarded_and_shadow_orders_without_historical_import(store):
    receipt(store);finish(store,claim(store))
    scanner=Reconciler(store,POLICY)
    assert scanner.recover_known(now=SALE+timedelta(days=90))==1
    with Session(store.engine) as s:
        receipts=s.exec(select(LoyaltyInbox)).all()
        assert len(receipts)==2
        assert all(r.order_id==order()['id'] for r in receipts)

def test_reconciliation_rejects_multi_page_cursor_cycle(store):
    scanner=Reconciler(store,POLICY);lease=scanner.claim(now=SALE)
    node={'id':order()['id'],'updatedAt':(SALE-timedelta(minutes=1)).isoformat()}
    scanner.page(lease,[node],True,'a',now=SALE)
    scanner.page(lease,[dict(node,id='gid://shopify/Order/12')],True,'b',now=SALE)
    from app.loyalty.domain import Review
    with pytest.raises(Review):scanner.page(lease,[dict(node,id='gid://shopify/Order/13')],True,'a',now=SALE)
