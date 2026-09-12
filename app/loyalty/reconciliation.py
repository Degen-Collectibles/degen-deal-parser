"""Overlapping updated-order windows and a bounded sweep of known entitlements."""
import json
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4
from sqlalchemy import or_, update
from sqlmodel import select
from .domain import Review, fingerprint, gid, timestamp
from .models import LoyaltyEntitlement, LoyaltyInbox, LoyaltyReconciliation
from .service import LEASE_SECONDS, LostLease, dump, insert_missing, naive


@dataclass(frozen=True)
class ScanLease:
    id: int
    token: str
    start: object
    end: object
    cursor: str | None


class Reconciler:
    def __init__(self,store,policy):
        self.store=store;self.policy=policy

    def claim(self,*,now=None):
        current=naive(now);launch=naive(self.policy.launch_at)
        def operation(s):
            insert_missing(s,LoyaltyReconciliation,dict(shop=self.policy.shop_domain,policy_hash=self.policy.digest()),['shop'])
            row=s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.shop==self.policy.shop_domain).with_for_update()).one()
            if not row.policy_hash:
                row.policy_hash=self.policy.digest();s.add(row)
            if row.policy_hash!=self.policy.digest():
                row.status='review';row.reason='program_policy_changed';s.add(row);return None
            if row.lease_until and row.lease_until>current: return None
            if not row.window_end:
                start=max(launch,(row.completed_through-timedelta(minutes=10)) if row.completed_through else launch)
                # Exclude the most recent 30 seconds to reduce changing-page races.
                end=min(current-timedelta(seconds=30),start+timedelta(days=1))
                if end<=start:return None
                row.window_start=start;row.window_end=end;row.cursor=None;row.cursor_history_json='[]'
            row.lease_token=uuid4().hex;row.lease_until=current+timedelta(seconds=LEASE_SECONDS)
            row.status='scanning';row.reason='';row.checked_at=current;s.add(row);s.flush()
            return ScanLease(row.id,row.lease_token,row.window_start,row.window_end,row.cursor)
        return self.store.transaction(operation)

    def locked(self,s,lease,current):
        row=s.exec(select(LoyaltyReconciliation).where(LoyaltyReconciliation.id==lease.id).with_for_update()).one()
        if row.lease_token!=lease.token or not row.lease_until or row.lease_until<=current:
            raise LostLease('loyalty_scan_lease_lost')
        return row

    def page(self,lease,nodes,more,cursor,*,now=None):
        current=naive(now)
        def operation(s):
            row=self.locked(s,lease,current)
            history=json.loads(row.cursor_history_json)
            if more and (not nodes or not cursor or cursor in history or len(history)>=10000):
                raise Review('reconciliation_cursor_invalid')
            if len(nodes)>50:raise Review('reconciliation_page_too_large')
            seen=set()
            for node in nodes:
                oid=gid('Order',node['id']);updated=naive(node['updatedAt'])
                if oid in seen or not row.window_start<=updated<row.window_end:
                    raise Review('reconciliation_page_inconsistent')
                seen.add(oid)
                evidence={'order_id':oid,'updated_at':timestamp(node['updatedAt']).isoformat()}
                key='scan:'+fingerprint([row.window_start.isoformat(),row.window_end.isoformat(),evidence])
                self.enqueue(s,oid,key,evidence,current)
            # Work and cursor commit in ONE transaction, never page acknowledgments
            # ahead of durable per-order jobs. Crashes replay the same business keys.
            row.cursor=cursor if more else None
            row.cursor_history_json=dump(history+[cursor]) if more else '[]'
            row.checked_at=current;row.lease_until=current+timedelta(seconds=LEASE_SECONDS)
            if not more:
                row.completed_through=row.window_end;row.window_start=None;row.window_end=None
                row.lease_token=None;row.lease_until=None;row.status='idle'
            s.add(row)
        self.store.transaction(operation)

    def enqueue(self,s,oid,key,evidence,current):
        insert_missing(s,LoyaltyEntitlement,dict(shop=self.policy.shop_domain,order_id=oid,next_check_at=current,created_at=current),['shop','order_id'])
        insert_missing(s,LoyaltyInbox,dict(shop=self.policy.shop_domain,delivery_id=key,topic='reconciliation',order_id=oid,
            payload_hash=fingerprint(evidence),evidence_json=dump(evidence),received_at=current,available_at=current),['shop','delivery_id'])

    def fail(self,lease,reason,*,now=None,checkpoint=False):
        current=naive(now)
        def operation(s):
            row=self.locked(s,lease,current)
            row.status='pending' if checkpoint else 'review';row.reason=reason;row.checked_at=current
            row.lease_token=None;row.lease_until=None;s.add(row)
        self.store.transaction(operation)

    def recover_known(self,*,now=None,limit=25):
        current=naive(now)
        if not 1<=limit<=100:raise ValueError('invalid_recovery_limit')
        def operation(s):
            rows=s.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.shop==self.policy.shop_domain,
                LoyaltyEntitlement.next_check_at<=current,
                or_(LoyaltyEntitlement.lease_until.is_(None),LoyaltyEntitlement.lease_until<=current))
                .order_by(LoyaltyEntitlement.next_check_at,LoyaltyEntitlement.id).limit(limit).with_for_update()).all()
            count=0
            for row in rows:
                # Coalesce an outstanding job instead of multiplying a backlog.
                pending=s.exec(select(LoyaltyInbox.id).where(LoyaltyInbox.shop==row.shop,LoyaltyInbox.order_id==row.order_id,
                    LoyaltyInbox.status.in_(['queued','processing']))).first()
                if pending is None:
                    evidence={'order_id':row.order_id}
                    self.enqueue(s,row.order_id,'recovery:'+fingerprint([row.id,row.next_check_at.isoformat()]),evidence,current)
                    count+=1
                row.next_check_at=current+timedelta(hours=6);s.add(row)
            return count
        return self.store.transaction(operation)
