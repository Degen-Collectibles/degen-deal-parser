"""Consistent, minimized ledger reads shared by POS API and synthetic preview."""
from datetime import datetime, timezone
from sqlalchemy import case, func, select, true
from .models import LoyaltyAccount as Account, LoyaltyEntitlement as Entitlement, LoyaltyLedger as Ledger


class SnapshotChanged(ValueError):
    pass


def ledger_snapshot(engine, shop, customer, *, limit=25, cursor=None):
    """One SQL statement keeps totals, page and current review flags consistent.

    Pagination excludes postings above its signed watermark. Count/sum checks
    detect a previously uncommitted lower-ID row becoming visible on a later
    page, instead of silently changing the prior snapshot. No locks or writes.
    """
    gid = f'gid://shopify/Customer/{customer}'
    conditions = [Ledger.shop == shop, Account.shop == shop, Account.customer_id == gid]
    if cursor:
        conditions.append(Ledger.id <= cursor['upper'])
    entries = select(Ledger.id, Ledger.delta, Ledger.reason, Ledger.created_at).join(
        Account, Ledger.account_id == Account.id).where(*conditions).cte('customer_ledger')
    totals = select(func.coalesce(func.sum(entries.c.delta), 0).label('balance'),
                    func.count(entries.c.id).label('count'),
                    func.coalesce(func.max(entries.c.id), 0).label('upper')).cte('totals')
    page = select(entries)
    if cursor:
        page = page.where(entries.c.id < cursor['before'])
    page = page.order_by(entries.c.id.desc()).limit(limit+1).cte('history_page')
    account_hold = select(Account.id).where(Account.shop == shop, Account.customer_id == gid, Account.identity_hold.is_(True)).exists()
    flags = select(
        func.max(Entitlement.checked_at).label('checked_at'),
        func.coalesce(func.max(case((Entitlement.status == 'review', 1), (Entitlement.identity_hold.is_(True), 1), else_=0)), 0).label('review'),
        func.coalesce(func.max(case((Entitlement.status.in_(['pending','shadow']), 1), else_=0)), 0).label('pending'),
    ).where(Entitlement.shop == shop, Entitlement.customer_id == gid).cte('flags')
    statement = select(totals, page, flags, account_hold.label('account_hold')).select_from(
        totals.outerjoin(page, true()).join(flags, true())).order_by(page.c.id.desc())
    with engine.connect() as connection:
        rows = connection.execute(statement).mappings().all()
    summary = rows[0]
    balance = str(int(summary['balance']))
    if balance.startswith('-'):
        raise SnapshotChanged('ledger_review_required')
    if cursor and (balance != cursor['balance'] or summary['count'] != cursor['count']):
        raise SnapshotChanged('snapshot_changed')
    history = [row for row in rows if row['id'] is not None]
    has_more = len(history) > limit
    history = history[:limit]
    as_of = cursor['as_of'] if cursor else datetime.now(timezone.utc).isoformat()
    def stamp(value):
        return value.replace(tzinfo=timezone.utc).isoformat() if value else None
    labels = {'earned':'Purchase points', 'refund':'Refund adjustment'}
    result = dict(customer_id=customer, balance_points=balance, has_history=summary['count'] > 0,
                  as_of=as_of, checked_at=stamp(summary['checked_at']),
                  needs_review=bool(summary['review'] or summary['account_hold']), updates_pending=bool(summary['pending']),
                  history=[dict(delta_points=str(int(row['delta'])), label=labels.get(row['reason'],'Administrator adjustment'),
                                created_at=stamp(row['created_at'])) for row in history])
    next_page = dict(customer=customer, upper=cursor['upper'] if cursor else summary['upper'],
                     before=history[-1]['id'], balance=balance, count=summary['count'], as_of=as_of) if has_more else None
    return result, next_page
