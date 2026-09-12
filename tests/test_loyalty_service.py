from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import json
import os
from uuid import uuid4
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select, create_engine
from app.loyalty.models import LoyaltyAccount, LoyaltyEntitlement, LoyaltyLedger, LoyaltyInbox, LoyaltyReconciliation
from app.loyalty.schema import install_schema, schema_ready
from app.loyalty.service import Store, LostLease
from test_loyalty_domain import order, attachment, POLICY, SALE, refund

@pytest.fixture(params=['sqlite'] + (['postgresql'] if os.environ.get('LOYALTY_TEST_PG_SOCKET') else []))
def store(tmp_path,request):
    if request.param=='postgresql':
        socket=os.environ['LOYALTY_TEST_PG_SOCKET']
        url=f'postgresql+psycopg://loyalty_test@/loyalty_test?host={socket}&port=55439'
        schema='test_'+uuid4().hex
        admin=create_engine(url)
        with admin.begin() as c:c.execute(text(f'CREATE SCHEMA {schema}'))
        admin.dispose()
        engine=create_engine(url,connect_args={'options':f'-c search_path={schema}'})
        install_schema(engine)
        return Store(engine)
    engine=create_engine('sqlite:///'+str(tmp_path/'loyalty.db'),connect_args={'check_same_thread':False,'timeout':10})
    install_schema(engine)
    return Store(engine)

def receipt(store,key='d1'):
    return store.receive(POLICY.shop_domain,key,'orders/create',{'order_id':order()['id'],'customer_id':order()['customer_id'],'attachment':attachment(),'updated_at':SALE.isoformat()}, 'a'*64, SALE)

def claim(store,now=SALE):
    return store.claim(POLICY.shop_domain,now=now)

def finish(store,lease,o=None,posting=True,now=SALE):
    return store.finish(lease,o or order(),POLICY,posting=posting,now=now)

def test_additive_schema_and_constraints(store):
    assert schema_ready(store.engine)
    install_schema(store.engine)
    with store.engine.begin() as c:
        with pytest.raises(IntegrityError):
            c.execute(text("INSERT INTO loyalty_account (shop,customer_id,identity_hold,created_at) VALUES ('s','c',FALSE,CURRENT_TIMESTAMP),('s','c',FALSE,CURRENT_TIMESTAMP)"))
    empty=create_engine('sqlite:///:memory:')
    assert not schema_ready(empty)

def test_receipt_dedupe_minimized_and_atomic_posting(store):
    assert receipt(store)==receipt(store)
    lease=claim(store)
    assert finish(store,lease)==20
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==20
    assert claim(store) is None
    receipt(store,'d2'); assert finish(store,claim(store))==0
    with Session(store.engine) as s:
        assert len(s.exec(select(LoyaltyLedger)).all())==1
        assert s.exec(select(LoyaltyEntitlement)).one().posted_points==20
        assert all(x.status=='done' for x in s.exec(select(LoyaltyInbox)).all())

def test_refund_deltas_and_shadow_recovery(store):
    receipt(store); finish(store,claim(store),posting=False)
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==0
    receipt(store,'post'); finish(store,claim(store))
    o=order(); o['updated_at']=(SALE+timedelta(minutes=1)).isoformat(); o['refunds']=[refund(50)]
    receipt(store,'refund'); assert finish(store,claim(store),o)==-1
    receipt(store,'repeat'); assert finish(store,claim(store),o)==0
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==19

def test_stale_canonical_fetch_and_same_timestamp_conflict(store):
    receipt(store); newer=order(updated_at=(SALE+timedelta(hours=1)).isoformat()); finish(store,claim(store),newer)
    receipt(store,'older')
    assert finish(store,claim(store),order())==0
    with Session(store.engine) as s:
        assert s.exec(select(LoyaltyEntitlement)).one().reason=='stale_canonical_read'
    receipt(store,'conflict'); changed=order(3000,updated_at=newer['updated_at'])
    assert finish(store,claim(store),changed)==0
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==20

def test_expired_worker_cannot_post_and_retry_reclaims(store):
    receipt(store); old=claim(store)
    new=claim(store,SALE+timedelta(minutes=6))
    assert new and new.token!=old.token
    with pytest.raises(LostLease): finish(store,old)
    assert finish(store,new,now=SALE+timedelta(minutes=6))==20

def test_concurrent_first_receipts_and_awards(store):
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda n:receipt(store,str(n)),range(12)))
        leases=list(pool.map(lambda _:claim(store),range(6)))
    assert sum(l is not None for l in leases)==1
    finish(store,next(l for l in leases if l))
    while (lease:=claim(store)) is not None: finish(store,lease)
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==20
    with Session(store.engine) as s:
        assert len(s.exec(select(LoyaltyAccount)).all())==1
        assert len(s.exec(select(LoyaltyEntitlement)).all())==1
        assert len(s.exec(select(LoyaltyLedger)).all())==1

def test_changed_original_basis_and_identity_hold_review(store):
    receipt(store); finish(store,claim(store))
    receipt(store,'changed'); o=order(2999,updated_at=(SALE+timedelta(hours=1)).isoformat())
    assert finish(store,claim(store),o)==0
    with Session(store.engine) as s: assert s.exec(select(LoyaltyEntitlement)).one().reason=='original_basis_changed'
    receipt(store,'identity'); o=order(customer_id='gid://shopify/Customer/4',updated_at=(SALE+timedelta(hours=2)).isoformat())
    assert finish(store,claim(store),o)==0
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==20

def test_append_only_ledger_and_rebuild(store):
    receipt(store); finish(store,claim(store))
    with store.engine.begin() as c:
        with pytest.raises(Exception): c.execute(text('UPDATE loyalty_ledger SET delta=21'))
    assert store.rebuild_check(POLICY.shop_domain)==[]

def test_delivery_key_collision_is_not_accepted(store):
    receipt(store)
    with pytest.raises(ValueError,match='delivery_conflict'):
        store.receive(POLICY.shop_domain,'d1','orders/paid',{'order_id':order()['id']},'b'*64,SALE)

def test_creation_attachment_survives_pending_payment(store):
    receipt(store);pending=order(received_cents=0);pending['payments'][0]['status']='PENDING'
    finish(store,claim(store),pending)
    store.receive(POLICY.shop_domain,'later-paid','orders/updated',{'order_id':order()['id']},'b'*64,SALE)
    paid=order(updated_at=(SALE+timedelta(hours=1)).isoformat())
    assert finish(store,claim(store),paid)==20

def test_prior_refund_cannot_disappear_from_newer_canonical(store):
    receipt(store);refunded=order();refunded['refunds']=[refund(50)]
    finish(store,claim(store),refunded)
    receipt(store,'history-missing');newer=order(updated_at=(SALE+timedelta(hours=1)).isoformat())
    assert finish(store,claim(store),newer)==0
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==19
    with Session(store.engine) as s:assert s.exec(select(LoyaltyEntitlement)).one().reason=='refund_history_changed'

def test_refund_webhook_requires_refund_visible_in_canonical(store):
    receipt(store);finish(store,claim(store))
    store.receive(POLICY.shop_domain,'new-refund','refunds/create',{'order_id':order()['id'],'refund_id':'gid://shopify/Refund/99'},'c'*64,SALE)
    assert finish(store,claim(store))==0
    with Session(store.engine) as s:assert s.exec(select(LoyaltyEntitlement)).one().reason=='refund_not_visible'

def test_observed_identity_reassignment_is_review_before_first_award(store):
    store.receive(POLICY.shop_domain,'no-proof','orders/updated',{'order_id':order()['id']},'d'*64,SALE)
    finish(store,claim(store))
    changed=order(customer_id='gid://shopify/Customer/99',updated_at=(SALE+timedelta(hours=1)).isoformat())
    store.receive(POLICY.shop_domain,'new-proof','orders/create',{'order_id':order()['id'],'attachment':attachment(changed)},'e'*64,SALE)
    assert finish(store,claim(store),changed)==0
    with Session(store.engine) as s:assert s.exec(select(LoyaltyEntitlement)).one().reason=='identity_reassignment'

def test_program_policy_frozen_across_new_orders(store):
    receipt(store);finish(store,claim(store))
    from dataclasses import replace
    altered=replace(POLICY,launch_at=POLICY.launch_at-timedelta(days=1))
    o=order(id='gid://shopify/Order/100')
    store.receive(POLICY.shop_domain,'other-policy','orders/create',{'order_id':o['id'],'attachment':attachment()},'f'*64,SALE)
    assert store.finish(claim(store),o,altered,posting=True,now=SALE)==0
    assert store.balance(POLICY.shop_domain,o['customer_id'])==20

def test_failed_post_transaction_rolls_back_entitlement_ledger_and_completion(store,monkeypatch):
    receipt(store);lease=claim(store)
    original=Session.commit
    def crash(session):
        session.flush()
        raise RuntimeError('synthetic crash before commit')
    monkeypatch.setattr(Session,'commit',crash)
    with pytest.raises(RuntimeError):finish(store,lease)
    monkeypatch.setattr(Session,'commit',original)
    assert store.balance(POLICY.shop_domain,order()['customer_id'])==0
    with Session(store.engine) as s:
        assert not s.exec(select(LoyaltyLedger)).all()
        assert s.exec(select(LoyaltyInbox)).one().status=='processing'
    assert finish(store,lease)==20

def test_fractional_points_rejected_by_database(store):
    if store.engine.dialect.name!='sqlite':
        # PostgreSQL has a native BIGINT column; SQLite requires the extra CHECK.
        from sqlalchemy import inspect,BigInteger
        fields={c['name']:c for c in inspect(store.engine).get_columns('loyalty_ledger')}
        assert isinstance(fields['delta']['type'],BigInteger)
        return
    receipt(store);finish(store,claim(store))
    with store.engine.begin() as c:
        with pytest.raises(IntegrityError):c.execute(text('UPDATE loyalty_entitlement SET posted_points=20.5'))

def test_partial_existing_schema_fails_readiness_without_rebuilding_tables(tmp_path):
    engine=create_engine('sqlite:///'+str(tmp_path/'old.db'))
    with engine.begin() as c:c.execute(text('CREATE TABLE loyalty_account (id INTEGER PRIMARY KEY, shop TEXT, customer_id TEXT)'))
    assert not schema_ready(engine)
    with pytest.raises(RuntimeError,match='schema_not_ready'):install_schema(engine)

def test_manual_hold_survives_fetch_failure_and_later_replay(store):
    receipt(store);finish(store,claim(store))
    with Session(store.engine) as s:
        row=s.exec(select(LoyaltyEntitlement)).one();row.manual_hold=True;row.reason='manual_correction_hold';s.add(row);s.commit()
    receipt(store,'failed');lease=claim(store);store.fail(lease,now=SALE)
    lease=claim(store,SALE+timedelta(minutes=2));assert finish(store,lease,now=SALE+timedelta(minutes=2))==0
    with Session(store.engine) as s:assert s.exec(select(LoyaltyEntitlement)).one().reason=='manual_correction_hold'

def test_receipt_identity_conflict_is_durable_review(store):
    receipt(store);changed=order(customer_id='gid://shopify/Customer/99')
    assert finish(store,claim(store),changed)==0
    store.receive(POLICY.shop_domain,'later-matching-proof','orders/create',{'order_id':changed['id'],'attachment':attachment(changed)},'e'*64,SALE)
    assert finish(store,claim(store),changed)==0
    with Session(store.engine) as s:assert s.exec(select(LoyaltyEntitlement)).one().identity_hold

def test_retention_scrubs_only_completed_receipts_and_keeps_dedupe(store):
    receipt(store);finish(store,claim(store));receipt(store,'pending')
    assert store.minimize_old_receipts(POLICY.shop_domain,30,now=SALE+timedelta(days=31))==1
    with Session(store.engine) as s:
        rows=s.exec(select(LoyaltyInbox).order_by(LoyaltyInbox.id)).all()
        assert rows[0].evidence_json=='{}' and rows[0].payload_hash=='a'*64
        assert rows[1].evidence_json!='{}'
    assert receipt(store)==1

def test_missing_ledger_guard_blocks_all_write_paths(store):
    with store.engine.begin() as c:
        if store.engine.dialect.name=='sqlite':c.execute(text('DROP TRIGGER loyalty_ledger_no_update'))
        else:c.execute(text('ALTER TABLE loyalty_ledger DISABLE TRIGGER loyalty_ledger_immutable'))
    assert not schema_ready(store.engine)
    with pytest.raises(RuntimeError,match='schema_not_ready'):receipt(store)

def test_noninteger_existing_column_does_not_pass_readiness(tmp_path):
    engine=create_engine('sqlite:///'+str(tmp_path/'bad-type.db'))
    install_schema(engine)
    # Recreate only the synthetic account table with incorrect identity type.
    with engine.begin() as c:
        c.execute(text('ALTER TABLE loyalty_account RENAME TO saved_loyalty_account'))
        c.execute(text('CREATE TABLE loyalty_account (id TEXT PRIMARY KEY,shop VARCHAR(255) NOT NULL,customer_id VARCHAR(100) NOT NULL,identity_hold BOOLEAN NOT NULL,created_at DATETIME NOT NULL,CONSTRAINT uq_loyalty_account_identity UNIQUE(shop,customer_id))'))
    assert not schema_ready(engine)


def test_concurrent_same_delivery_first_insert(store):
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids=list(pool.map(lambda _:receipt(store),range(16)))
    assert len(set(ids))==1
    with Session(store.engine) as s:
        assert len(s.exec(select(LoyaltyInbox)).all())==1
        assert len(s.exec(select(LoyaltyEntitlement)).all())==1


@pytest.mark.parametrize('status',['failed','in_progress','pending','unknown','completed','succeeded'])
def test_only_successful_merge_freezes_identity(store,status):
    receipt(store);finish(store,claim(store))
    store.receive(POLICY.shop_domain,'merge','customers/merge',{'merge_ids':[order()['customer_id'],'gid://shopify/Customer/4'],'merge_status':status},'c'*64,SALE)
    o=order(returns=True,updated_at=(SALE+timedelta(hours=1)).isoformat());o['refunds']=[refund(50)]
    receipt(store,'refund-after-merge')
    assert finish(store,claim(store),o)==(0 if status in ('completed','succeeded') else -1)


def test_followup_attachment_observed_within_deadline_and_retained(store):
    o=order(customer_id=None)
    store.receive(POLICY.shop_domain,'unattached','orders/create',{'order_id':o['id']},'1'*64,SALE)
    assert finish(store,claim(store),o)==0
    later=SALE+timedelta(days=6)
    store.receive(POLICY.shop_domain,'attached','orders/updated',{'order_id':o['id']},'2'*64,later)
    assert finish(store,claim(store,later),order(updated_at=later.isoformat()),now=later)==20
    much_later=SALE+timedelta(days=30)
    store.receive(POLICY.shop_domain,'refund','orders/updated',{'order_id':o['id']},'3'*64,much_later)
    o=order(updated_at=much_later.isoformat());o['refunds']=[refund(50)]
    assert finish(store,claim(store,much_later),o,now=much_later)==-1


def test_delayed_create_header_cannot_backdate_attachment(store):
    late=SALE+timedelta(days=8)
    store.receive(POLICY.shop_domain,'late','orders/create',{'order_id':order()['id'],'attachment':attachment()},'4'*64,late)
    assert finish(store,claim(store,late),now=late)==0
    with Session(store.engine) as s:
        row=s.exec(select(LoyaltyEntitlement)).one()
        assert row.reason=='attachment_outside_window' and not row.account_id
        assert json.loads(row.attachment_json)['at'].startswith('2026-09-19')


def test_readiness_detects_missing_index(store):
    with store.engine.begin() as c:c.execute(text('DROP INDEX ix_loyalty_inbox_available_at'))
    assert not schema_ready(store.engine)


def test_readiness_detects_same_name_wrong_check(store):
    if store.engine.dialect.name=='postgresql':
        with store.engine.begin() as c:
            c.execute(text('ALTER TABLE loyalty_entitlement DROP CONSTRAINT ck_loyalty_net_cents'))
            c.execute(text('ALTER TABLE loyalty_entitlement ADD CONSTRAINT ck_loyalty_net_cents CHECK (net_cents >= -100)'))
    else:
        with store.engine.begin() as c:
            c.execute(text('PRAGMA writable_schema=ON'))
            c.execute(text("UPDATE sqlite_master SET sql=replace(sql,'net_cents >= 0','net_cents >= -100') WHERE name='loyalty_entitlement'"))
            c.execute(text('PRAGMA writable_schema=OFF'))
        store.engine.dispose()
    assert not schema_ready(store.engine)


def test_postgresql_bigint_capacity_and_type_corruption(store):
    if store.engine.dialect.name!='postgresql':pytest.skip('PostgreSQL numeric type regression')
    o=order(1_000_000_000_000)
    receipt(store);assert finish(store,claim(store),o)==10_000_000_000
    with store.engine.begin() as c:
        c.execute(text('UPDATE loyalty_entitlement SET net_cents=0'))
        c.execute(text('ALTER TABLE loyalty_entitlement ALTER COLUMN net_cents TYPE INTEGER'))
    assert not schema_ready(store.engine)


def test_correction_replay_and_worker_share_order_fence(store):
    from app.loyalty import ops
    from app.loyalty.domain import Review
    from app.models import User,AuditLog
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(store.engine,tables=[User.__table__,AuditLog.__table__])
    with Session(store.engine) as s:
        actor=User(username='synthetic',password_hash='unused',role='admin');s.add(actor);s.commit();s.refresh(actor);s.expunge(actor)
    authorize=lambda s,p:actor
    receipt(store);finish(store,claim(store))
    ops.replay(store,POLICY.shop_domain,[1],'concurrent-replay',authorize)
    from app.loyalty.service import naive
    current=SALE+timedelta(days=1)
    # Replay uses local current time; claim just after that durable receipt.
    with Session(store.engine) as s:
        queued=s.exec(select(LoyaltyInbox).where(LoyaltyInbox.topic=='admin/replay')).one()
        current=queued.available_at.replace(tzinfo=SALE.tzinfo)
    lease=claim(store,current)
    def worker():return finish(store,lease,now=current)
    def correction():
        try:return ops.correct(store,POLICY.shop_domain,1,1,19,'concurrent-correction','refund_review','case-1',authorize)
        except Review as exc:return str(exc)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda f:f(),[worker,correction]))
    assert results[0]==0 and results[1] in (2,'order_busy')
    assert store.balance(POLICY.shop_domain,order()['customer_id']) in (19,20)
    assert store.rebuild_check(POLICY.shop_domain)==[]
    # A correction with an old revision cannot race a later replay into restoring points.
    if results[1]==2:
        receipt(store,'after-correction');assert finish(store,claim(store))==0
        assert store.balance(POLICY.shop_domain,order()['customer_id'])==19
