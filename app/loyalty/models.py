"""Additive loyalty tables; no customer contact fields or floating point money."""
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import BigInteger, CheckConstraint, Column, Text, UniqueConstraint
from sqlmodel import Field, SQLModel


def now_utc():
    # SQLModel's existing DB convention is UTC-naive storage.
    return datetime.now(timezone.utc).replace(tzinfo=None)


class LoyaltyAccount(SQLModel, table=True):
    __tablename__ = 'loyalty_account'
    __table_args__ = (UniqueConstraint('shop','customer_id',name='uq_loyalty_account_identity'),)
    id: Optional[int] = Field(default=None, primary_key=True)
    shop: str = Field(max_length=255,index=True)
    customer_id: str = Field(max_length=100,index=True)
    identity_hold: bool = False
    created_at: datetime = Field(default_factory=now_utc)


class LoyaltyEntitlement(SQLModel, table=True):
    __tablename__ = 'loyalty_entitlement'
    __table_args__ = (
        UniqueConstraint('shop','order_id',name='uq_loyalty_entitlement_order'),
        CheckConstraint('posted_points >= 0 AND posted_points = CAST(posted_points AS BIGINT)',name='ck_loyalty_posted_whole'),
        CheckConstraint('candidate_points >= 0 AND candidate_points = CAST(candidate_points AS BIGINT)',name='ck_loyalty_candidate_whole'),
        CheckConstraint('net_cents >= 0 AND net_cents = CAST(net_cents AS BIGINT)',name='ck_loyalty_net_cents'),
        CheckConstraint('revision >= 0',name='ck_loyalty_revision'),
    )
    id: Optional[int] = Field(default=None,primary_key=True)
    shop: str = Field(max_length=255,index=True)
    order_id: str = Field(max_length=100,index=True)
    account_id: Optional[int] = Field(default=None,foreign_key='loyalty_account.id',index=True)
    customer_id: Optional[str] = Field(default=None,max_length=100)
    manual_hold: bool = False
    identity_hold: bool = False
    status: str = Field(default='pending',max_length=30,index=True)
    reason: str = Field(default='awaiting_canonical',max_length=100)
    posted_points: int = Field(default=0, sa_column=Column(BigInteger, nullable=False, default=0))
    candidate_points: int = Field(default=0, sa_column=Column(BigInteger, nullable=False, default=0))
    net_cents: int = Field(default=0, sa_column=Column(BigInteger, nullable=False, default=0))
    revision: int = 0
    rule_version: str = Field(default='',max_length=80)
    policy_hash: str = Field(default='',max_length=64)
    basis_hash: str = Field(default='',max_length=64)
    evidence_hash: str = Field(default='',max_length=64)
    evidence_json: str = Field(default='{}',sa_column=Column(Text,nullable=False))
    observed_evidence_json: str = Field(default='{}',sa_column=Column(Text,nullable=False))
    attachment_json: str = Field(default='{}',sa_column=Column(Text,nullable=False))
    source_updated_at: Optional[datetime] = None
    lease_token: Optional[str] = Field(default=None,max_length=64)
    lease_until: Optional[datetime] = Field(default=None,index=True)
    next_check_at: datetime = Field(default_factory=now_utc,index=True)
    created_at: datetime = Field(default_factory=now_utc)
    checked_at: Optional[datetime] = None


class LoyaltyLedger(SQLModel, table=True):
    __tablename__ = 'loyalty_ledger'
    __table_args__ = (
        UniqueConstraint('shop','business_key',name='uq_loyalty_ledger_business'),
        UniqueConstraint('entitlement_id','revision',name='uq_loyalty_ledger_revision'),
        CheckConstraint('delta <> 0 AND delta = CAST(delta AS BIGINT)',name='ck_loyalty_delta_whole'),
        CheckConstraint('resulting_points >= 0 AND resulting_points = CAST(resulting_points AS BIGINT)',name='ck_loyalty_result_whole'),
        CheckConstraint('revision > 0',name='ck_loyalty_ledger_revision'),
    )
    id: Optional[int] = Field(default=None,primary_key=True)
    shop: str = Field(max_length=255,index=True)
    account_id: int = Field(foreign_key='loyalty_account.id',index=True)
    entitlement_id: int = Field(foreign_key='loyalty_entitlement.id',index=True)
    revision: int
    business_key: str = Field(max_length=180)
    delta: int = Field(sa_column=Column(BigInteger, nullable=False))
    resulting_points: int = Field(sa_column=Column(BigInteger, nullable=False))
    reason: str = Field(max_length=100)
    rule_version: str = Field(max_length=80)
    evidence_hash: str = Field(max_length=64)
    evidence_json: str = Field(sa_column=Column(Text,nullable=False))
    actor_user_id: Optional[int] = None
    created_at: datetime = Field(default_factory=now_utc)


class LoyaltyInbox(SQLModel, table=True):
    __tablename__ = 'loyalty_inbox'
    __table_args__ = (
        UniqueConstraint('shop','delivery_id',name='uq_loyalty_inbox_delivery'),
        CheckConstraint('attempts >= 0',name='ck_loyalty_inbox_attempts'),
        CheckConstraint("status IN ('queued','processing','done','review')",name='ck_loyalty_inbox_status'),
    )
    id: Optional[int] = Field(default=None,primary_key=True)
    shop: str = Field(max_length=255,index=True)
    delivery_id: str = Field(max_length=180)
    topic: str = Field(max_length=80)
    order_id: Optional[str] = Field(default=None,max_length=100,index=True)
    payload_hash: str = Field(max_length=64)
    evidence_json: str = Field(sa_column=Column(Text,nullable=False))
    status: str = Field(default='queued',max_length=30,index=True)
    reason: str = Field(default='',max_length=100)
    attempts: int = 0
    lease_token: Optional[str] = Field(default=None,max_length=64)
    lease_until: Optional[datetime] = Field(default=None,index=True)
    available_at: datetime = Field(default_factory=now_utc,index=True)
    received_at: datetime = Field(default_factory=now_utc,index=True)
    completed_at: Optional[datetime] = None


class LoyaltyReconciliation(SQLModel, table=True):
    __tablename__ = 'loyalty_reconciliation'
    __table_args__ = (UniqueConstraint('shop',name='uq_loyalty_reconciliation_shop'),)
    id: Optional[int] = Field(default=None,primary_key=True)
    shop: str = Field(max_length=255)
    policy_hash: str = Field(default='',max_length=64)
    completed_through: Optional[datetime] = None
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    cursor: Optional[str] = Field(default=None,sa_column=Column(Text))
    cursor_history_json: str = Field(default='[]',sa_column=Column(Text,nullable=False))
    lease_token: Optional[str] = Field(default=None,max_length=64)
    lease_until: Optional[datetime] = None
    status: str = Field(default='pending',max_length=30)
    reason: str = Field(default='',max_length=100)
    checked_at: Optional[datetime] = None


TABLES = [m.__table__ for m in (LoyaltyAccount,LoyaltyEntitlement,LoyaltyLedger,LoyaltyInbox,LoyaltyReconciliation)]
