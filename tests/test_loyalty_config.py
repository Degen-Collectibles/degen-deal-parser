import pytest
from app.config import Settings
from app.loyalty.config import receiving_gates,processing_gates,posting_gates,policy_from_settings
from app.loyalty.domain import Review

def test_independent_flags_and_required_decisions_unset():
    s=Settings(_env_file=None)
    assert not s.loyalty_receiving_enabled and not s.loyalty_processing_enabled and not s.loyalty_posting_enabled
    assert not s.loyalty_shop_id and not s.loyalty_launch_at and not s.loyalty_exception_owner
    assert s.loyalty_budget_cents is None and s.loyalty_ledger_retention_days is None
    assert receiving_gates(s) and processing_gates(s) and posting_gates(s)
    s.loyalty_receiving_enabled=True
    assert not s.loyalty_processing_enabled and not s.loyalty_posting_enabled
    assert receiving_gates(s)

def test_transport_and_policy_mismatch_fail_closed():
    s=Settings(_env_file=None)
    s.loyalty_shop_domain='test.myshopify.com';s.shopify_store_domain='different.myshopify.com'
    with pytest.raises(Review,match='transport_shop'):policy_from_settings(s)
    s.shopify_store_domain=s.loyalty_shop_domain
    s.loyalty_shop_id='1';s.loyalty_location_ids='2';s.loyalty_launch_at='2026-09-10T12:00:00-07:00'
    with pytest.raises(Review,match='utc'):policy_from_settings(s)

def test_main_registers_models_before_initialization_and_routes():
    from app import db,main
    from sqlmodel import SQLModel
    assert 'loyalty_ledger' in SQLModel.metadata.tables
    paths=set(main.app.openapi()['paths'])
    assert {'/loyalty','/webhooks/shopify/loyalty','/loyalty/replay','/loyalty/correct'} <= paths
