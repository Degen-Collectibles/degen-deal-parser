import json
from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine, select
from starlette.requests import Request

from app.bank_overlap import migrate_legacy_discord_overlaps, overlap_warning_note
from app.bank_allocations import set_employee_allocation
from app.discord.bank_reconciliation import summarize_bank_transactions
from app.models import AuditLog, BankStatementImport, BankTransaction, LedgerRule

EVIDENCE = "[unresolved-discord-overlap] Candidate transaction 99; exact payment unknown."


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(BankStatementImport(id=1, label="test", account_label="checking"))
        s.add(BankTransaction(id=1, import_id=1, row_index=1, account_label="checking",
            amount=-100, posted_at=datetime(2026, 9, 1), description="PSA",
            expense_category="grading_fees", match_override_status="force_unmatched",
            match_override_note=EVIDENCE))
        s.commit()
        yield s
    engine.dispose()


def test_legacy_migration_is_audited_idempotent_and_preserves_original_fields(session):
    row = session.get(BankTransaction, 1)
    before = row.model_dump()
    assert migrate_legacy_discord_overlaps(session) == 1
    session.commit(); session.expire_all()
    row = session.get(BankTransaction, 1)
    assert row.discord_overlap_unresolved and row.discord_overlap_note == EVIDENCE
    assert all(getattr(row, k) == v for k, v in before.items()
               if k not in {"discord_overlap_unresolved", "discord_overlap_note"})
    row.match_override_note = "Edited reconciliation note"
    session.commit()
    assert migrate_legacy_discord_overlaps(session) == 0
    assert overlap_warning_note(row) == EVIDENCE
    audits = session.exec(select(AuditLog)).all()
    assert len(audits) == 1 and audits[0].action == "financial.bank_overlap.migrate"
    assert json.loads(audits[0].details_json)["after"]["discord_overlap_unresolved"] is True


@pytest.mark.parametrize("mode", ["force", "clear"])
@pytest.mark.parametrize("migrated", [True, False])
def test_real_override_route_cannot_clear_persistent_warning(session, mode, migrated):
    from app.routers.ledger import ledger_row_force_unmatch_form
    if migrated:
        migrate_legacy_discord_overlaps(session); session.commit()
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "session": {}})
    with patch("app.routers.ledger.require_role_response", return_value=None), patch(
        "app.routers.ledger.current_user_label", return_value="reviewer"):
        response = ledger_row_force_unmatch_form(request, 1, mode=mode, note="New note", session=session)
    assert response.status_code == 303
    session.expire_all(); row = session.get(BankTransaction, 1)
    assert row.match_override_note == (None if mode == "clear" else "New note")
    assert summarize_bank_transactions([row])["reconciliation_warnings"] == [{"id": 1, "note": EVIDENCE}]


@pytest.mark.parametrize("mode", ["force_unmatched", "clear"])
@pytest.mark.parametrize("migrated", [True, False])
def test_bulk_rule_cannot_clear_persistent_warning(session, mode, migrated):
    from app.ledger import LedgerFilters, apply_ledger_rule
    if migrated:
        migrate_legacy_discord_overlaps(session); session.commit()
    rule = LedgerRule(name="Note edit", conditions_json=json.dumps({"description_contains": "psa"}),
        actions_json=json.dumps({"match_override_status": mode, "note": "Replacement note"}))
    session.add(rule); session.commit()
    result = apply_ledger_rule(session, rule, filters=LedgerFilters(status="all"), applied_by="reviewer")
    assert result["updated_count"] == 1
    session.commit(); session.expire_all(); row = session.get(BankTransaction, 1)
    assert row.match_override_note == (None if mode == "clear" else "Replacement note")
    assert overlap_warning_note(row) == EVIDENCE


@pytest.mark.parametrize("migrated", [True, False])
def test_allocation_edit_and_clear_preserve_overlap_context(session, migrated):
    if migrated:
        migrate_legacy_discord_overlaps(session); session.commit()
    for amount in ["25", "0"]:
        row = session.get(BankTransaction, 1)
        set_employee_allocation(session, row, employee_amount=amount, business_category="grading_fees",
            estimated=True, note="Changed allocation", actor_label="reviewer")
        session.commit(); session.expire_all(); row = session.get(BankTransaction, 1)
        assert EVIDENCE not in row.match_override_note
        assert overlap_warning_note(row) == EVIDENCE
        assert summarize_bank_transactions([row])["debits"] == -100


def test_new_reserved_marker_rejected_before_any_mutation(session):
    from app.bank_overlap import set_reconciliation_note
    from app.ledger import LedgerFilters, apply_ledger_rule
    row = session.get(BankTransaction, 1)
    before = row.model_dump()
    with pytest.raises(ValueError, match="reserved"):
        set_reconciliation_note(session, row, EVIDENCE, actor_label="reviewer")
    with pytest.raises(ValueError, match="reserved"):
        set_employee_allocation(session, row, employee_amount="25", business_category="grading_fees",
            estimated=True, note=EVIDENCE, actor_label="reviewer")
    rule = LedgerRule(name="bad marker", conditions_json="{}", actions_json=json.dumps({
        "match_override_status": "force_unmatched", "note": EVIDENCE, "category": "payroll"}))
    with pytest.raises(ValueError, match="reserved"):
        apply_ledger_rule(session, rule, filters=LedgerFilters(status="all"))
    assert row.model_dump() == before
    assert session.exec(select(AuditLog)).all() == []


def test_legacy_sqlite_columns_upgrade_and_backfill(session, monkeypatch):
    from app import db
    engine = session.get_bind()
    session.close()
    with engine.begin() as c:
        c.execute(text("ALTER TABLE bank_transactions DROP COLUMN discord_overlap_unresolved"))
        c.execute(text("ALTER TABLE bank_transactions DROP COLUMN discord_overlap_note"))
    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(db, "database_url", str(engine.url))
    db.ensure_sqlite_schema()
    with Session(engine) as s:
        assert migrate_legacy_discord_overlaps(s) == 1
        s.commit()
        assert overlap_warning_note(s.get(BankTransaction, 1)) == EVIDENCE


def test_startup_migration_survives_file_database_restart_and_fails_closed(tmp_path, monkeypatch):
    from app import db
    from app.ledger import _bank_row_view
    from app.discord.bank_reconciliation import build_finance_bank_expense_data
    from app.routers.ledger import ledger_row_force_unmatch_form
    url = f"sqlite:///{(tmp_path / 'overlap.sqlite3').as_posix()}"
    engine = create_engine(url)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(BankStatementImport(id=1, label="test", account_label="checking"))
        s.add(BankTransaction(id=1, import_id=1, row_index=1, account_label="checking",
            amount=-100, posted_at=datetime(2026, 9, 1), description="PSA",
            match_override_note=EVIDENCE))
        s.commit()
    with engine.begin() as c:
        c.execute(text("ALTER TABLE bank_transactions DROP COLUMN discord_overlap_unresolved"))
        c.execute(text("ALTER TABLE bank_transactions DROP COLUMN discord_overlap_note"))
    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(db, "database_url", url)
    monkeypatch.setattr(db.settings, "employee_portal_enabled", False)
    db.init_db()
    with Session(engine) as s:
        request = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "session": {}})
        with patch("app.routers.ledger.require_role_response", return_value=None), patch(
            "app.routers.ledger.current_user_label", return_value="reviewer"):
            assert ledger_row_force_unmatch_form(request, 1, mode="clear", note="", session=s).status_code == 303
    engine.dispose()
    engine = create_engine(url)
    monkeypatch.setattr(db, "engine", engine)
    db.init_db()
    with Session(engine) as s:
        row = s.get(BankTransaction, 1)
        assert row.match_override_note is None
        assert _bank_row_view(row)["discord_overlap_warning"] == EVIDENCE
        result = build_finance_bank_expense_data(s, start=datetime(2026, 9, 1), end=datetime(2026, 9, 30))
        assert result["reconciliation_warnings"] == [{"id": 1, "note": EVIDENCE}]
        assert len(s.exec(select(AuditLog).where(AuditLog.action == "financial.bank_overlap.migrate")).all()) == 1
    with patch("app.bank_overlap.migrate_legacy_discord_overlaps", side_effect=RuntimeError("migration failed")):
        with pytest.raises(RuntimeError, match="migration failed"):
            db.init_db()
    engine.dispose()
