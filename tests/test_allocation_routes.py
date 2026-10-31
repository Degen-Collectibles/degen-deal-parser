from unittest.mock import patch
from starlette.requests import Request
from sqlmodel import Session, SQLModel, create_engine

from app.models import BankStatementImport, BankTransaction
from app.routers.ledger import ledger_row_allocation_form


def request():
    return Request({'type':'http','method':'POST','path':'/ledger/rows/1/allocation-form','headers':[], 'session':{}})


def test_allocation_route_reviewer_guard_and_validation():
    from fastapi.responses import JSONResponse
    denial=JSONResponse({'error':'forbidden'},status_code=403)
    with patch('app.routers.ledger.require_role_response',return_value=denial):
        response=ledger_row_allocation_form(1,request(),employee_amount='25',business_category='grading_fees',note='test',estimated=True,session=None)
    assert response.status_code==403
    e=create_engine('sqlite:///:memory:');SQLModel.metadata.create_all(e)
    with Session(e) as s:
        s.add(BankStatementImport(id=1,label='test',account_label='checking'))
        row=BankTransaction(id=1,import_id=1,row_index=1,account_label='checking',amount=-100,expense_category='grading_fees')
        s.add(row);s.commit()
        with patch('app.routers.ledger.require_role_response',return_value=None), patch('app.routers.ledger.current_user_label',return_value='reviewer'):
            response=ledger_row_allocation_form(1,request(),employee_amount='101',business_category='grading_fees',note='test',estimated=True,session=s)
            assert response.status_code==400 and row.allocations_json=='[]'
            response=ledger_row_allocation_form(1,request(),employee_amount='25',business_category='grading_fees',note='test',estimated=True,session=s)
            assert response.status_code==303 and '2500' in row.allocations_json


def test_split_export_uses_parts_and_bookkeeping_route_blocks_overwrite():
    from app.routers.ledger import _ledger_export_rows, LEDGER_EXPORT_COLUMNS
    from app.ledger import LedgerFilters
    from app.routers.bookkeeping import bank_reconciliation_row_status_form
    from app.bank_allocations import set_employee_allocation
    e=create_engine('sqlite:///:memory:');SQLModel.metadata.create_all(e)
    with Session(e) as s:
        s.add(BankStatementImport(id=1,label='test',account_label='checking'))
        row=BankTransaction(id=1,import_id=1,row_index=1,account_label='checking',amount=-100,expense_category='grading_fees')
        s.add(row);s.commit()
        set_employee_allocation(s,row,employee_amount='25',business_category='grading_fees',estimated=True,note='test',actor_label='reviewer');s.commit()
        rows=_ledger_export_rows(s,LedgerFilters(status='all'))
        parts=[dict(zip(LEDGER_EXPORT_COLUMNS,r)) for r in rows]
        assert sum(r['amount'] for r in parts)==-100
        assert {r['category']:r['amount'] for r in parts}=={'employee_advances':-25,'grading_fees':-75}
        with patch('app.routers.bookkeeping.require_role_response',return_value=None):
            result=bank_reconciliation_row_status_form(request(),1,import_id=1,review_status='open',classification='logged_in_discord_strong',expense_category='',note='',session=s)
        assert result.status_code==400 and row.classification!='logged_in_discord_strong'


def test_discord_edit_rejects_bank_only_employee_categories():
    from app.routers.ledger import ledger_transaction_edit_form
    for category in ['employee_advances','employee_reimbursements']:
        with patch('app.routers.ledger.require_role_response',return_value=None):
            response=ledger_transaction_edit_form(request(),1,expense_category=category,session=None)
        assert response.status_code==400
