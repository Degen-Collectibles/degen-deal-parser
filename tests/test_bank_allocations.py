import json
from datetime import datetime

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.bank_allocations import bank_allocations, expand_bank_allocations, set_employee_allocation
from app.models import BankStatementImport, BankTransaction, AuditLog
from app.discord.bank_reconciliation import build_finance_bank_expense_data, dedupe_bank_rows_for_reporting, summarize_bank_transactions
from app.discord.bookkeeping import infer_occurred_at, normalize_bookkeeping_rows, read_tabular_rows


def bank(**kwargs):
    return BankTransaction(**dict(dict(import_id=1, row_index=1, account_label="checking", account_type="checking",
        amount=-3410.37, description="PSA", expense_category="grading_fees",
        classification="expense_or_purchase_needs_review", posted_at=datetime(2026,9,23)), **kwargs))


def test_split_preserves_cash_and_excludes_employee_share_with_audit():
    engine=create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(BankStatementImport(id=1,label="test",account_label="checking"));s.commit()
        row=bank();s.add(row);s.commit()
        set_employee_allocation(s,row,employee_amount="852.59",business_category="grading_fees",estimated=True,note="Owner 25/75 estimate",actor_label="reviewer")
        s.commit()
        data=build_finance_bank_expense_data(s,start=datetime(2026,9,1),end=datetime(2026,9,30))
        assert data['gross_outflow_total']==3410.37
        assert data['bank_only_total']==3410.37
        assert data['non_operating_total']==852.59
        assert data['operating_total']==2557.78
        assert data['row_count']==data['bank_only_count']==1
        assert row.amount==-3410.37
        assert s.exec(select(AuditLog)).one().action=='financial.bank_allocation.edit'
        summary=summarize_bank_transactions([row])
        assert summary['expense_total']==2557.78 and summary['rows']==1
        assert bank_allocations(row)[0]['estimated'] is True
        set_employee_allocation(s,row,employee_amount="0",business_category="grading_fees",estimated=False,note="Clear estimate",actor_label="reviewer")
        assert bank_allocations(row)==[]


@pytest.mark.parametrize('amount',['3410.38','-1','NaN','Infinity','0.001'])
def test_bad_split_rejected_without_mutating(amount):
    row=bank();before=row.model_dump()
    with pytest.raises(ValueError):
        set_employee_allocation(None,row,employee_amount=amount,business_category="grading_fees",estimated=True,note="test",actor_label="reviewer")
    assert row.model_dump()==before


def test_reimbursement_receipt_is_non_sales_and_preserves_original():
    row=bank(amount=100,expense_category="sales_collections")
    class SessionStub:
        def add(self, obj): pass
    set_employee_allocation(SessionStub(),row,employee_amount="75",business_category="sales_collections",estimated=False,note="Repays documented employee grading",actor_label="reviewer")
    parts=expand_bank_allocations([row])
    assert [(r.expense_category,r.amount) for r in parts]==[('employee_reimbursements',75),('sales_collections',25)]
    assert row.amount==100 and row.match_override_status=='force_unmatched'


def test_allocations_follow_relinked_duplicate_without_losing_repeated_payments():
    first=bank(id=1,created_at=datetime(2026,9,24))
    duplicate=bank(id=2,import_id=2,created_at=datetime(2026,9,25),allocations_json=json.dumps([{'category':'employee_advances','amount_cents':341037}]))
    repeated=bank(id=3,row_index=2,created_at=datetime(2026,9,24))
    rows=dedupe_bank_rows_for_reporting([first,duplicate,repeated])
    assert len(rows)==2 and rows[0].id==1
    assert bank_allocations(rows[0])[0]['category']=='employee_advances'
    assert first.allocations_json=='[]'  # no in-memory ORM mutation


def test_drift_and_discord_overlap_fail_visibly():
    row=bank(allocations_json='[{"category":"employee_advances","amount_cents":341037}]')
    row.amount=-3411
    with pytest.raises(ValueError,match='no longer equals'):bank_allocations(row)
    row.amount=-3410.37;row.matched_transaction_id=123
    with pytest.raises(ValueError,match='also matched'):bank_allocations(row)


def test_money_never_becomes_date_but_date_column_serials_work():
    assert infer_occurred_at({'description':'Sealed lot','amount':35054}) is None
    assert infer_occurred_at({'candidate_cost':35054}) is None
    assert infer_occurred_at({'date':35054,'amount':12})==datetime(1995,12,21)
    assert infer_occurred_at({'column_1':'2026-10-01','amount':35054})==datetime(2026,10,1)


@pytest.mark.parametrize('sheet',['Codes + Calculation','MONTHLY EXPENSES','Overview'])
def test_summary_sheets_not_imported(sheet):
    assert normalize_bookkeeping_rows([{'__sheet_name':sheet,'label':'Sealed','amount':35054}])==[]


def test_summary_rows_skipped_but_actual_total_named_product_kept():
    rows=normalize_bookkeeping_rows([
        {'__sheet_name':'INCOME','description':'Subtotal','amount':35054},
        {'__sheet_name':'INCOME','description':'Total Recall card','amount':25},
        {'__sheet_name':'SPENDING','description':'Sealed lot','amount':32570},
    ])
    assert len(rows)==2 and all(r['occurred_at'] is None for r in rows)


def test_xlsx_and_csv_roundtrip_keep_real_transactions_only():
    from io import BytesIO
    from openpyxl import Workbook
    workbook=Workbook();sheet=workbook.active;sheet.title='INCOME'
    sheet.append(['Date','Description','Amount']);sheet.append([datetime(2026,10,1),'Sale',35054]);sheet.append([None,'Subtotal',35054])
    stream=BytesIO();workbook.save(stream)
    xlsx=normalize_bookkeeping_rows(read_tabular_rows('test.xlsx',stream.getvalue()))
    csv=normalize_bookkeeping_rows(read_tabular_rows('test.csv',b'Date,Description,Amount\n2026-10-01,Sale,35054\n,Subtotal,35054\n'))
    assert len(xlsx)==len(csv)==1
    assert xlsx[0]['occurred_at']==csv[0]['occurred_at']==datetime(2026,10,1)


def test_legacy_summary_quarantined_and_excluded_from_match_totals():
    from app.models import BookkeepingEntry, BookkeepingImport
    from app.discord.bookkeeping import reconcile_bookkeeping_import
    engine=create_engine('sqlite:///:memory:');SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        imp=BookkeepingImport(show_label='test');s.add(imp);s.commit()
        row=BookkeepingEntry(import_id=imp.id,row_index=1,sheet_name='Codes + Calculation',entry_kind='sale',amount=35054,raw_row_json='{"label":"Sealed","amount":35054}',matched_transaction_id=99,match_status='matched_exact')
        s.add(row);s.commit()
        data=reconcile_bookkeeping_import(s,imp.id)
        assert data['summary']['import_net']==0
        assert data['summary']['excluded_summary_rows']==1
        assert row.amount==35054 and row.match_status=='matched_exact' and row.matched_transaction_id==99
        assert len(s.exec(select(AuditLog)).all())==0
        reconcile_bookkeeping_import(s,imp.id)
        assert len(s.exec(select(AuditLog)).all())==0


def test_plaid_modified_row_preserves_manual_split_and_unmatch(monkeypatch):
    from app.discord.plaid_bank_feed import _apply_payloads
    engine=create_engine('sqlite:///:memory:');SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        imp=BankStatementImport(id=1,label='test',account_label='checking');s.add(imp);s.commit()
        row=bank(provider_transaction_id='test-provider');s.add(row);s.commit()
        set_employee_allocation(s,row,employee_amount='852.59',business_category='grading_fees',estimated=True,note='test',actor_label='test');s.commit()
        def matcher(payloads, transactions):
            assert payloads[0]['match_override_status']=='force_unmatched'
            payloads[0].update(classification='expense_or_purchase_needs_review',matched_transaction_id=None)
        monkeypatch.setattr('app.discord.plaid_bank_feed.match_bank_rows_to_transactions',matcher)
        monkeypatch.setattr('app.discord.plaid_bank_feed.load_matchable_transactions',lambda *args: [])
        _apply_payloads(s,imp,[{'provider_transaction_id':'test-provider','amount':-3410.37,'description':'PSA','posted_at':datetime(2026,9,23),'row_index':1}])
        assert row.matched_transaction_id is None
        assert bank_allocations(row)[0]['amount_cents']==85259


def test_existing_bank_claim_blocks_another_payment_but_allows_own_refresh():
    from app.models import Transaction
    from app.discord.bank_reconciliation import load_matchable_transactions, match_bank_rows_to_transactions
    e=create_engine('sqlite:///:memory:');SQLModel.metadata.create_all(e)
    with Session(e) as s:
        s.add(BankStatementImport(id=1,label='test',account_label='checking'))
        s.add(Transaction(id=5,source_message_id=5,occurred_at=datetime(2026,9,23),entry_kind='sale',amount=120,money_in=120,money_out=0,parse_status='parsed',needs_review=False,payment_method='zelle'))
        first=bank(id=1,amount=120,description='Zelle payment from Alice',matched_transaction_id=5,provider_transaction_id='alice',expense_category='sales_collections')
        s.add(first);s.commit()
        second={'amount':120,'posted_at':datetime(2026,9,24),'description':'Zelle payment from Bob','account_label':'checking','account_type':'checking','import_id':2}
        transactions=load_matchable_transactions(s,[second])
        assert second['unavailable_transaction_ids']=={5}
        match_bank_rows_to_transactions([second],transactions)
        assert second['matched_transaction_id'] is None
        own={'id':1,'amount':120,'posted_at':datetime(2026,9,23),'description':first.description,'provider_transaction_id':'alice','import_id':1}
        load_matchable_transactions(s,[own])
        assert own['unavailable_transaction_ids']==set()


def test_duplicate_split_edit_clear_and_matched_group_guard():
    e=create_engine('sqlite:///:memory:');SQLModel.metadata.create_all(e)
    with Session(e) as s:
        s.add_all([BankStatementImport(id=i,label='test',account_label='checking') for i in [1,2]])
        first=bank(id=1);second=bank(id=2,import_id=2)
        s.add_all([first,second]);s.commit()
        args=dict(business_category='grading_fees',estimated=True,note='test',actor_label='reviewer')
        set_employee_allocation(s,second,employee_amount='852.59',**args);s.commit()
        assert first.allocations_json==second.allocations_json
        set_employee_allocation(s,first,employee_amount='900',**args);s.commit()
        assert bank_allocations(dedupe_bank_rows_for_reporting([first,second])[0])[0]['amount_cents']==90000
        set_employee_allocation(s,first,employee_amount='0',**args);s.commit()
        assert first.allocations_json==second.allocations_json=='[]'
        second.matched_transaction_id=99
        with pytest.raises(ValueError,match='all duplicate'):
            set_employee_allocation(s,first,employee_amount='100',**args)


def test_invalid_allocation_does_not_break_summary_and_can_clear():
    row=bank(allocations_json='[{"category":"employee_advances","amount_cents":1}]')
    summary=summarize_bank_transactions([row])
    assert summary['debits']==-3410.37 and summary['expense_total']==3410.37
    assert summary['allocation_errors'] and summary['uncategorized_expenses']==1
    class Stub:
        def add(self,obj):pass
    row.classification='logged_in_discord_strong'
    set_employee_allocation(Stub(),row,employee_amount='0',business_category='grading_fees',estimated=False,note='clear invalid',actor_label='reviewer')
    assert row.allocations_json=='[]'
