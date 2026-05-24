from pathlib import Path


TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "app" / "templates"


def _template(name: str) -> str:
    return (TEMPLATE_DIR / name).read_text(encoding="utf-8")


def test_financial_sidebar_separates_reports_sheet_imports_and_bank_reconciliation():
    source = _template("_linear_sidebar.html")

    assert '_ls_item("/reports", "Channel Comparison"' in source
    assert '_ls_item("/bookkeeping", "Sheet Imports"' in source
    assert '_ls_item("/bookkeeping/bank", "Bank Reconciliation"' in source
    assert '_ls_item("/bookkeeping/gmail", "Gmail Receipts"' in source


def test_finance_quick_link_names_channel_comparison():
    source = _template("finance.html")

    assert "Channel comparison" in source
    assert "Legacy reports" not in source


def test_reports_template_uses_shared_money_filter_and_clickable_scope_chips():
    source = _template("reports.html")

    assert '${{ "%.2f"|format' not in source
    assert '"$%.2f"|format' not in source
    assert "|money(2)" in source
    assert '<a class="scope-chip" href="{{ reports_url(source=\'discord\'' in source
    assert '{{ discord_summary["rows"] }} Discord transactions</a>' in source
    assert ".period-comparison-table th:first-child" in source
    assert "position: sticky" in source


def test_bookkeeping_templates_use_shared_money_filter():
    bookkeeping = _template("bookkeeping.html")
    bank = _template("bank_reconciliation.html")
    gmail = _template("gmail_financials.html")

    assert '"$%.2f"|format' not in bookkeeping
    assert "|money(2)" in bookkeeping
    assert "|money(2)" in bank
    assert "|money(2)" in gmail


def test_bank_reconciliation_tabs_preserve_filter_state_and_recategorize_confirms():
    source = _template("bank_reconciliation.html")

    assert "return confirm('Re-categorize this bank import?" in source
    assert "bank_url(" in source
    assert "Gmail Evidence" not in source
    assert "/bookkeeping/gmail/sync-form" not in source
    assert 'href="/bookkeeping/bank?import_id={{ selected_import.id }}&classification=' not in source
    assert 'href="/bookkeeping/bank?import_id={{ selected_import.id }}&review_status=' not in source


def test_gmail_financials_template_has_receipt_transaction_table():
    source = _template("gmail_financials.html")

    assert '<form method="post" action="/bookkeeping/gmail/sync-form"' in source
    assert "Gmail Receipts" in source
    assert "SortSwift" in source
    assert "<th>Transaction</th>" in source
    assert "<th>Bank Link</th>" in source
    assert 'name="start_date"' in source
    assert 'value="2026-01-01"' in source
    assert 'name="sync_limit"' in source
