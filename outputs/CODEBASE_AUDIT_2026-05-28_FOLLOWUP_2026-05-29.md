# Codebase Audit Follow-Up - 2026-05-29

Scope: follow-up on `outputs/CODEBASE_AUDIT_2026-05-28.md` against current `main` at `f5faa03`.

## Closed / stale finding

### F-1: `/reports` Combined Revenue includes loan proceeds

Status: stale / already fixed on current `main`.

Evidence:
- `app/routers/reports.py` computes `discord_operating_money_in = discord_operating_money_in_from_totals(discord_summary["totals"])` before `report_totals["combined_revenue"]`.
- `app/shared.py` has `discord_operating_money_in_from_totals()`, which subtracts `non_operating_money_in` from gross Discord `money_in`.
- `app/shared.py::build_report_period_comparison_rows()` uses the same helper before computing period `combined_revenue`.
- Existing regression coverage: `tests/test_tiktok_reporting.py::test_report_period_rows_exclude_discord_non_operating_cash_in`.
- Manual runtime check with `$1,000` sale plus `$5,000` loan draw returned period `combined_revenue = 1000.0`, not `6000.0`.

Focused verification run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_tiktok_reporting.py -k report_period_rows_exclude_discord_non_operating_cash_in -q
```

Result: `1 passed, 67 deselected`.

## Fixed in this follow-up

### `/admin/logs` stored XSS

Fix: escape tailed log content before inserting it into the admin HTML response, and normalize invalid `file` query values before echoing them in line-count links.

Regression test:
- `tests/test_admin_logs_security.py::test_admin_logs_escape_log_content`
- `tests/test_admin_logs_security.py::test_admin_logs_does_not_echo_invalid_file_query`

### `Nx amount` parser total bug

Fix: skip numbers that are immediately preceded by a quantity multiplier such as `5x`, both in unlabeled amount extraction and payment segment extraction. The fallback parser also drops the multiplier itself as the parsed amount, leaving the row review-required without a bogus amount instead of recording the multiplier or per-unit price as the full transaction total.

Regression tests:
- `tests/test_amount_and_loan_parsing.py::TestExtractUnlabeledAmount::test_ignores_price_after_leading_multiplier_quantity`
- `tests/test_amount_and_loan_parsing.py::TestExtractPaymentSegments::test_ignores_payment_amount_after_leading_multiplier_quantity`
- `tests/test_amount_and_loan_parsing.py::TestParseByRulesEndToEnd::test_leading_multiplier_does_not_become_review_amount`

### `managed_session` post-yield retry bug

Fix: keep connection/pre-yield retry behavior, but do not catch and retry `OperationalError` thrown by caller code inside the `with managed_session()` block. Caller errors now propagate as their original exception type instead of being converted to `RuntimeError("generator didn't stop after throw()")`.

Regression test:
- `tests/test_db_session.py::test_managed_session_preserves_caller_operational_error_type`
- `tests/test_db_session.py::test_managed_session_closes_session_when_postgres_health_check_fails`

### Follow-up review item: `managed_session` pre-yield session close

Claude's re-audit found that the first `managed_session` fix moved `session.close()` out of the pre-yield `OperationalError` path. That would leak the just-created session if the Postgres `SELECT 1` health check failed before yielding.

Fix: close the session at the top of the pre-yield `except OperationalError` branch before retrying or re-raising.

Regression test:
- `tests/test_db_session.py::test_managed_session_closes_session_when_postgres_health_check_fails`
