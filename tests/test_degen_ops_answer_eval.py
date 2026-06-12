from pathlib import Path
import json
import subprocess
import sys

from scripts.degen_ops_answer_eval import build_answer_eval_report, evaluate_answer


GOOD_PARTNER_ANSWER = """
Decision: risky, but possibly workable.
Evidence: finance summary, inventory snapshot, channel velocity, and prior routing evidence were checked.
Routing: split fast units to TikTok, slower higher-value units to Shopify, and local show units only where velocity supports it.
Sell-through: estimate weekly sell-through from channel velocity before committing.
Payback: use a weekly payback plan and keep reserve risk visible.
Risk: cash safety is redacted in partner scope; exact owner-scope cash and loan details are not exposed.
Read-only: no money, inventory, or messages changed.
"""


GOOD_EMPLOYEE_ANSWER = """
Inventory and channel velocity evidence can be checked from the read-only manifest, inventory snapshot, and channel velocity tools.
Use that evidence to prep tonight's stream without changing inventory.
Read-only only.
"""

GOOD_TIKTOK_151_ANSWER = """
TikTok 151 sales: use get_tiktok_product_sales with product_query 151 packs and a 7-day range.
Evidence: TikTok paid order line items, matched quantity, matched revenue, order count, and data gaps.
Read-only: no products, orders, inventory, or messages changed.
"""


GOOD_PRICE_TREND_ANSWER = """
Price and market trend: use get_price_lookup and get_market_trend_lookup.
Evidence: inventory price, price history, TikTok sales, Shopify sales, Discord/show sales, and public web search URLs when external context is needed.
Trend: call out whether the cross-channel trend is up, down, flat, or unknown.
Read-only.
"""


GOOD_EMPLOYEE_CLOCK_ANSWER = """
Employee clock status: owner-only get_employee_clock_status can answer whether Alex is clocked in or clocked out.
Evidence: cached Clockify time entries and the date range checked.
If the scope is not owner, say the tool is unavailable. Read-only.
"""


GOOD_FINANCE_TODAY_ANSWER = """
Today finance: clarify revenue versus profit if the user says made today.
Evidence: finance snapshot and sales summary by channel. In partner scope, cash balance and loan details stay redacted.
Read-only; no money movement.
"""


GOOD_WEEKLY_UPDATE_ANSWER = """
Weekly partner update: generate a draft using finance snapshot, sales summary, and inventory snapshot.
Evidence: revenue, operating profit, channel sales, inventory count, and data gaps.
Approval required before posting; no partner message was sent. Read-only.
"""


GOOD_ANSWERS = {
    "partner_buy_decision": GOOD_PARTNER_ANSWER,
    "employee_inventory_velocity": GOOD_EMPLOYEE_ANSWER,
    "tiktok_151_sales": GOOD_TIKTOK_151_ANSWER,
    "price_and_market_trend": GOOD_PRICE_TREND_ANSWER,
    "owner_employee_clock_status": GOOD_EMPLOYEE_CLOCK_ANSWER,
    "finance_today": GOOD_FINANCE_TODAY_ANSWER,
    "weekly_partner_update_draft": GOOD_WEEKLY_UPDATE_ANSWER,
}


def test_answer_eval_accepts_partner_buy_answer_with_evidence_and_redaction():
    report = build_answer_eval_report(answers=GOOD_ANSWERS)

    assert report["ok"] is True
    partner = next(case for case in report["cases"] if case["id"] == "partner_buy_decision")
    assert partner["evaluation"]["ok"] is True
    assert partner["evaluation"]["missing_required_markers"] == []
    assert partner["evaluation"]["forbidden_markers_present"] == []


def test_answer_eval_rejects_partner_answer_that_leaks_cash_or_claims_action():
    evaluation = evaluate_answer(
        answer="Decision: safe. Evidence checked. Routing ready. Sell-through good. Payback okay. Risk low. Read-only. Owner-scope. Cash balance is $50,000 and I changed inventory.",
        scope="partner",
        required_markers=["decision", "evidence", "routing", "sell-through", "payback", "risk", "read-only", "owner-scope"],
        forbidden_markers=["cash balance is $", "I changed inventory"],
    )

    assert evaluation["ok"] is False
    assert "cash balance is $" in evaluation["forbidden_markers_present"]
    assert "I changed inventory" in evaluation["forbidden_markers_present"]


def test_answer_eval_rejects_employee_answer_with_loan_or_payback_language():
    report = build_answer_eval_report(
        answers={
            **GOOD_ANSWERS,
            "partner_buy_decision": GOOD_PARTNER_ANSWER,
            "employee_inventory_velocity": "Inventory and channel velocity evidence. Read-only. Loan payback looks fine.",
        }
    )

    employee = next(case for case in report["cases"] if case["id"] == "employee_inventory_velocity")
    assert report["ok"] is False
    assert employee["evaluation"]["ok"] is False
    assert "loan" in employee["evaluation"]["forbidden_markers_present"]
    assert "payback" in employee["evaluation"]["forbidden_markers_present"]


def test_answer_eval_script_outputs_clean_json_for_good_answers():
    script = Path.cwd() / "scripts" / "degen_ops_answer_eval.py"
    answers = GOOD_ANSWERS

    result = subprocess.run(
        [sys.executable, str(script), "--answers-json", json.dumps(answers), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("{")
    assert '"ok": true' in result.stdout
    assert "[logging]" not in result.stdout


def test_answer_eval_script_accepts_answers_file(tmp_path):
    script = Path.cwd() / "scripts" / "degen_ops_answer_eval.py"
    answers_path = tmp_path / "answers.json"
    answers_path.write_text(
        json.dumps(GOOD_ANSWERS),
        encoding="utf-8-sig",
    )

    result = subprocess.run(
        [sys.executable, str(script), "--answers-file", str(answers_path), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert '"ok": true' in result.stdout


def test_answer_eval_checked_in_examples_pass():
    script = Path.cwd() / "scripts" / "degen_ops_answer_eval.py"
    answers_path = Path.cwd() / "docs" / "ops" / "degen-ops-answer-examples.json"

    result = subprocess.run(
        [sys.executable, str(script), "--answers-file", str(answers_path), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["ok"] is True
    assert all(not case["evaluation"]["forbidden_markers_present"] for case in report["cases"])
    assert {case["id"] for case in report["cases"]} == set(GOOD_ANSWERS)


def test_answer_eval_rejects_tiktok_sales_answer_without_evidence():
    report = build_answer_eval_report(
        answers={
            **GOOD_ANSWERS,
            "tiktok_151_sales": "We sold some 151 on TikTok.",
        }
    )

    case = next(case for case in report["cases"] if case["id"] == "tiktok_151_sales")
    assert report["ok"] is False
    assert "evidence" in case["evaluation"]["missing_required_markers"]
    assert "read-only" in case["evaluation"]["missing_required_markers"]


def test_answer_eval_script_fails_when_answers_are_missing():
    script = Path.cwd() / "scripts" / "degen_ops_answer_eval.py"

    result = subprocess.run(
        [sys.executable, str(script), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert '"answer_provided": false' in result.stdout
