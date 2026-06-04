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


def test_answer_eval_accepts_partner_buy_answer_with_evidence_and_redaction():
    report = build_answer_eval_report(
        answers={
            "partner_buy_decision": GOOD_PARTNER_ANSWER,
            "employee_inventory_velocity": GOOD_EMPLOYEE_ANSWER,
        }
    )

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
    answers = {
        "partner_buy_decision": GOOD_PARTNER_ANSWER,
        "employee_inventory_velocity": GOOD_EMPLOYEE_ANSWER,
    }

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
        json.dumps(
            {
                "partner_buy_decision": GOOD_PARTNER_ANSWER,
                "employee_inventory_velocity": GOOD_EMPLOYEE_ANSWER,
            }
        ),
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
