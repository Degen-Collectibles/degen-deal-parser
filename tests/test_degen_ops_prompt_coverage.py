from pathlib import Path
import json
import subprocess
import sys

from scripts.degen_ops_prompt_coverage import build_prompt_coverage_report


def test_prompt_coverage_confirms_core_ops_questions_have_tools():
    report = build_prompt_coverage_report()

    assert report["ok"] is True
    cases = {case["id"]: case for case in report["cases"]}
    assert cases["tiktok_151_sales"]["status"] == "pass"
    assert "get_tiktok_product_sales" in cases["tiktok_151_sales"]["available_tools"]
    assert cases["price_and_market_trend"]["status"] == "pass"
    assert "get_price_lookup" in cases["price_and_market_trend"]["available_tools"]
    assert "get_market_trend_lookup" in cases["price_and_market_trend"]["available_tools"]
    assert cases["owner_employee_clock_status"]["scope"] == "owner"
    assert "get_employee_clock_status" in cases["owner_employee_clock_status"]["available_tools"]
    assert cases["partner_buy_decision"]["scope"] == "partner"
    assert "get_cash_snapshot" in cases["partner_buy_decision"]["forbidden_tools_absent"]


def test_prompt_coverage_script_outputs_clean_json():
    script = Path.cwd() / "scripts" / "degen_ops_prompt_coverage.py"

    result = subprocess.run(
        [sys.executable, str(script), "--json"],
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["ok"] is True
    assert report["name"] == "degen_ops_prompt_coverage"
    assert "[logging]" not in result.stdout


def test_prompt_coverage_marks_missing_required_tool_as_failure():
    report = build_prompt_coverage_report(
        cases=[
            {
                "id": "bad",
                "scope": "employee",
                "prompt": "Show cash.",
                "required_tools": ["get_cash_snapshot"],
                "forbidden_tools": [],
            }
        ]
    )

    assert report["ok"] is False
    assert report["cases"][0]["status"] == "fail"
    assert report["cases"][0]["missing_required_tools"] == ["get_cash_snapshot"]
