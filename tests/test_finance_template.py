from pathlib import Path

from app.shared import build_finance_kpi_rows


FINANCE_TEMPLATE = Path(__file__).resolve().parents[1] / "app" / "templates" / "finance.html"


def _template_text() -> str:
    return FINANCE_TEMPLATE.read_text(encoding="utf-8")


def test_finance_hero_uses_estimated_margin_owner_language():
    template = _template_text()

    assert "<h1>Finance Dashboard</h1>" in template
    assert '<div class="hero-label">Estimated Operating Profit</div>' in template
    assert "20% gross product margin" in template
    assert '<div class="hero-label">Operating Cash Profit</div>' not in template


def test_finance_surfaces_readiness_before_kpis():
    template = _template_text()

    readiness_index = template.index('class="finance-readiness"')
    kpi_index = template.index('class="kpi-grid"')

    assert readiness_index < kpi_index
    assert "Data confidence" in template
    assert "Open ledger cleanup" in template

    data_quality_anchor = template.index('id="data-quality"')
    data_quality_block = template[data_quality_anchor : data_quality_anchor + 500]
    assert '<h2 class="section-title">Data Quality Details</h2>' in data_quality_block


def test_finance_hero_uses_named_kpi_context():
    template = _template_text()

    assert "kpi_rows[2]" not in template
    assert "finance_hero_kpi" in template


def test_finance_kpi_rows_expose_stable_keys():
    current = {
        "revenue": 1000.0,
        "gross_profit": 700.0,
        "operating_profit": 450.0,
        "operating_margin_pct": 45.0,
        "inventory_spend": 300.0,
        "external_tax": 82.0,
    }
    prior = {
        "revenue": 900.0,
        "gross_profit": 600.0,
        "operating_profit": 400.0,
        "operating_margin_pct": 44.0,
        "inventory_spend": 280.0,
        "external_tax": 80.0,
    }

    rows = build_finance_kpi_rows(current, prior)
    rows_by_key = {row["key"]: row for row in rows}

    assert rows_by_key["operating_profit"]["label"] == "Estimated Operating Profit"
    assert rows_by_key["operating_profit"]["value_display"] == "$450"


def test_finance_kpi_grid_does_not_force_clipped_desktop_cards():
    template = _template_text()

    assert "repeat(auto-fit, minmax(170px, 1fr))" in template
    assert ".kpi-grid { grid-template-columns: repeat(3" not in template


def test_finance_kpi_values_stay_on_one_line_in_narrow_cards():
    template = _template_text()

    assert ".metric-card {" in template
    assert "container-type: inline-size;" in template
    assert ".metric-value {" in template
    assert "white-space: nowrap;" in template
    assert "overflow-wrap: normal;" in template
    assert "font-size: clamp(28px, 18cqw, 40px);" in template
    assert "font-size: clamp(28px, 2.6vw, 40px);" not in template


def test_finance_date_filters_label_pacific_time():
    template = _template_text()

    assert "Start (Pacific Time)" in template
    assert "End (Pacific Time)" in template
