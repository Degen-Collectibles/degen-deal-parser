import re
from pathlib import Path

import pytest

from app.inventory.barcode import (
    _BARCODE_AVAILABLE,
    DEFAULT_LABEL_FIELDS,
    LABEL_LAYOUT_OPTIONS,
    generate_barcode_value,
    label_context_for_items,
    parse_label_fields,
    render_barcode_svg,
)
from app.models import InventoryItem
from app.shared import templates


class _FakeUrl:
    path = "/inventory/labels"

    def include_query_params(self, **params):
        return "/inventory/labels?" + "&".join(f"{key}={value}" for key, value in params.items())


class _FakeRequest:
    url = _FakeUrl()


class _FakeUser:
    role = "admin"
    display_name = "Admin"
    username = "admin"


def test_generate_barcode_value_uses_stable_shop_prefix():
    assert generate_barcode_value(42) == "DGN-000042"


def test_render_barcode_svg_uses_real_code128_dependency():
    assert _BARCODE_AVAILABLE is True
    svg = render_barcode_svg("DGN-000042")
    assert "<svg" in svg
    assert "DGN-000042" in svg
    assert "fill:#000000" in svg or 'fill="black"' in svg or "fill:black" in svg


def test_label_context_includes_product_type_and_customer_price():
    item = InventoryItem(
        id=42,
        barcode="DGN-000042",
        item_type="sealed",
        game="Pokemon",
        card_name="Prismatic Evolutions Super Premium Collection",
        set_name="SV Prismatic Evolutions",
        sealed_product_kind="Super Premium Collection",
        condition="Sealed",
        auto_price=129.99,
        list_price=139.99,
    )

    label = label_context_for_items([item])[0]

    assert label["barcode_value"] == "DGN-000042"
    assert label["product_type"] == "Super Premium Collection"
    assert label["grade_or_condition"] == "Sealed"
    assert label["price_text"] == "$139.99"
    assert label["price_source"] == "Manual price"


def test_label_context_falls_back_to_market_price():
    item = InventoryItem(
        id=43,
        barcode="DGN-000043",
        item_type="single",
        game="Pokemon",
        card_name="Pikachu",
        condition="NM",
        auto_price=12.345,
    )

    label = label_context_for_items([item])[0]

    assert label["product_type"] == "Single"
    assert label["price_text"] == "$12.35"
    assert label["price_source"] == "Market price"


def test_label_context_builds_default_employee_fields_for_wraparound_labels():
    item = InventoryItem(
        id=45,
        barcode="DGN-000045",
        item_type="single",
        game="Pokemon",
        card_name="Charizard ex",
        set_name="Obsidian Flames",
        card_number="223/197",
        variant="Special Illustration Rare",
        condition="NM",
        location="Case A3",
        list_price=700,
    )

    label = label_context_for_items([item])[0]

    assert label["selected_fields"] == DEFAULT_LABEL_FIELDS
    assert label["item_title"] == "Charizard ex"
    assert label["employee_lines"] == [
        {"field": "barcode", "label": "Barcode", "value": "DGN-000045"},
        {"field": "name", "label": "Item", "value": "Charizard ex"},
        {"field": "set", "label": "Set", "value": "Obsidian Flames"},
        {"field": "condition", "label": "Condition", "value": "NM"},
        {"field": "location", "label": "Location", "value": "Case A3"},
    ]


def test_label_context_defaults_to_customer_logo():
    item = InventoryItem(
        id=49,
        barcode="DGN-000049",
        item_type="single",
        game="Pokemon",
        card_name="Charmander",
        condition="NM",
        list_price=4,
    )

    label = label_context_for_items([item])[0]

    assert "logo" in label["selected_fields"]
    assert [line["field"] for line in label["employee_lines"]] == ["barcode", "name", "condition"]


def test_label_context_respects_selected_employee_fields():
    item = InventoryItem(
        id=46,
        barcode="DGN-000046",
        item_type="slab",
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
        cert_number="12345678",
        location="Slab Case",
        list_price=1200,
    )

    label = label_context_for_items([item], selected_fields=["barcode", "name", "grade", "cert"])[0]

    assert [line["field"] for line in label["employee_lines"]] == ["barcode", "name", "grade", "cert"]
    assert label["employee_lines"][2]["value"] == "PSA 10"
    assert label["employee_lines"][3]["value"] == "Cert 12345678"


def test_label_context_marks_price_length_for_wraparound_fit():
    compact = InventoryItem(
        id=47,
        barcode="DGN-000047",
        item_type="single",
        game="Pokemon",
        card_name="Bulbasaur",
        list_price=10.84,
    )
    wide = InventoryItem(
        id=48,
        barcode="DGN-000048",
        item_type="single",
        game="Pokemon",
        card_name="Blastoise",
        list_price=1200,
    )
    very_wide = InventoryItem(
        id=50,
        barcode="DGN-000050",
        item_type="single",
        game="Pokemon",
        card_name="Trophy Pikachu",
        list_price=12345.67,
    )

    compact_label, wide_label, very_wide_label = label_context_for_items([compact, wide, very_wide])

    assert compact_label["price_class"] == "price-short"
    assert wide_label["price_class"] == "price-medium"
    assert very_wide_label["price_class"] == "price-long"


def test_parse_label_fields_defaults_and_filters_invalid_values():
    assert parse_label_fields([]) == DEFAULT_LABEL_FIELDS
    assert parse_label_fields([], default_to_all=False) == ()
    assert parse_label_fields(["name, set", "bogus", "location"]) == ("name", "set", "location")


def test_label_template_has_wrap_sheet_and_thermal_layouts():
    item = InventoryItem(
        id=44,
        barcode="DGN-000044",
        item_type="sealed",
        game="Pokemon",
        card_name="Test Booster Box",
        sealed_product_kind="Booster Box",
        auto_price=119.99,
    )
    label = label_context_for_items([item])[0]

    html = templates.env.get_template("inventory_labels.html").render(
        request=_FakeRequest(),
        current_user=_FakeUser(),
        csrf_token="",
        labels=[label],
        layout="wrap",
        label_layout_options=[
            {"value": "wrap", "label": "Wraparound"},
            {"value": "sheet", "label": "Sheet Labels"},
            {"value": "thermal", "label": '2.25" Thermal'},
        ],
        label_field_options=[
            {"value": "barcode", "label": "Barcode"},
            {"value": "name", "label": "Product name"},
        ],
        selected_fields=("barcode", "name"),
        ids="44",
        status="",
    )

    assert 'class="label-layout-wrap"' in html
    assert "Wraparound" in html
    assert "Sheet Labels" in html
    assert "2.25" in html
    assert "Thermal" in html
    assert "$119.99" in html
    assert "Test Booster Box" in html
    assert "Customer side" in html
    assert "Employee side" in html


def test_label_layout_options_include_compact_wrap_size():
    assert LABEL_LAYOUT_OPTIONS[0] == {"value": "wrap", "label": '3.5" x 1" Wraparound'}
    assert {"value": "wrap-3x1", "label": '3" x 1" Wraparound'} in LABEL_LAYOUT_OPTIONS


def test_compact_wrap_layout_renders_wraparound_label_markup():
    item = InventoryItem(
        id=61,
        barcode="DGN-000061",
        item_type="single",
        game="Pokemon",
        card_name="Charizard ex",
        set_name="151",
        condition="NM",
        location="Ungrouped",
        list_price=10.84,
    )
    label = label_context_for_items([item])[0]

    html = templates.env.get_template("inventory_labels.html").render(
        request=_FakeRequest(),
        current_user=_FakeUser(),
        csrf_token="",
        labels=[label],
        layout="wrap-3x1",
        label_layout_options=LABEL_LAYOUT_OPTIONS,
        label_field_options=[{"value": "barcode", "label": "Barcode"}],
        selected_fields=("barcode", "name", "condition", "location"),
        ids="61",
        status="",
    )

    assert 'class="label-layout-wrap-3x1"' in html
    assert 'data-label-section="customer"' in html
    assert 'data-label-section="employee"' in html
    assert 'body.label-layout-wrap-3x1 .wrap-label-card' in html
    assert "3in" in html


def test_wraparound_template_puts_optional_logo_above_customer_price():
    item = InventoryItem(
        id=62,
        barcode="DGN-000062",
        item_type="single",
        game="Pokemon",
        card_name="Pikachu",
        condition="NM",
        list_price=25,
    )
    label = label_context_for_items([item])[0]

    html = templates.env.get_template("inventory_labels.html").render(
        request=_FakeRequest(),
        current_user=_FakeUser(),
        csrf_token="",
        labels=[label],
        layout="wrap-3x1",
        label_layout_options=LABEL_LAYOUT_OPTIONS,
        label_field_options=[{"value": "logo", "label": "Logo"}],
        selected_fields=("logo", "barcode", "name", "condition"),
        ids="62",
        status="",
    )
    customer_section = html.split('data-label-section="customer"', 1)[1].split('data-label-section="employee"', 1)[0]

    assert 'class="wrap-logo"' in customer_section
    assert 'src="/static/degen-logo-label.png"' in customer_section
    assert customer_section.index('class="wrap-logo"') < customer_section.index('class="wrap-price price-short"')
    assert "Logo and price." in html


def test_wraparound_template_uses_print_safe_logo_and_price_styles():
    html = templates.env.get_template("inventory_labels.html").render(
        request=_FakeRequest(),
        current_user=_FakeUser(),
        csrf_token="",
        labels=[],
        layout="wrap-3x1",
        label_layout_options=LABEL_LAYOUT_OPTIONS,
        label_field_options=[{"value": "logo", "label": "Logo"}],
        selected_fields=("logo", "barcode", "name", "condition"),
        ids="",
        status="",
    )

    assert ".wrap-customer.has-logo" in html
    assert ".wrap-customer.no-logo" in html
    assert ".wrap-logo { display:block; width:auto; max-width:100%; height:.58in" in html
    assert "body.label-layout-wrap-3x1 .wrap-logo { height:.52in; }" in html
    assert ".wrap-price.price-long { font-size:11pt; }" in html
    assert "body.label-layout-wrap-3x1 .wrap-price.price-long { font-size:9pt; }" in html
    assert 'name="fields_present" value="1"' in html


def test_label_logo_asset_is_print_optimized_full_logo():
    original = Path("app/static/degen-logo.png")
    label_logo = Path("app/static/degen-logo-label.png")

    assert label_logo.exists()
    assert label_logo.stat().st_size < original.stat().st_size / 4

    from PIL import Image

    with Image.open(original) as original_img, Image.open(label_logo) as label_img:
        assert label_img.width <= 900
        assert abs((label_img.width / label_img.height) - (original_img.width / original_img.height)) < 0.01


def test_wraparound_labels_fit_in_print_media(tmp_path):
    pytest.importorskip("playwright.sync_api")
    from playwright.sync_api import Error as PlaywrightError, sync_playwright

    root = Path.cwd()
    logo_uri = (root / "app" / "static" / "degen-logo-label.png").resolve().as_uri()
    linear_uri = (root / "app" / "static" / "linear.css").resolve().as_uri()
    safe_area_uri = (root / "app" / "static" / "safe-area.css").resolve().as_uri()

    def render_html(layout: str) -> Path:
        items = [
            InventoryItem(
                id=70,
                barcode="DGN-000070",
                item_type="single",
                game="Pokemon",
                card_name="Charizard ex",
                set_name="151",
                condition="NM",
                location="Ungrouped",
                list_price=10.84,
            ),
            InventoryItem(
                id=71,
                barcode="DGN-000071",
                item_type="single",
                game="Pokemon",
                card_name="Trophy Pikachu",
                set_name="Promo",
                condition="NM",
                location="Case A",
                list_price=12345.67,
            ),
        ]
        labels = label_context_for_items(
            items,
            selected_fields=("logo", "barcode", "name", "set", "condition", "location"),
        )
        html = templates.env.get_template("inventory_labels.html").render(
            request=_FakeRequest(),
            current_user=_FakeUser(),
            csrf_token="",
            labels=labels,
            layout=layout,
            label_layout_options=LABEL_LAYOUT_OPTIONS,
            label_field_options=[{"value": "logo", "label": "Logo (wrap only)"}],
            selected_fields=("logo", "barcode", "name", "set", "condition", "location"),
            ids="70,71",
            status="",
        )
        html = html.replace("/static/degen-logo-label.png", logo_uri)
        html = re.sub(r'href="/static/linear\.css\?v=[^"]+"', f'href="{linear_uri}"', html)
        html = re.sub(r'href="/static/safe-area\.css\?v=[^"]+"', f'href="{safe_area_uri}"', html)
        html_path = tmp_path / f"{layout}.html"
        html_path.write_text(html, encoding="utf-8")
        return html_path

    try:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
    except PlaywrightError as exc:
        pytest.skip(f"playwright chromium is not available: {exc}")

    try:
        page = browser.new_page(viewport={"width": 1100, "height": 700}, device_scale_factor=2)
        page.emulate_media(media="print")
        failures: list[str] = []
        for layout in ("wrap", "wrap-3x1"):
            page.goto(render_html(layout).resolve().as_uri(), wait_until="networkidle")
            rects = page.evaluate(
                """() => Array.from(document.querySelectorAll('.wrap-label-card')).map((card) => {
                    const q = (sel) => {
                        const el = sel === ':scope' ? card : card.querySelector(sel);
                        if (!el) return null;
                        const r = el.getBoundingClientRect();
                        return {top:r.top, right:r.right, bottom:r.bottom, left:r.left, width:r.width, height:r.height};
                    };
                    return {
                        price: card.querySelector('.wrap-price')?.textContent.trim(),
                        bodyBg: getComputedStyle(document.body).backgroundColor,
                        card: q(':scope'),
                        customer: q('.wrap-customer'),
                        logo: q('.wrap-logo'),
                        priceBox: q('.wrap-price'),
                    };
                })"""
            )
            for rect in rects:
                label = f"{layout} {rect['price']}"
                customer = rect["customer"]
                logo = rect["logo"]
                price = rect["priceBox"]
                card = rect["card"]
                eps = 1.0
                if rect["bodyBg"] != "rgb(255, 255, 255)":
                    failures.append(f"{label}: print background is {rect['bodyBg']}")
                if logo["top"] < customer["top"] - eps or logo["bottom"] > customer["bottom"] + eps:
                    failures.append(f"{label}: logo outside customer cell")
                if price["top"] < customer["top"] - eps or price["bottom"] > customer["bottom"] + eps:
                    failures.append(f"{label}: price outside customer cell")
                if logo["bottom"] > price["top"] + eps:
                    failures.append(f"{label}: logo overlaps price")
                if customer["top"] < card["top"] - eps or customer["bottom"] > card["bottom"] + eps:
                    failures.append(f"{label}: customer cell outside card")
                min_logo_height = 54 if layout == "wrap" else 48
                if logo["height"] < min_logo_height:
                    failures.append(f"{label}: logo rendered too small")

        assert failures == []
    finally:
        browser.close()
        pw.stop()

def test_wraparound_template_hides_customer_logo_when_not_selected():
    item = InventoryItem(
        id=63,
        barcode="DGN-000063",
        item_type="single",
        game="Pokemon",
        card_name="Squirtle",
        condition="NM",
        list_price=8,
    )
    label = label_context_for_items([item], selected_fields=("barcode", "name"))[0]

    html = templates.env.get_template("inventory_labels.html").render(
        request=_FakeRequest(),
        current_user=_FakeUser(),
        csrf_token="",
        labels=[label],
        layout="wrap",
        label_layout_options=LABEL_LAYOUT_OPTIONS,
        label_field_options=[{"value": "logo", "label": "Logo"}],
        selected_fields=("barcode", "name"),
        ids="63",
        status="",
    )

    assert 'class="wrap-logo"' not in html
    assert 'class="wrap-customer no-logo"' in html


def test_wraparound_template_uses_empty_fold_divider_and_fit_class():
    item = InventoryItem(
        id=60,
        barcode="DGN-000060",
        item_type="single",
        game="Pokemon",
        card_name="Charizard ex",
        set_name="151",
        condition="NM",
        location="Ungrouped",
        list_price=10.84,
    )
    label = label_context_for_items([item])[0]

    html = templates.env.get_template("inventory_labels.html").render(
        request=_FakeRequest(),
        current_user=_FakeUser(),
        csrf_token="",
        labels=[label],
        layout="wrap",
        label_layout_options=[{"value": "wrap", "label": "Wraparound"}],
        label_field_options=[{"value": "barcode", "label": "Barcode"}],
        selected_fields=("barcode", "name", "set", "condition", "location"),
        ids="60",
        status="",
    )

    assert 'class="wrap-price price-short"' in html
    assert '<div class="wrap-fold" aria-hidden="true"></div>' in html
    assert "<span>Fold</span>" not in html
