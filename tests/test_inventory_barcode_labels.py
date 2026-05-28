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

    compact_label, wide_label = label_context_for_items([compact, wide])

    assert compact_label["price_class"] == "price-short"
    assert wide_label["price_class"] == "price-medium"


def test_parse_label_fields_defaults_and_filters_invalid_values():
    assert parse_label_fields([]) == DEFAULT_LABEL_FIELDS
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
    assert 'src="/static/degen-logo.png"' in customer_section
    assert customer_section.index('class="wrap-logo"') < customer_section.index('class="wrap-price price-short"')
    assert "Logo and price." in html


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
