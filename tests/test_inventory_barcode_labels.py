from app.inventory.barcode import (
    _BARCODE_AVAILABLE,
    DEFAULT_LABEL_FIELDS,
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
