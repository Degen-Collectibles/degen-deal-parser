"""
Barcode generation for inventory items.

Generates Code 128 barcodes as SVG strings using python-barcode.
The barcode value for each item is its DGN-XXXXXX code, which USB
barcode scanners send as keyboard input followed by Enter.
"""
from __future__ import annotations

import io
from typing import TYPE_CHECKING

try:
    import barcode
    from barcode.writer import SVGWriter
    _BARCODE_AVAILABLE = True
except ImportError:
    _BARCODE_AVAILABLE = False

if TYPE_CHECKING:
    from ..models import InventoryItem


LABEL_LAYOUT_OPTIONS = [
    {"value": "wrap", "label": "Wraparound"},
    {"value": "sheet", "label": "Sheet Labels"},
    {"value": "thermal", "label": '2.25" Thermal'},
]

LABEL_FIELD_OPTIONS = [
    {"value": "barcode", "label": "Barcode"},
    {"value": "name", "label": "Product name"},
    {"value": "set", "label": "Set"},
    {"value": "card_number", "label": "Card #"},
    {"value": "variant", "label": "Variant"},
    {"value": "type", "label": "Type"},
    {"value": "condition", "label": "Condition"},
    {"value": "grade", "label": "Grade"},
    {"value": "cert", "label": "Cert #"},
    {"value": "location", "label": "Location"},
    {"value": "game", "label": "Game"},
]
DEFAULT_LABEL_FIELDS = ("barcode", "name", "set", "condition", "location")
_LABEL_FIELD_LABELS = {option["value"]: option["label"] for option in LABEL_FIELD_OPTIONS}
_LABEL_FIELD_LABELS["name"] = "Item"
_VALID_LABEL_FIELDS = tuple(option["value"] for option in LABEL_FIELD_OPTIONS)


def _money(value: float | None) -> str:
    if value is None:
        return ""
    return f"${value:,.2f}"


def _label_price(item: "InventoryItem") -> tuple[str, str]:
    """Return the customer-facing price text plus the source used."""
    if item.list_price is not None:
        return _money(round(item.list_price, 2)), "Manual price"
    if item.auto_price is not None:
        return _money(round(item.auto_price, 2)), "Market price"
    return "Price not set", ""


def _label_product_type(item: "InventoryItem") -> str:
    item_type = (item.item_type or "").strip().lower()
    if item_type == "sealed":
        return item.sealed_product_kind or "Sealed"
    if item_type == "slab":
        return "Graded card"
    if item_type == "single":
        return "Single"
    return item.item_type or "Inventory"


def _label_price_class(price_text: str) -> str:
    length = len((price_text or "").strip())
    if length <= 6:
        return "price-short"
    if length <= 9:
        return "price-medium"
    return "price-long"


def parse_label_fields(raw_fields: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    """Return valid employee-facing label fields in display order."""
    selected: list[str] = []
    for raw in raw_fields or []:
        for part in str(raw).split(","):
            field = part.strip().lower()
            if field in _VALID_LABEL_FIELDS and field not in selected:
                selected.append(field)
    return tuple(selected) or DEFAULT_LABEL_FIELDS


def _label_grade(item: "InventoryItem") -> str:
    parts = [part for part in [item.grading_company, item.grade] if part]
    return " ".join(parts)


def _label_field_value(
    item: "InventoryItem",
    field: str,
    *,
    grade_or_condition: str,
    product_type: str,
) -> str:
    if field == "barcode":
        return item.barcode or ""
    if field == "name":
        return item.card_name or ""
    if field == "set":
        return item.set_name or ""
    if field == "card_number":
        return f"#{item.card_number}" if item.card_number else ""
    if field == "variant":
        return item.variant or ""
    if field == "type":
        return product_type
    if field == "condition":
        return grade_or_condition
    if field == "grade":
        return _label_grade(item)
    if field == "cert":
        return f"Cert {item.cert_number}" if item.cert_number else ""
    if field == "location":
        return item.location or ""
    if field == "game":
        return item.game or ""
    return ""


def generate_barcode_value(item_id: int) -> str:
    """Return the canonical barcode string for an item, e.g. 'DGN-000042'."""
    return f"DGN-{item_id:06d}"


def render_barcode_svg(barcode_value: str) -> str:
    """
    Render a Code 128 barcode as an SVG string.

    Returns an SVG string ready to embed in HTML or serve as a response.
    Falls back to a minimal placeholder SVG when python-barcode is not installed.
    """
    if not _BARCODE_AVAILABLE:
        return _fallback_svg(barcode_value)

    Code128 = barcode.get_barcode_class("code128")
    buf = io.BytesIO()
    writer = SVGWriter()
    code = Code128(barcode_value, writer=writer)
    # write() returns SVG bytes; write_options control size
    code.write(
        buf,
        options={
            "module_height": 10.0,   # mm, controls bar height
            "module_width": 0.2,     # mm, controls bar width
            "font_size": 6,
            "text_distance": 3,
            "quiet_zone": 4.0,
        },
    )
    return buf.getvalue().decode("utf-8")


def _fallback_svg(barcode_value: str) -> str:
    """Minimal placeholder SVG returned when python-barcode is unavailable."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="60">'
        f'<rect width="200" height="60" fill="#f5f5f5" stroke="#ccc"/>'
        f'<text x="100" y="35" text-anchor="middle" font-size="11" font-family="monospace">{barcode_value}</text>'
        "</svg>"
    )


def label_context_for_items(items: list, selected_fields: list[str] | tuple[str, ...] | None = None) -> list[dict]:
    """
    Build a list of label context dicts for use in the print-labels template.
    Each dict contains barcode_value, barcode_svg, and display fields.
    """
    labels = []
    parsed_fields = parse_label_fields(selected_fields)
    for item in items:
        barcode_value = item.barcode
        svg = render_barcode_svg(barcode_value)
        grade_or_condition = (
            f"{item.grading_company} {item.grade}" if item.grading_company and item.grade
            else item.condition or ""
        )
        price_text, price_source = _label_price(item)
        product_type = _label_product_type(item)
        employee_lines = []
        for field in parsed_fields:
            value = _label_field_value(
                item,
                field,
                grade_or_condition=grade_or_condition,
                product_type=product_type,
            )
            if value:
                employee_lines.append({
                    "field": field,
                    "label": _LABEL_FIELD_LABELS[field],
                    "value": value,
                })
        labels.append({
            "item": item,
            "item_title": item.card_name or item.barcode,
            "barcode_value": barcode_value,
            "barcode_svg": svg,
            "grade_or_condition": grade_or_condition,
            "product_type": product_type,
            "price_text": price_text,
            "price_source": price_source,
            "price_class": _label_price_class(price_text),
            "selected_fields": parsed_fields,
            "employee_lines": employee_lines,
        })
    return labels
