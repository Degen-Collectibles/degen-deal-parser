from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import re
from typing import Any, Optional

import httpx

from .inventory.shopify import shopify_graphql_request


CDTFA_CITY_RATES_URL = "https://cdtfa.ca.gov/taxes-and-fees/rates.aspx"

NON_TAXABLE_PHYSICAL_VARIANTS_QUERY = """
query NonTaxablePhysicalVariants($cursor: String) {
  productVariants(first: 100, after: $cursor, query: "taxable:false") {
    pageInfo { hasNextPage endCursor }
    nodes {
      id title sku taxable
      inventoryItem { requiresShipping }
      product { id title status isGiftCard }
    }
  }
}
"""

_EFFECTIVE_DATE_PATTERN = re.compile(
    r"\beffective(?:\s+date)?(?:\s+as\s+of)?\s*:?[\s(]*"
    r"((?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{1,2},\s+\d{4})\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class OfficialTaxRate:
    city: str
    county: str
    rate: Decimal
    effective_label: str


@dataclass(frozen=True)
class ShopifyVariantTaxState:
    product_id: str
    product_title: str
    variant_id: str
    variant_title: str
    sku: str


class TableRowTextParser(HTMLParser):
    """Collect table rows by cell boundaries while retaining page text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self.text_parts: list[str] = []
        self._row: Optional[list[str]] = None
        self._cell_tag: Optional[str] = None
        self._cell_parts: list[str] = []

    @property
    def page_text(self) -> str:
        return _normalize_text(" ".join(self.text_parts))

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, Optional[str]]]
    ) -> None:
        del attrs
        tag = tag.lower()
        if tag == "tr":
            self._row = []
            self._cell_tag = None
            self._cell_parts = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell_tag = tag
            self._cell_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._cell_tag == tag and self._row is not None:
            self._row.append(_normalize_text(" ".join(self._cell_parts)))
            self._cell_tag = None
            self._cell_parts = []
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None
            self._cell_tag = None
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if data:
            self.text_parts.append(data)
            if self._cell_tag is not None:
                self._cell_parts.append(data)


def _normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def parse_cdtfa_city_rate_html(
    html: str,
    *,
    city: str,
    county: str,
) -> OfficialTaxRate:
    parser = TableRowTextParser()
    parser.feed(html)
    parser.close()

    header_indexes: Optional[dict[str, int]] = None
    rate_text: Optional[str] = None
    for row in parser.rows:
        normalized_headers = [cell.casefold() for cell in row]
        if {"location", "rate", "county"}.issubset(normalized_headers):
            header_indexes = {
                "location": normalized_headers.index("location"),
                "rate": normalized_headers.index("rate"),
                "county": normalized_headers.index("county"),
            }
            continue
        if header_indexes is None:
            continue
        largest_index = max(header_indexes.values())
        if len(row) <= largest_index:
            continue
        if (
            row[header_indexes["location"]] == city
            and row[header_indexes["county"]] == county
        ):
            rate_text = row[header_indexes["rate"]]
            break

    if rate_text is None:
        raise ValueError(f"CDTFA rate row not found for {city}, {county}")

    effective_match = _EFFECTIVE_DATE_PATTERN.search(parser.page_text)
    if effective_match is None:
        raise ValueError(f"CDTFA effective date not found for {city}")
    effective_label = effective_match.group(1)
    try:
        datetime.strptime(effective_label, "%B %d, %Y")
    except ValueError as exc:
        raise ValueError(
            f"Invalid CDTFA effective date for {city}: {effective_label}"
        ) from exc

    normalized_rate = rate_text.strip()
    if not normalized_rate.endswith("%"):
        raise ValueError(f"Invalid CDTFA rate for {city}: {rate_text}")
    try:
        percent = Decimal(normalized_rate[:-1].strip())
    except InvalidOperation as exc:
        raise ValueError(f"Invalid CDTFA rate for {city}: {rate_text}") from exc
    if not percent.is_finite() or percent < 0 or percent > 100:
        raise ValueError(f"Invalid CDTFA rate for {city}: {rate_text}")
    rate = percent / Decimal("100")

    return OfficialTaxRate(
        city=city,
        county=county,
        rate=rate,
        effective_label=effective_label,
    )


async def fetch_cdtfa_city_rate(
    *,
    city: str,
    county: str,
    client: Optional[httpx.AsyncClient] = None,
) -> OfficialTaxRate:
    async def _run(active_client: httpx.AsyncClient) -> OfficialTaxRate:
        response = await active_client.get(CDTFA_CITY_RATES_URL)
        response.raise_for_status()
        return parse_cdtfa_city_rate_html(
            response.text,
            city=city,
            county=county,
        )

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=20.0) as active_client:
        return await _run(active_client)


async def fetch_non_taxable_physical_variants(
    *,
    store_domain: str,
    access_token: str,
    client: Optional[httpx.AsyncClient] = None,
) -> list[ShopifyVariantTaxState]:
    if not store_domain or not access_token:
        raise ValueError("Shopify store domain and Admin token are required")

    async def _run(
        active_client: httpx.AsyncClient,
    ) -> list[ShopifyVariantTaxState]:
        variants: list[ShopifyVariantTaxState] = []
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()

        while True:
            payload = await shopify_graphql_request(
                active_client,
                store_domain=store_domain,
                access_token=access_token,
                query=NON_TAXABLE_PHYSICAL_VARIANTS_QUERY,
                variables={"cursor": cursor},
            )
            if not isinstance(payload, dict):
                raise ValueError("Shopify GraphQL response must be an object")
            errors = payload.get("errors")
            if errors:
                messages = []
                for error in errors if isinstance(errors, list) else [errors]:
                    if isinstance(error, dict):
                        messages.append(str(error.get("message") or error))
                    else:
                        messages.append(str(error))
                raise ValueError(
                    "Shopify GraphQL errors: " + "; ".join(messages)
                )

            data = payload.get("data")
            connection = data.get("productVariants") if isinstance(data, dict) else None
            if not isinstance(connection, dict):
                raise ValueError("Shopify GraphQL response missing productVariants")
            page_info = connection.get("pageInfo")
            if not isinstance(page_info, dict):
                raise ValueError("Shopify GraphQL response missing productVariants.pageInfo")
            nodes = connection.get("nodes")
            if not isinstance(nodes, list):
                raise ValueError("Shopify GraphQL response missing productVariants.nodes")

            for node in nodes:
                if not isinstance(node, dict):
                    raise ValueError("Shopify GraphQL productVariants node is malformed")
                variant_id = node.get("id")
                if not isinstance(variant_id, str) or not variant_id.strip():
                    raise ValueError("Shopify GraphQL productVariants node has invalid id")
                variant_title = node.get("title")
                if not isinstance(variant_title, str):
                    raise ValueError("Shopify GraphQL productVariants node has invalid title")
                if "sku" not in node:
                    raise ValueError("Shopify GraphQL productVariants node has invalid sku")
                sku_value = node["sku"]
                if sku_value is not None and not isinstance(sku_value, str):
                    raise ValueError("Shopify GraphQL productVariants node has invalid sku")
                sku = sku_value or ""
                taxable = node.get("taxable")
                if not isinstance(taxable, bool):
                    raise ValueError("Shopify GraphQL productVariants node has invalid taxable")
                inventory_item = node.get("inventoryItem")
                if not isinstance(inventory_item, dict):
                    raise ValueError(
                        "Shopify GraphQL productVariants node has invalid inventoryItem"
                    )
                requires_shipping = inventory_item.get("requiresShipping")
                if not isinstance(requires_shipping, bool):
                    raise ValueError(
                        "Shopify GraphQL productVariants node has invalid requiresShipping"
                    )
                product = node.get("product")
                if not isinstance(product, dict):
                    raise ValueError(
                        "Shopify GraphQL productVariants node has invalid product"
                    )
                product_id = product.get("id")
                if not isinstance(product_id, str) or not product_id.strip():
                    raise ValueError(
                        "Shopify GraphQL productVariants node has invalid product id"
                    )
                product_title = product.get("title")
                if not isinstance(product_title, str) or not product_title.strip():
                    raise ValueError(
                        "Shopify GraphQL productVariants node has invalid product title"
                    )
                product_status = product.get("status")
                if not isinstance(product_status, str):
                    raise ValueError(
                        "Shopify GraphQL productVariants node has invalid product status"
                    )
                is_gift_card = product.get("isGiftCard")
                if not isinstance(is_gift_card, bool):
                    raise ValueError(
                        "Shopify GraphQL productVariants node has invalid isGiftCard"
                    )

                if taxable is not False:
                    continue
                if requires_shipping is not True:
                    continue
                if product_status != "ACTIVE":
                    continue
                if is_gift_card is not False:
                    continue
                variants.append(
                    ShopifyVariantTaxState(
                        product_id=product_id,
                        product_title=product_title,
                        variant_id=variant_id,
                        variant_title=variant_title,
                        sku=sku,
                    )
                )

            has_next_page = page_info.get("hasNextPage")
            if not isinstance(has_next_page, bool):
                raise ValueError("Shopify GraphQL pagination has invalid hasNextPage")
            if not has_next_page:
                return variants

            next_cursor_value = page_info.get("endCursor")
            next_cursor = (
                next_cursor_value.strip()
                if isinstance(next_cursor_value, str)
                else ""
            )
            if not next_cursor or next_cursor in seen_cursors:
                raise ValueError("Shopify GraphQL pagination returned a blank or repeated cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=30.0) as active_client:
        return await _run(active_client)
