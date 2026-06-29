# Shopify POS Tax Sentinel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only sentinel that verifies Degen's San Jose Shopify POS tax configuration, physical-variant taxability, actual POS tax lines, and POS-only operating boundary without changing Shopify.

**Architecture:** Add a focused source-adapter module for CDTFA and Shopify reads, then a coordinator module that evaluates those observations and records deduplicated `ShopifySyncIssue` findings. Reuse the existing Shopify Admin credentials, stored raw order payloads, issue queue, managed database sessions, and supervised background-task pattern. Keep all external operations read-only and require a separate approval-gated production activation after local verification.

**Tech Stack:** Python 3.14, FastAPI lifespan tasks, SQLModel, httpx, Shopify Admin GraphQL/REST 2026-04, standard-library `html.parser`, pytest/unittest, existing `ShopifySyncIssue` UI.

---

## File Map

- Create `app/shopify_tax_sources.py`: read-only CDTFA and Shopify source adapters plus structured source result types.
- Create `app/shopify_tax_sentinel.py`: pure order evaluation, audit coordination, finding persistence, scheduling, and structured logs.
- Modify `app/inventory/shopify.py`: expose the existing GraphQL request helper safely and return full Shopify location rows without duplicating authentication code.
- Modify `app/shopify_sync.py`: add tax-sentinel issue constants and a complete-check-only auto-resolution helper.
- Modify `app/config.py`: add explicit disabled-by-default sentinel policy fields.
- Modify `app/main.py`: start the sentinel only when enabled and Shopify Admin credentials are configured.
- Create `tests/test_shopify_tax_sentinel.py`: source parsing, pagination, order evaluation, issue lifecycle, coordinator, and scheduler/config tests.
- Create `docs/ops/shopify-pos-tax-sentinel-runbook.md`: operator response, verification, rollback, and shipping launch gate.

Do not add a new database table, route, template, Shopify scope, browser automation path, or Shopify mutation.

### Task 1: Issue Lifecycle and Read-Only Source Adapters

**Files:**
- Modify: `app/shopify_sync.py:20-175`
- Modify: `app/inventory/shopify.py:104-129,409-434`
- Create: `app/shopify_tax_sources.py`
- Create: `tests/test_shopify_tax_sentinel.py`

- [ ] **Step 1: Write failing issue-lifecycle tests**

Create `tests/test_shopify_tax_sentinel.py` with an isolated SQLite database and these tests:

```python
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase

from sqlmodel import SQLModel, Session, create_engine, select

from app.models import ShopifySyncIssue
from app.shopify_sync import (
    SHOPIFY_SYNC_ISSUE_OPEN,
    SHOPIFY_SYNC_ISSUE_RESOLVED,
    SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
    record_shopify_sync_issue,
    resolve_unobserved_shopify_sync_issues,
)


class ShopifyTaxIssueLifecycleTests(TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def test_complete_check_resolves_only_unobserved_open_issues(self) -> None:
        with Session(self.engine) as session:
            keep = record_shopify_sync_issue(
                session,
                issue_type=SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
                shopify_variant_id="gid://shopify/ProductVariant/1",
                message="Variant is not taxable.",
            )
            close = record_shopify_sync_issue(
                session,
                issue_type=SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
                shopify_variant_id="gid://shopify/ProductVariant/2",
                message="Variant is not taxable.",
            )
            session.commit()

            resolved = resolve_unobserved_shopify_sync_issues(
                session,
                issue_type=SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
                observed_issue_keys={keep.issue_key},
                resolution_note="Complete catalog check no longer observed this condition.",
            )
            session.commit()

            rows = session.exec(select(ShopifySyncIssue)).all()
            by_key = {row.issue_key: row for row in rows}
            assert resolved == 1
            assert by_key[keep.issue_key].status == SHOPIFY_SYNC_ISSUE_OPEN
            assert by_key[close.issue_key].status == SHOPIFY_SYNC_ISSUE_RESOLVED
            assert by_key[close.issue_key].resolved_at is not None
```

- [ ] **Step 2: Run the issue test and confirm the expected import failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py::ShopifyTaxIssueLifecycleTests -q
```

Expected: FAIL because the tax issue constant and resolver do not exist.

- [ ] **Step 3: Add issue constants and complete-check resolution**

Add these constants to `app/shopify_sync.py`:

```python
SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED = "taxable_variant_disabled"
SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH = "pos_tax_rate_mismatch"
SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING = "pos_tax_lines_missing"
SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH = "pos_location_mismatch"
SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE = "pos_tax_override_observed"
SHOPIFY_TAX_ISSUE_NON_POS_ORDER = "non_pos_order_detected"
SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED = "official_tax_rate_changed"
SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE = "official_tax_source_unavailable"

SHOPIFY_TAX_SENTINEL_ISSUE_TYPES = frozenset(
    {
        SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
        SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH,
        SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
        SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
        SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE,
        SHOPIFY_TAX_ISSUE_NON_POS_ORDER,
        SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED,
        SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE,
    }
)
```

Add the resolver below `record_shopify_sync_issue`:

```python
def resolve_unobserved_shopify_sync_issues(
    session: Session,
    *,
    issue_type: str,
    observed_issue_keys: set[str],
    resolution_note: str,
) -> int:
    rows = session.exec(
        select(ShopifySyncIssue).where(
            ShopifySyncIssue.issue_type == issue_type,
            ShopifySyncIssue.status == SHOPIFY_SYNC_ISSUE_OPEN,
        )
    ).all()
    now = utcnow()
    resolved = 0
    for issue in rows:
        if issue.issue_key in observed_issue_keys:
            continue
        issue.status = SHOPIFY_SYNC_ISSUE_RESOLVED
        issue.resolution_note = resolution_note
        issue.resolved_by = "shopify_pos_tax_sentinel"
        issue.resolved_at = now
        issue.last_seen_at = now
        session.add(issue)
        resolved += 1
    return resolved
```

- [ ] **Step 4: Write failing CDTFA and Shopify source tests**

Append source tests that use a real `httpx.Response` through `httpx.MockTransport` or a small fake client. The assertions must cover structured HTML parsing, an absent city, variant pagination, gift-card exclusion, physical-product filtering, and location rows:

```python
from app.shopify_tax_sources import (
    OfficialTaxRate,
    fetch_non_taxable_physical_variants,
    parse_cdtfa_city_rate_html,
)


CDTFA_HTML = """
<html><body>
<h1>California City &amp; County Sales &amp; Use Tax Rates (effective April 1, 2026)</h1>
<table><tbody>
<tr><th>Location</th><th>Rate</th><th>County</th><th>Type</th></tr>
<tr><td>Oakland</td><td>10.750%</td><td>Alameda</td><td>City</td></tr>
<tr><td>San Jose</td><td>10.000%</td><td>Santa Clara</td><td>City</td></tr>
</tbody></table>
</body></html>
"""


class ShopifyTaxSourceTests(IsolatedAsyncioTestCase):
    def test_parse_cdtfa_city_rate_uses_structured_table_cells(self) -> None:
        result = parse_cdtfa_city_rate_html(CDTFA_HTML, city="San Jose", county="Santa Clara")
        assert result == OfficialTaxRate(
            city="San Jose",
            county="Santa Clara",
            rate=Decimal("0.10000"),
            effective_label="April 1, 2026",
        )

    def test_parse_cdtfa_city_rate_rejects_missing_city(self) -> None:
        with self.assertRaisesRegex(ValueError, "San Jose"):
            parse_cdtfa_city_rate_html(CDTFA_HTML, city="San Jose", county="Alameda")
```

For pagination, mock two GraphQL payloads. Page one includes one active physical non-taxable variant and a gift card; page two includes a non-shipping digital item. Assert that only the physical non-gift-card variant is returned and both cursors were requested.

- [ ] **Step 5: Run source tests and confirm they fail**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py::ShopifyTaxSourceTests -q
```

Expected: FAIL because `app.shopify_tax_sources` does not exist.

- [ ] **Step 6: Expose read-only Shopify helpers**

In `app/inventory/shopify.py`, rename `_graphql_post` to `shopify_graphql_request` and update all internal call sites. Keep the arguments and behavior unchanged.

Add a full-row location reader and make the existing primary-location helper use it:

```python
async def get_shopify_locations(
    *,
    store_domain: str,
    access_token: str,
    client: Optional[httpx.AsyncClient] = None,
) -> list[dict[str, Any]]:
    if not store_domain or not access_token:
        return []
    url = f"{_shopify_base(store_domain)}/locations.json"

    async def _run(active_client: httpx.AsyncClient) -> list[dict[str, Any]]:
        response = await active_client.get(url, headers=_shopify_headers(access_token))
        response.raise_for_status()
        rows = response.json().get("locations") or []
        return [row for row in rows if isinstance(row, dict)]

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=20.0) as active_client:
        return await _run(active_client)
```

`get_shopify_primary_location_id` must select from `await get_shopify_locations(...)` and retain its current active-first behavior.

- [ ] **Step 7: Implement structured source adapters**

Create `app/shopify_tax_sources.py` with immutable result types:

```python
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from html.parser import HTMLParser
import re
from typing import Any, Optional

import httpx

from .inventory.shopify import shopify_graphql_request

CDTFA_CITY_RATES_URL = "https://cdtfa.ca.gov/taxes-and-fees/rates.aspx"


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


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: list[str] = []
        self._cell: list[str] | None = None
        self.text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"}:
            self._cell = []

    def handle_data(self, data: str) -> None:
        self.text.append(data)
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row:
            self.rows.append(self._row)
            self._row = []


def parse_cdtfa_city_rate_html(html: str, *, city: str, county: str) -> OfficialTaxRate:
    parser = _TableParser()
    parser.feed(html)
    normalized_text = " ".join(" ".join(parser.text).split())
    effective_match = re.search(r"effective\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", normalized_text, re.I)
    effective_label = effective_match.group(1) if effective_match else "unknown"
    for row in parser.rows:
        if len(row) < 3 or row[0].casefold() != city.casefold() or row[2].casefold() != county.casefold():
            continue
        percent = Decimal(row[1].replace("%", "").strip())
        return OfficialTaxRate(city=city, county=county, rate=percent / Decimal("100"), effective_label=effective_label)
    raise ValueError(f"CDTFA rate row not found for {city}, {county}")
```

Add `fetch_cdtfa_city_rate` using `httpx.AsyncClient`, `raise_for_status()`, and the parser. Add `fetch_non_taxable_physical_variants` with this GraphQL query and a complete cursor loop:

```graphql
query NonTaxablePhysicalVariants($cursor: String) {
  productVariants(first: 100, after: $cursor, query: "taxable:false") {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      sku
      taxable
      inventoryItem { requiresShipping }
      product { id title status isGiftCard }
    }
  }
}
```

Return only nodes where `taxable` is false, `inventoryItem.requiresShipping` is true, `product.status == "ACTIVE"`, and `product.isGiftCard` is false. Raise on GraphQL errors or malformed pagination so a partial catalog never appears complete.

- [ ] **Step 8: Run focused tests, compile, full suite, and commit**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py tests\test_inventory_shopify_sync.py -q
.\.venv\Scripts\python.exe -m compileall app
.\.venv\Scripts\python.exe -m pytest --tb=short -q
git diff --check
```

Expected: all commands PASS.

Commit only Task 1 files:

```powershell
git add -- app/shopify_sync.py app/inventory/shopify.py app/shopify_tax_sources.py tests/test_shopify_tax_sentinel.py
git diff --cached --stat
git commit -m "Add Shopify tax sentinel source checks"
```

### Task 2: Pure POS Order Evaluation

**Files:**
- Create: `app/shopify_tax_sentinel.py`
- Modify: `tests/test_shopify_tax_sentinel.py`

- [ ] **Step 1: Write the order-evaluation matrix first**

Append tests that construct complete Shopify payload dictionaries and call `evaluate_shopify_order`. Use this baseline helper:

```python
def paid_pos_order(**overrides):
    payload = {
        "id": 101,
        "name": "#101",
        "source_name": "pos",
        "location_id": 555,
        "financial_status": "paid",
        "total_tax": "10.00",
        "tax_lines": [{"title": "State Tax", "rate": 0.10, "price": "10.00"}],
        "line_items": [
            {
                "id": 1,
                "title": "Pokemon Booster Box",
                "sku": "PKM-BOX",
                "taxable": True,
                "tax_lines": [{"title": "State Tax", "rate": 0.10, "price": "10.00"}],
            }
        ],
    }
    payload.update(overrides)
    return payload
```

Required tests:

```python
from app.shopify_tax_sentinel import evaluate_shopify_order


def test_expected_pos_order_has_no_findings():
    assert evaluate_shopify_order(
        paid_pos_order(), expected_location_id="555", expected_rate=Decimal("0.10"), pos_only=True
    ) == []


def test_non_pos_order_is_critical():
    findings = evaluate_shopify_order(
        paid_pos_order(source_name="web"), expected_location_id="555", expected_rate=Decimal("0.10"), pos_only=True
    )
    assert [(row.issue_type, row.severity) for row in findings] == [("non_pos_order_detected", "critical")]


def test_wrong_location_is_critical():
    findings = evaluate_shopify_order(
        paid_pos_order(location_id=777), expected_location_id="555", expected_rate=Decimal("0.10"), pos_only=True
    )
    assert findings[0].issue_type == "pos_location_mismatch"


def test_zero_tax_on_taxable_items_is_override_observation():
    findings = evaluate_shopify_order(
        paid_pos_order(total_tax="0.00", tax_lines=[], line_items=[{"id": 1, "title": "Box", "sku": "BOX", "taxable": True, "tax_lines": []}]),
        expected_location_id="555",
        expected_rate=Decimal("0.10"),
        pos_only=True,
    )
    assert [row.issue_type for row in findings] == ["pos_tax_override_observed"]
```

Also test an 8.25 percent rate mismatch, a missing-tax-lines payload with positive `total_tax`, a refunded order that is skipped, string and integer location IDs, multiple tax lines whose rates sum to 10 percent, and malformed `raw_payload` JSON.

- [ ] **Step 2: Run evaluation tests and confirm import failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py -k "order or override or location" -q
```

Expected: FAIL because `app.shopify_tax_sentinel` does not exist.

- [ ] **Step 3: Implement immutable findings and deterministic evaluation**

Create `app/shopify_tax_sentinel.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .shopify_sync import (
    SHOPIFY_TAX_ISSUE_NON_POS_ORDER,
    SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
    SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
    SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE,
    SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH,
)


@dataclass(frozen=True)
class TaxFinding:
    issue_type: str
    severity: str
    message: str
    order_id: str
    order_number: str
    location_id: str
    payload: dict[str, Any]


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _taxable_lines(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("line_items") or []
    return [row for row in rows if isinstance(row, dict) and row.get("taxable") is True]


def evaluate_shopify_order(
    payload: dict[str, Any],
    *,
    expected_location_id: str,
    expected_rate: Decimal,
    pos_only: bool,
) -> list[TaxFinding]:
    if str(payload.get("financial_status") or "").casefold() != "paid":
        return []
    order_id = str(payload.get("id") or "")
    order_number = str(payload.get("name") or payload.get("order_number") or order_id)
    location_id = str(payload.get("location_id") or "")
    source_name = str(payload.get("source_name") or "").casefold()
    evidence = {
        "source_name": source_name,
        "location_id": location_id,
        "total_tax": payload.get("total_tax"),
        "tax_lines": payload.get("tax_lines") or [],
    }
    if pos_only and source_name != "pos":
        return [TaxFinding(SHOPIFY_TAX_ISSUE_NON_POS_ORDER, "critical", "A non-POS Shopify order was detected while POS-only mode is enabled.", order_id, order_number, location_id, evidence)]
    findings: list[TaxFinding] = []
    if source_name == "pos" and location_id != str(expected_location_id):
        findings.append(TaxFinding(SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH, "critical", f"POS order location {location_id or 'missing'} does not match expected location {expected_location_id}.", order_id, order_number, location_id, evidence))
    taxable_lines = _taxable_lines(payload)
    tax_lines = [row for row in (payload.get("tax_lines") or []) if isinstance(row, dict)]
    total_tax = _decimal(payload.get("total_tax"))
    if taxable_lines and total_tax == 0 and not tax_lines:
        findings.append(TaxFinding(SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE, "warning", "A paid POS order contains taxable items but charged zero tax.", order_id, order_number, location_id, evidence))
        return findings
    if total_tax > 0 and not tax_lines:
        findings.append(TaxFinding(SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING, "warning", "A paid POS order charged tax but has no order tax lines to verify.", order_id, order_number, location_id, evidence))
        return findings
    observed_rate = sum((_decimal(row.get("rate")) for row in tax_lines), Decimal("0"))
    if tax_lines and abs(observed_rate - expected_rate) > Decimal("0.0001"):
        findings.append(TaxFinding(SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH, "critical", f"POS order tax rate {observed_rate} does not match expected rate {expected_rate}.", order_id, order_number, location_id, {**evidence, "observed_rate": str(observed_rate), "expected_rate": str(expected_rate)}))
    return findings
```

Add a `parse_order_payload(raw_payload: str) -> dict[str, Any]` helper using `json.loads`; raise `ValueError("Shopify order raw payload is not a JSON object")` for malformed JSON or non-object values so the coordinator can record a visible parse finding.

- [ ] **Step 4: Run focused tests, compile, full suite, and commit**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py -q
.\.venv\Scripts\python.exe -m compileall app
.\.venv\Scripts\python.exe -m pytest --tb=short -q
git diff --check
```

Expected: all commands PASS.

Commit only Task 2 files:

```powershell
git add -- app/shopify_tax_sentinel.py tests/test_shopify_tax_sentinel.py
git diff --cached --stat
git commit -m "Evaluate Shopify POS tax evidence"
```

### Task 3: Audit Coordinator and Existing Issue Queue Integration

**Files:**
- Modify: `app/shopify_tax_sentinel.py`
- Modify: `tests/test_shopify_tax_sentinel.py`

- [ ] **Step 1: Write coordinator tests against isolated SQLModel state**

Add test fixtures that insert `ShopifyOrder` rows with `raw_payload` from `paid_pos_order()`. Patch `fetch_cdtfa_city_rate`, `fetch_non_taxable_physical_variants`, and `get_shopify_locations` with `AsyncMock`.

Required assertions:

- A normal official rate, fully taxable catalog, matching San Jose location, and normal POS order produce zero open tax-sentinel issues.
- A non-taxable physical variant creates `taxable_variant_disabled` with product ID, variant ID, SKU, title, and raw evidence.
- A CDTFA observation of 10.25 percent against the configured 10 percent creates `official_tax_rate_changed` at critical severity.
- A CDTFA exception creates `official_tax_source_unavailable` and does not resolve a prior rate-change issue.
- A complete later catalog check resolves a prior variant issue that is no longer observed.
- A partial or failed catalog check resolves nothing.
- A non-POS order creates `non_pos_order_detected` tied to the Shopify order ID and number.
- `AppSetting` stores the last successful official check timestamp, rate, and effective label.

Call the coordinator with injected settings and engine so tests never read production configuration:

```python
summary = await run_shopify_tax_sentinel_once(
    settings_obj=SimpleNamespace(
        shopify_store_domain="degen-test.myshopify.com",
        shopify_access_token="token",
        shopify_api_key="",
        shopify_pos_location_id="555",
        shopify_pos_tax_city="San Jose",
        shopify_pos_tax_county="Santa Clara",
        shopify_pos_expected_tax_rate=0.10,
        shopify_pos_only=True,
        shopify_pos_tax_order_lookback_days=7,
    ),
    session_factory=self.session_factory,
    now=datetime(2026, 6, 28, tzinfo=timezone.utc),
    force_official_check=True,
)
assert summary.success is True
```

Define the test factory with the same context-manager contract as production:

```python
from contextlib import contextmanager


@contextmanager
def session_factory(self):
    with Session(self.engine) as session:
        yield session
```

- [ ] **Step 2: Run coordinator tests and confirm failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py -k "coordinator or complete or official" -q
```

Expected: FAIL because the coordinator and summary type do not exist.

- [ ] **Step 3: Implement the coordinator with per-check completion boundaries**

Add these types and keys to `app/shopify_tax_sentinel.py`:

```python
@dataclass(frozen=True)
class SentinelRunSummary:
    success: bool
    variants_checked: int
    orders_checked: int
    findings_recorded: int
    findings_resolved: int
    errors: tuple[str, ...]


OFFICIAL_CHECK_AT_KEY = "shopify_pos_tax_sentinel.official_checked_at"
OFFICIAL_RATE_KEY = "shopify_pos_tax_sentinel.official_rate"
OFFICIAL_EFFECTIVE_KEY = "shopify_pos_tax_sentinel.official_effective_label"
```

Implement `run_shopify_tax_sentinel_once` as an async function that:

1. Validates a non-empty expected POS location ID and a rate strictly between zero and one.
2. Resolves the configured location through `get_shopify_locations` and requires an active row whose ID matches and whose city/state normalize to San Jose/CA.
3. Fetches every non-taxable active physical variant with complete pagination.
4. Runs the official CDTFA check when forced or when `OFFICIAL_CHECK_AT_KEY` is missing or older than seven days.
5. Reads `ShopifyOrder` rows created inside the configured lookback window, parses raw payloads, and calls `evaluate_shopify_order`.
6. Records findings with `record_shopify_sync_issue`; variant findings use product/variant/SKU identifiers, while order findings use order ID/number/location.
7. Calls `resolve_unobserved_shopify_sync_issues` only for a check that completed successfully.
8. Writes official observation settings only after a successful official fetch and parse.
9. Commits once after all local issue and setting updates; rolls back on a database exception.
10. Returns `SentinelRunSummary` and prints one `structured_log_line` without credentials or customer PII.

Give `run_shopify_tax_sentinel_once` a `session_factory: Callable[[], ContextManager[Session]] = managed_session` keyword parameter and open database work with `with session_factory() as session:`. Tests pass the isolated factory shown above. Do not change the shared `managed_session` helper.

Record source failures with this exact evidence boundary:

```python
record_shopify_sync_issue(
    session,
    issue_type=SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE,
    message="The CDTFA San Jose tax rate could not be verified.",
    severity="critical",
    payload={"source": CDTFA_CITY_RATES_URL, "error": str(exc)},
)
```

Do not place the access token, store headers, customer name, email, street address, or complete order payload in issue evidence.

- [ ] **Step 4: Run focused tests, compile, full suite, and commit**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py tests\test_shopify_ingest.py -q
.\.venv\Scripts\python.exe -m compileall app
.\.venv\Scripts\python.exe -m pytest --tb=short -q
git diff --check
```

Expected: all commands PASS.

Commit only Task 3 files:

```powershell
git add -- app/shopify_tax_sentinel.py tests/test_shopify_tax_sentinel.py
git diff --cached --stat
git commit -m "Persist Shopify POS tax sentinel findings"
```

### Task 4: Disabled-by-Default Scheduling, Runbook, and Final Verification

**Files:**
- Modify: `app/config.py:272-288`
- Modify: `app/main.py:160,485-498`
- Modify: `app/shopify_tax_sentinel.py`
- Modify: `tests/test_shopify_tax_sentinel.py`
- Create: `docs/ops/shopify-pos-tax-sentinel-runbook.md`

- [ ] **Step 1: Write configuration and loop tests first**

Add tests proving:

```python
from app.config import Settings
from app.shopify_tax_sentinel import shopify_pos_tax_sentinel_configured


def test_sentinel_is_disabled_by_default():
    settings = Settings(_env_file=None)
    assert settings.shopify_pos_tax_sentinel_enabled is False
    assert shopify_pos_tax_sentinel_configured(settings) is False


def test_sentinel_requires_location_and_admin_credentials():
    settings = Settings(
        _env_file=None,
        SHOPIFY_POS_TAX_SENTINEL_ENABLED=True,
        SHOPIFY_POS_LOCATION_ID="555",
        SHOPIFY_STORE_DOMAIN="degen-test.myshopify.com",
        SHOPIFY_ACCESS_TOKEN="token",
    )
    assert shopify_pos_tax_sentinel_configured(settings) is True
```

Add an async loop test that patches `run_shopify_tax_sentinel_once`, supplies a set `threading.Event` after one call, and proves exactly one cycle runs without waiting 24 hours. Add a failure test proving a raised cycle exception is logged and does not escape the loop.

- [ ] **Step 2: Run scheduling tests and confirm failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py -k "disabled or requires_location or loop" -q
```

Expected: FAIL because the configuration fields and loop do not exist.

- [ ] **Step 3: Add explicit policy configuration**

Add to the Inventory section of `app/config.py`:

```python
shopify_pos_tax_sentinel_enabled: bool = Field(default=False, alias="SHOPIFY_POS_TAX_SENTINEL_ENABLED")
shopify_pos_location_id: str = Field(default="", alias="SHOPIFY_POS_LOCATION_ID")
shopify_pos_tax_city: str = Field(default="San Jose", alias="SHOPIFY_POS_TAX_CITY")
shopify_pos_tax_county: str = Field(default="Santa Clara", alias="SHOPIFY_POS_TAX_COUNTY")
shopify_pos_expected_tax_rate: float = Field(default=0.10, alias="SHOPIFY_POS_EXPECTED_TAX_RATE")
shopify_pos_only: bool = Field(default=True, alias="SHOPIFY_POS_ONLY")
shopify_pos_tax_order_lookback_days: int = Field(default=7, alias="SHOPIFY_POS_TAX_ORDER_LOOKBACK_DAYS")
```

Do not default the location ID from `SHOPIFY_LOCATION_ID`; inventory and POS location intent must remain separate.

- [ ] **Step 4: Add the supervised daily loop and lifespan wiring**

Add to `app/shopify_tax_sentinel.py`:

```python
SHOPIFY_POS_TAX_SENTINEL_INTERVAL_SECONDS = 24 * 60 * 60


def shopify_pos_tax_sentinel_configured(settings_obj: Any) -> bool:
    return bool(
        getattr(settings_obj, "shopify_pos_tax_sentinel_enabled", False)
        and str(getattr(settings_obj, "shopify_pos_location_id", "") or "").strip()
        and shopify_admin_configured(settings_obj)
    )


async def periodic_shopify_pos_tax_sentinel_loop(stop_event: Event) -> None:
    while not stop_event.is_set():
        try:
            await run_shopify_tax_sentinel_once(settings_obj=settings)
        except Exception as exc:
            print(structured_log_line(runtime=f"{settings.runtime_name}_shopify_tax", action="shopify.pos_tax_sentinel.failed", success=False, error=str(exc)))
        if stop_event.is_set():
            break
        await asyncio.sleep(SHOPIFY_POS_TAX_SENTINEL_INTERVAL_SECONDS)
```

In `app/main.py`, import the configured predicate and loop. Start it with `track_background_task` only when the predicate is true, use task name `shopify-pos-tax-sentinel`, append it to `background_tasks`, and otherwise set `app.state.shopify_pos_tax_sentinel_task = None`.

- [ ] **Step 5: Write the operations runbook**

Create `docs/ops/shopify-pos-tax-sentinel-runbook.md` with these exact sections:

```markdown
# Shopify POS Tax Sentinel Runbook

## Current Operating Boundary
- In-person Shopify POS only at the San Jose location.
- Expected rate: 10.000 percent, manually verified with CDTFA by exact store address.
- Every physical variant is taxable by default.
- Cashier tax changes are transaction exceptions, not catalog policy.

## Read-Only Preflight
1. Verify the exact store address in CDTFA's address lookup.
2. Verify `SHOPIFY_POS_LOCATION_ID` resolves to the active San Jose Shopify location.
3. Verify the Admin token can read products, inventory items, and locations without printing it.
4. Run one sentinel cycle with production writes disabled except local issue recording.
5. Review `/inventory/shopify-sync` and structured logs.

## Finding Response
Document the response for each of the eight tax issue types. Correct Shopify manually, preserve evidence, rerun a complete sentinel cycle, and resolve only after verification.

## Shipping Launch Gate
Do not enable online checkout, shipping, or local delivery under the POS-only Manual Tax configuration. Obtain tax-professional guidance, choose Shopify Tax or a qualified destination-tax provider, test San Jose and Oakland addresses, and receive explicit approval before changing this boundary.

## Rollback
Set `SHOPIFY_POS_TAX_SENTINEL_ENABLED=false`, deploy through the normal path, and verify the sentinel task is absent while Shopify ingestion and inventory sync remain healthy.
```

Expand `Finding Response` into a flat table with issue type, evidence to inspect, manual action, and verification. Do not include credentials, real customer PII, or instructions to edit production without approval.

- [ ] **Step 6: Run final implementation verification**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_shopify_tax_sentinel.py tests\test_inventory_shopify_sync.py tests\test_shopify_ingest.py tests\test_shopify_webhook.py -q
.\.venv\Scripts\python.exe -m compileall app
.\.venv\Scripts\python.exe -m pytest --tb=short -q
git diff --check
git status --short --branch
```

Expected: all tests PASS; compile succeeds; only Task 4 files are intended for staging; unrelated `docs/superpowers/plans/2026-06-19-degen-ops-bot-action-engine.md` and `output/` remain untracked and unstaged.

- [ ] **Step 7: Commit Task 4 without activating production**

```powershell
git add -- app/config.py app/main.py app/shopify_tax_sentinel.py tests/test_shopify_tax_sentinel.py docs/ops/shopify-pos-tax-sentinel-runbook.md
git diff --cached --stat
git diff --cached --check
git commit -m "Schedule read-only Shopify POS tax audits"
```

Expected: commit succeeds. Do not push, deploy, set production environment variables, restart services, edit Shopify, or complete a POS transaction in this task.

## Production Activation Preflight

Production activation is a separate externally visible action requiring Jeffrey's explicit `proceed` after implementation review. The preflight must state:

- Exact target: Green/Brev `openclaw-9902ae`, `/opt/degen/app`, normal `origin/main` deployment path.
- Change: enable the sentinel with the verified San Jose Shopify location ID and POS-only policy.
- Reversible effects: the background task and local issue creation stop when the flag is disabled.
- Irreversible effects: none expected because the sentinel performs no Shopify writes; audit history remains.
- Backup/rollback: retain the prior environment configuration, disable the flag, redeploy normally, and verify task absence.
- Post-action verification: authenticated status surfaces, structured logs, `/inventory/shopify-sync`, one read-only sentinel cycle, and no change to Shopify rates, variants, orders, or locations.

Do not perform production activation as part of implementation execution.
