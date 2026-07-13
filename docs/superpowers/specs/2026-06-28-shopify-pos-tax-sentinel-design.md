# Shopify POS Tax Sentinel PRD

Date: 2026-06-28
Status: Approved for implementation planning
Owner: Degen Collectibles operations

## Problem

Degen Collectibles stopped using paid Shopify Tax for United States sales and now uses Manual Tax. The current business workflow is in-person Shopify POS sales at the San Jose, California store. The current combined San Jose sales and use tax rate is 10.000 percent, effective April 1, 2026.

Manual Tax does not automatically maintain address-level district rates. A stale San Jose rate, a Shopify POS device assigned to the wrong location, a physical variant with tax disabled, or an unexpected online or shipped order can therefore cause Degen to undercharge or overcharge tax.

The immediate need is not an autonomous tax-setting agent. It is a read-only sentinel that proves the current POS assumptions still hold, makes failures visible, and stops the team from treating the POS-only configuration as safe for future shipped orders.

## Current State

- United States tax calculation is set to Manual Tax in Shopify.
- The California manual rate is set to 10 percent.
- Degen currently accepts only in-store Shopify POS orders.
- Every physical product should be taxable by default.
- Employees may use Shopify POS's per-cart or per-item tax control for a deliberate transaction-level exception.
- The Shopify admin currently warns that some product variants are not charging tax.
- Shopify POS applies tax based on the Shopify location assigned to the POS device.
- The California Department of Tax and Fee Administration (CDTFA) lists San Jose at 10.000 percent effective April 1, 2026.
- Oakland is currently 10.750 percent, illustrating why the San Jose POS rate cannot be reused for future destination-shipped orders.
- `app/inventory/shopify.py` already provides authenticated Shopify Admin REST and GraphQL helpers.
- `ShopifyOrder.raw_payload` retains the full Shopify order payload, including tax lines and source/location fields when Shopify sends them.
- `ShopifySyncIssue` and `/inventory/shopify-sync` already provide a visible, deduplicated Shopify issue queue.
- `app/main.py` already supervises the periodic Shopify inventory sync task.

## Success Criteria

The sentinel is successful when:

- A physical Shopify variant with tax disabled appears as an open Shopify sync issue within one completed sentinel cycle.
- A completed POS order from the expected San Jose location with the expected 10 percent tax rate passes without a finding.
- A POS order with an unexpected tax rate, missing tax lines, or an unexpected location creates a visible finding with the order number and evidence.
- A deliberate cashier tax override is visible for review without changing the product's default taxable state.
- A non-POS Shopify order creates a critical finding explaining that destination-aware tax calculation is required before shipped orders are accepted.
- A change to the official CDTFA San Jose rate creates a critical finding instead of silently changing Shopify.
- Failure to fetch or parse the official CDTFA source is visible and does not reuse stale data as if it were current.
- No sentinel path changes a Shopify tax rate, product, variant, order, location, sales channel, or production configuration.
- The feature can be disabled with one configuration flag without affecting ingestion, inventory sync, or POS checkout.

## Scope

In scope:

- A deterministic, read-only Shopify POS tax sentinel.
- A weekly official-rate check against CDTFA's current California city and county rate table for San Jose.
- A Shopify catalog audit for active physical variants whose `taxable` value is false.
- A recent-order audit using stored Shopify raw payloads and tax lines.
- Verification that POS orders come from the configured San Jose Shopify location.
- Detection of unexpected non-POS orders while the store is declared POS-only.
- Deduplicated findings in the existing Shopify sync issue queue.
- Structured logs and last-success timestamps for each check.
- Focused unit and integration tests with mocked Shopify and CDTFA responses.
- A short operations runbook covering findings, manual correction, verification, and the shipping-launch gate.

## Non-Scope

Out of scope:

- Automatically editing Shopify tax rates.
- Automatically changing a product or variant's taxable flag.
- Browser automation against Shopify Admin.
- Building or certifying a Shopify tax-calculation app.
- Replacing Shopify Tax, a third-party tax provider, or professional tax advice.
- Calculating destination tax for online or shipped orders.
- Enabling an online storefront, shipping, local delivery, or customer checkout.
- Blocking or modifying live POS transactions.
- Sending Discord, SMS, or other external notifications in phase one.
- Deciding whether a cashier's tax override was legally justified.
- Monitoring taxes outside the United States.

## Constraints

- Tax checks must be deterministic. AI may summarize a finding later but cannot determine a rate or apply a correction.
- The official source is CDTFA, not a search-engine result or an inferred rate.
- The store's exact address must be verified once with CDTFA's address lookup before enabling the sentinel. The city table is then monitored for subsequent San Jose rate changes.
- A source fetch or parse failure is an error, not evidence that the existing rate remains correct.
- Manual Tax has no supported Admin GraphQL mutation for changing merchant tax rates. Deprecated REST tax fields are not an acceptable write path.
- Existing Shopify credentials must not be printed, logged, or expanded to broader scopes unless implementation proves a narrowly required scope is missing.
- Existing unrelated worktree files must remain untouched.
- Production activation is a separate, approval-gated action after local verification.

## Approaches Considered

### Recommended: Read-Only POS Sentinel

Use the existing Shopify Admin integration, stored order payloads, and Shopify sync issue queue. Check the official San Jose rate weekly, audit catalog taxability, and audit actual POS orders. Raise findings but never write to Shopify.

This gives Degen early warning with a small blast radius and makes the POS-only boundary explicit.

### Rejected: Autonomous Global Rate Editor

An agent that rewrites California's global rate cannot solve destination taxation. Changing the rate to Oakland's 10.75 percent for one shipped order would make later San Jose POS orders wrong. Shopify does not expose a supported merchant tax-rate mutation for this use case.

### Deferred: Destination-Aware Tax Service

Shopify Tax or a qualified third-party provider is the correct architecture once Degen accepts shipped orders. It calculates from the destination instead of mutating one global California rate. This is deferred until an online or shipped-order project is approved.

## Recommended Design

### 1. Sentinel Module

Add a focused module responsible for four independent checks:

- `check_official_pos_rate`: fetch and parse CDTFA's current San Jose city rate.
- `check_shopify_variant_taxability`: query active physical Shopify variants and find `taxable = false`.
- `check_recent_pos_orders`: inspect recent stored Shopify order payloads for source, location, tax lines, and overrides or rate deviations.
- `check_pos_only_boundary`: flag any Shopify order that is not attributable to Shopify POS while POS-only mode is enabled.

Each check returns structured results. It does not write external state.

### 2. Configuration

Add explicit configuration rather than inferring tax policy:

- Sentinel enabled flag, default false.
- Expected Shopify POS location ID.
- Expected jurisdiction: San Jose, California.
- Last manually verified baseline rate: 10 percent.
- POS-only mode, default true for this deployment.
- Lookback window for completed-order auditing.

The expected location ID must resolve through Shopify to a San Jose address before the first production run.

### 3. Scheduling

Run catalog and order checks daily. Run the CDTFA source check weekly and once at startup only when the last successful official-rate check is older than seven days.

Use the existing supervised background-task pattern in `app/main.py`. One failed check must not terminate unrelated workers or suppress the other tax checks.

### 4. Findings

Reuse `ShopifySyncIssue` and the existing `/inventory/shopify-sync` queue. Add narrowly defined issue types:

- `taxable_variant_disabled`
- `pos_tax_rate_mismatch`
- `pos_tax_lines_missing`
- `pos_location_mismatch`
- `pos_tax_override_observed`
- `non_pos_order_detected`
- `official_tax_rate_changed`
- `official_tax_source_unavailable`

Issue keys must be deterministic so repeated checks increment occurrence counts instead of creating duplicates. Resolved conditions should close their corresponding open issues with an automated resolution note.

### 5. Actual-Charge Verification

Use Shopify order and line-item tax lines from `ShopifyOrder.raw_payload` as the primary evidence of what was charged. Avoid calculating tax as only `total_tax / subtotal` because discounts, rounding, refunds, and mixed taxable states can make that ratio misleading.

Only completed, non-refunded POS orders from the configured location participate in the normal-rate check. Orders with explicit or inferred cashier overrides are findings for review, not automatic failures and not inputs that change the expected rate.

### 6. Shipping Launch Gate

POS-only mode is an operational assertion, not a tax engine. Any non-POS order is a critical finding.

Before enabling online checkout, shipping, or local delivery, Degen must:

1. Determine its destination district-tax obligations with a tax professional.
2. Select Shopify Tax or a qualified third-party destination-tax provider.
3. Test representative California destination addresses, including San Jose and Oakland.
4. Disable POS-only mode only after the destination-aware service passes verification.

## Error Handling

- Shopify authentication or scope errors create a visible source-unavailable finding without exposing credentials.
- CDTFA network, markup, or parsing failures create `official_tax_source_unavailable` and preserve the last successful observation as stale evidence, clearly labeled with its timestamp.
- Malformed order payloads create `pos_tax_lines_missing` or a structured parse error tied to the order.
- Pagination must be complete before a catalog audit is marked successful.
- Partial audit results must not resolve existing findings.
- Every run logs start time, completion time, counts checked, findings opened, findings resolved, and check-specific errors.

## Verification

Implementation verification must include:

- Unit tests for parsing a CDTFA San Jose row at 10 percent.
- Unit tests for a changed official rate and a source parse failure.
- Paginated Shopify catalog tests covering taxable and non-taxable physical variants.
- Exclusion tests for non-physical products such as gift cards.
- POS order tests for a normal 10 percent tax line, zero-tax override, alternate rate, missing tax lines, refund, expected location, unexpected location, and non-POS source.
- Deduplication and auto-resolution tests for all new issue types.
- A disabled-feature test proving no background task starts.
- Compile check with `.\.venv\Scripts\python.exe -m compileall app`.
- Focused test execution for sentinel, Shopify sync, and order-ingest behavior.
- Full suite execution with `.\.venv\Scripts\python.exe -m pytest --tb=short -q` before commit.
- A read-only Shopify production preflight before activation.
- A manual POS test transaction or draft cart confirming 10 percent at the San Jose location after activation, without completing a real customer payment unless explicitly approved.

## Rollback

- Disable the sentinel configuration flag and restart through the normal deployment path.
- The sentinel performs no Shopify writes, so rollback does not require reversing tax, product, order, or customer data.
- Existing findings remain as audit history and can be resolved with a rollback note.
- If the CDTFA parser becomes unreliable, disable only the official-rate subcheck while preserving catalog and completed-order audits.

## Risks

- CDTFA can change page markup without changing rates. Mitigation: fail visibly, retain a stale timestamp, and never infer success from cached data.
- City-level tables cannot prove an address lies within incorporated San Jose. Mitigation: require one address-level CDTFA verification before activation.
- Shopify order payload shapes can vary by API version or sales channel. Mitigation: test real sanitized payload shapes and fail visibly on missing evidence.
- A cashier override may be legitimate. Mitigation: report it for review without changing global or product configuration.
- Some non-physical products may correctly be non-taxable. Mitigation: limit the invariant to physical variants and explicitly exclude gift cards.
- The sentinel could create alert fatigue. Mitigation: deterministic issue keys, occurrence counts, severity levels, and auto-resolution only after a complete successful check.
- The POS-only assertion could become stale when the business launches shipping. Mitigation: treat any non-POS order as critical and require a destination-tax launch gate.

## Open Questions

No blocking product questions remain for implementation planning. Production activation still requires live verification of the Shopify POS location ID, the exact store address in CDTFA's address lookup, current API scopes, and sanitized real POS order payload fields. Those are preflight evidence checks, not assumptions to encode in the implementation.

## Official References

- CDTFA California city and county rates: https://cdtfa.ca.gov/taxes-and-fees/rates.aspx
- CDTFA district tax delivery rules: https://www.cdtfa.ca.gov/formspubs/pub105/basic-rules.htm
- Shopify Manual Tax settings and limitations: https://help.shopify.com/en/manual/taxes/manual-tax-settings
- Shopify POS tax setup: https://help.shopify.com/en/manual/taxes/set-adjust-pos-taxes/
- Shopify POS cart tax adjustments: https://help.shopify.com/en/manual/sell-in-person/shopify-pos/order-management/adjust-tax-rates
