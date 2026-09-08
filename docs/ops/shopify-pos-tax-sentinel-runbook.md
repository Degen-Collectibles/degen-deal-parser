# Shopify POS Tax Sentinel Runbook

## Current Operating Boundary

- This sentinel is read-only and covers in-person Shopify POS sales at the San Jose location only.
- The CDTFA city table baseline is San Jose 10.000%, effective April 1, 2026.
- Exact store-address lookup is still a mandatory activation preflight and must produce dated evidence. No exact-address result is asserted here.
- Physical product variants are expected to remain taxable.
- Cashier tax changes are per-transaction exceptions. They require review but are not automatically wrong.
- The sentinel records local Shopify sync issues and structured logs. It does not change Shopify products, orders, locations, or tax settings.

## Read-Only Preflight

Before considering production activation:

1. Verify the exact San Jose operating address with the CDTFA address lookup and record the effective date and combined rate.
2. Confirm **SHOPIFY_POS_LOCATION_ID** resolves through the Shopify Admin API to the active San Jose POS location.
3. Inspect the granted handles through **GET /admin/oauth/access_scopes.json** without showing or printing the token. Sentinel source reads require **read_products** for variant reads and **read_locations** for the REST **/admin/api/2026-04/locations.json** call. Verify the actual granted handles and do not request broader scopes automatically.
4. Run one sentinel cycle with only local issue recording enabled. Do not permit Shopify writes or tax-setting changes.
5. Review **/inventory/shopify-sync** and the structured logs for a single handled-run summary, sanitized failures, and expected evidence.

## Finding Response

All corrections are manual. The sentinel must never autonomously write tax, product, order, variant, or location data.

| Issue type | Evidence | Manual action | Verification |
|---|---|---|---|
| **taxable_variant_disabled** | Product, variant, and sanitized SKU identifiers show a physical variant with tax disabled. | Confirm the item should be taxable, then correct the variant tax setting in Shopify Admin. | Re-run the catalog check and confirm the issue resolves without a new finding. |
| **pos_tax_rate_mismatch** | Paid POS order tax lines sum to a rate different from the configured expected rate. | Confirm the exact sale address, effective rate, and order details; correct configuration or document and correct the transaction through the approved Shopify/accounting process. | Re-check the source rate and inspect a new controlled POS transaction at the expected rate. |
| **pos_tax_lines_missing** | A paid POS order has positive tax without usable order tax lines, or its stored payload cannot be safely evaluated. | Inspect the order in Shopify Admin and determine whether ingestion evidence is incomplete or the order needs manual accounting correction. | Refresh the order evidence and confirm the next sentinel cycle can evaluate it cleanly. |
| **pos_location_mismatch** | The paid POS order location differs from **SHOPIFY_POS_LOCATION_ID**, or the configured location does not resolve as active San Jose. | Confirm the register/location assignment and the dedicated POS location ID; correct the relevant Shopify or deployment configuration manually. | Resolve the configured ID through Shopify and inspect a new POS order from the intended location. |
| **pos_tax_override_observed** | A paid POS order contains taxable items but records no tax and no tax lines. | Review the cashier override and supporting exemption or correction. An observed override is not automatically wrong. | Document the approved exception or complete the manual correction, then verify the issue disposition in **/inventory/shopify-sync**. |
| **non_pos_order_detected** | A paid order source is not POS while POS-only mode is active. | Stop and investigate the sales channel. Do not treat online, shipping, or delivery orders as covered by the current manual-tax boundary. | Confirm online checkout, shipping, and local delivery remain disabled, or complete the Shipping Launch Gate before allowing them. |
| **official_tax_rate_changed** | The official CDTFA result for San Jose and Santa Clara differs from the configured expected rate. | Verify the exact operating address and effective date, consult the tax professional, and update configuration only through an approved deployment. | Repeat the exact-address lookup and controlled POS tax test after the approved change. |
| **official_tax_source_unavailable** | The official source could not provide a valid matching city and county rate. | Treat the official check as incomplete; investigate source availability or parsing without assuming the configured rate is current. | Re-run until the official source succeeds and the sentinel resolves the finding. |
| Sentinel-owned **sync_error** | Sanitized tax-sentinel evidence identifies which configuration, location, or catalog source check failed and includes only an error type. | Repair the named read path or configuration. Do not expose credentials or customer data while diagnosing. | Re-run the complete named check and confirm only the sentinel-owned error resolves. |

Persistence and database failures are not converted into sentinel-owned **sync_error** findings. They are rolled back, re-raised to the supervised loop, and represented only by its sanitized structured failure log.

## Shipping Launch Gate

Do not enable online checkout, shipping, or local delivery while relying on the POS-only Manual Tax boundary.

Before launch:

1. Obtain review from a qualified tax professional for destination-based collection obligations.
2. Adopt Shopify Tax or another qualified destination-aware tax provider.
3. Test at least San Jose and Oakland destinations, including product taxability, shipping treatment, refunds, and reporting.
4. Record results, rollback steps, and accountable owners.
5. Obtain explicit approval before enabling any customer-facing checkout, shipping, or delivery path.

## Rollback

1. Set **SHOPIFY_POS_TAX_SENTINEL_ENABLED** to false through the normal deployment process.
2. Verify the **shopify-pos-tax-sentinel** background task is absent after deployment.
3. Confirm Shopify order ingestion and the existing inventory sync remain healthy.
4. Preserve recorded findings and logs for review; disabling the scheduler does not require deleting evidence.

## Production Activation Boundary

This implementation does not activate the sentinel or change any production environment. Production activation is an externally visible operational change and requires a separate preflight showing the exact target, configuration values by name, reversibility, rollback, and post-change verification. Wait for Jeffrey's explicit "proceed" before any environment change, deployment, restart, or production cycle.
