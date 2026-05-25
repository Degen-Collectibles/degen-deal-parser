# Singles Lookup + POS Pilot Runbook

## Goal

Use inventory as the employee lookup source for tracked singles. Shopify POS only deducts inventory for linked Shopify POS products or variants whose SKU is the matching Degen barcode.

## What To Inventory

Inventory singles that employees are likely to be asked about or need to find quickly:

- Binder singles
- Case singles
- High-demand singles
- Higher-value singles
- Cards customers repeatedly ask about

Do not inventory bulk commons for this pilot.

## Required Fields

Every tracked single needs:

- Card identity
- Condition
- Quantity
- Location or bin when known; blank locations default to `Ungrouped`
- Degen barcode
- Optional sell price

## Lookup Rule

Use `/inventory` for employee lookup. Trust rows with a specific location first. `Ungrouped` means the item is tracked but still needs a home location. If a row has the wrong location, looks duplicated, or seems stale, flag it for cleanup before relying on it with a customer.

## POS Checkout Rule

When deduction matters, ring the tracked single through the Shopify POS product or variant whose SKU is the Degen barcode. Do not ring tracked singles as custom products if you expect inventory to deduct.

## Channel Rule

Pilot singles are POS-only by default. They must not be published to Online Store, TikTok, Shop, Google, marketplaces, or any other customer-facing channel unless a manager explicitly approves that specific product.

## Shopify Setup Rule

Singles can auto-create Shopify products when a manager/admin explicitly runs sync. The created product keeps the Degen barcode as SKU, is scoped for Point of Sale, and is not Online Store published by default. After creation, sync removes non-POS sales-channel publications that Shopify reports. If that cleanup cannot be verified, the product is drafted and sync fails instead of leaving a possibly public single active. Managers can deliberately enable additional sales channels later in Shopify.

## Cleanup Queue

Prioritize cleanup for:

- Legacy rows with missing location
- `Ungrouped` default locations that should be moved to a real bin
- Missing condition
- Duplicate-looking singles
- Shopify unknown SKU issues
- Anything unexpectedly customer-facing

## Preflight Note

Shopify admin preflight showed:

- Store domain: `degencollectibles.myshopify.com`
- Admin configured: `true`
- Configured location id: blank

Because the configured Shopify location id was blank, quantity sync should rely on item-level or primary-location discovery until a specific POS location id is configured.
