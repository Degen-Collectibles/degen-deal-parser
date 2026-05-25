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
- Location or bin
- Degen barcode
- Optional sell price

## Lookup Rule

Use `/inventory` for employee lookup. Trust rows only when they have a usable location and condition. If a row is missing data, has the wrong location, looks duplicated, or seems stale, flag it for cleanup before relying on it with a customer.

## POS Checkout Rule

When deduction matters, ring the tracked single through the Shopify POS product or variant whose SKU is the Degen barcode. Do not ring tracked singles as custom products if you expect inventory to deduct.

## Channel Rule

Pilot singles are POS-only by default. They must not be published to Online Store, TikTok, Shop, Google, marketplaces, or any other customer-facing channel unless a manager explicitly approves that specific product.

## Shopify Setup Rule

Automatic Shopify product creation for singles is blocked. Managers or admins must link tracked singles to an existing POS-only Shopify variant until POS-only channel safety is proven.

## Cleanup Queue

Prioritize cleanup for:

- Missing location
- Missing condition
- Duplicate-looking singles
- Shopify unknown SKU issues
- Anything unexpectedly customer-facing

## Preflight Note

Shopify admin preflight showed:

- Store domain: `degencollectibles.myshopify.com`
- Admin configured: `true`
- Configured location id: blank

Because the configured Shopify location id was blank, automatic single product creation remains blocked.
