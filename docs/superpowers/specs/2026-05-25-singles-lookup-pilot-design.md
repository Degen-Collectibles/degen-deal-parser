# Official Singles Lookup + POS Deduction Pilot PRD

Date: 2026-05-25
Status: Approved for implementation planning
Owner: Degen Collectibles operations

## Problem

Employees spend too much time figuring out whether a single card is in stock, where it is, and what condition or price it has. That slows down customer answers on the floor and makes inventory knowledge depend on whichever employee last handled the card.

The first problem is internal lookup: employees need a fast, trusted way to answer, "Do we have this card, and where is it?"

There is one important correction to the original internal-only framing: Shopify is also the store POS. If a tracked single is sold through Shopify POS, the sale should deduct inventory. The pilot should therefore support POS-only Shopify linkage for checkout and inventory deduction while preventing customer-facing publication on the website, TikTok, Shop, Google, marketplaces, or any other sales channel unless Jeffrey explicitly enables that product.

## Current State

The repo already has the core inventory foundation:

- `/inventory` lists stock with search and filters across card name, barcode, set, variant, certificate number, UPC, and location.
- `/inventory/add-stock` can receive sealed product and singles from search results.
- Degen Eye scanner routes can identify singles and build a local batch for review.
- `/inventory/batch/confirm` can create inventory items from scanned cards.
- `InventoryItem` already stores item type, game, card identity, variant, condition, quantity, location, pricing, status, image, barcode, and Shopify fields.
- `InventoryStockMovement` records stock movement history for receive and adjustment flows.
- Employee permissions already allow shop-floor access to Degen Eye, scanner pages, add-stock, and a limited inventory list.
- Shopify order webhooks already call `mark_inventory_sold_from_shopify_order()`, which decrements local inventory when a Shopify line-item SKU exactly matches a local Degen barcode such as `DGN-000123`.
- Shopify POS can track inventory at the selected POS location when products are tracked and assigned to locations.

Known gaps before official use:

- The scanner batch-confirm singles path creates `InventoryItem` rows directly instead of reusing the fuller single receiving workflow.
- The direct scanner path can miss stock movement history and consistent merge behavior.
- Location is critical for internal lookup, but current single merge logic does not include location as part of the identity key.
- The current inventory surface is broad; employees need a simple lookup-first operating pattern.
- There is no pilot policy defining which singles should be entered, who reviews bad data, or when the data is trustworthy enough to become official.
- The current Shopify push path can create Shopify products. For tracked singles, that path must not publish items to the Online Store or other customer-purchasable channels by default.
- POS-only products need an explicit channel safety check: available in Point of Sale, unavailable from Online Store and other customer-facing sales channels unless manually approved.

## Success Criteria

The pilot is successful when:

- Employees can answer a customer stock question from `/inventory` without asking the whole team.
- A normal lookup by card name returns useful results in under 10 seconds of employee time.
- Every pilot single has a condition and a usable location or bin.
- Newly received pilot singles create an auditable stock movement.
- Scanner-confirmed singles merge or create records consistently with manual single receive.
- Duplicate records are rare and visible enough for manager cleanup.
- Tracked singles sold through Shopify POS decrement local Degen inventory through the existing webhook path.
- Pilot singles are not purchasable by customers on the website, TikTok, Shop, Google, marketplaces, or any other non-POS sales channel unless specifically marked for that channel.
- Unknown or mismatched Shopify POS SKUs are visible in the Shopify sync issue queue instead of silently failing.
- After 1 to 2 weeks, the team can decide from evidence whether singles lookup is ready to become official shop practice.

## Scope

In scope:

- Internal employee lookup for singles.
- Receiving singles through existing `/inventory/add-stock`, Degen Eye, and batch review flows.
- Making scanned singles use the same receiving rules as manually received singles.
- Requiring or strongly enforcing condition and location during pilot intake.
- Preserving inventory movement history for received singles.
- Adding or improving lookup filters needed for floor use, such as missing location, singles only, and potentially location/bin search.
- Manager cleanup views for missing locations, duplicate-looking singles, and questionable scan results.
- POS-only Shopify linkage for tracked singles that employees will check out through Shopify POS.
- Local inventory deduction from Shopify order webhooks when sold POS line items use the local Degen barcode as SKU.
- Guardrails to prevent pilot singles from being published to customer-facing sales channels by default.
- A written shop-floor operating procedure for what to inventory and how to answer lookup questions.

## Non-Scope

Out of scope for the pilot:

- Public Shopify website publication for singles by default.
- TikTok product creation or sync by default.
- Publishing pilot singles to Shop, Google, marketplaces, social commerce, or any non-POS sales channel by default.
- Automatic "sell everywhere" behavior for newly tracked singles.
- Financial inventory accounting changes.
- Full bulk-card inventory.
- Perfect card-level provenance for every low-value common.
- Warehouse-grade multi-location quantity tracking unless the pilot proves it is needed.
- Replacing the existing `/inventory` and Degen Eye surfaces with a separate standalone tool.

## Constraints

- Prefer improving existing routes and workflows instead of building a parallel app.
- Keep the pilot reversible and low-risk.
- Do not change Shopify webhook behavior or TikTok integrations.
- Do not make production data destructive migrations for the pilot.
- Keep employee workflow simple enough for the shop floor.
- Do not require employees to enter every card in bulk boxes.
- The current `InventoryItem` model has one `location` field per item. If identical cards exist in multiple places, the pilot needs an explicit operating rule or a future location-quantity model.
- A Shopify-linked pilot single must use the local Degen barcode as the Shopify variant SKU so the existing webhook deduction path can match it.
- The default Shopify state for pilot singles must be POS-only. Website and other customer-facing channel availability must be opt-in per product.
- Before any code path creates or links a Shopify product for singles, it must verify the product cannot be purchased outside POS unless explicitly approved.

## Recommended Design

Use a lookup-first pilot built on the existing inventory system, with POS-only Shopify linkage for items that employees will check out through Shopify POS.

### Employee Workflow

Employees add singles only when the card is worth tracking for customer lookup:

- binder cards
- case cards
- high-demand cards
- higher-value singles
- cards customers ask about often
- anything staff repeatedly wastes time searching for

Employees should not inventory every bulk common during the pilot.

For each single, employees must capture:

- game
- card name
- set or card number when available
- variant or printing when relevant
- condition
- quantity
- location or bin
- optional sell price
- optional notes

The lookup behavior should be simple:

1. Employee opens `/inventory`.
2. Searches card name or scans barcode.
3. Filters to singles if needed.
4. Reads quantity, condition, location, and price.
5. If the result is missing location or looks wrong, employee flags it for manager cleanup rather than guessing.

The checkout behavior should also be simple:

1. If a tracked single is sold in person, employees check it out through Shopify POS using a Shopify product/variant whose SKU is the local Degen barcode.
2. Shopify POS decrements Shopify's POS-location inventory.
3. The Shopify order webhook reaches the Degen app.
4. The Degen app matches the line-item SKU to `InventoryItem.barcode`.
5. The Degen app decrements local quantity and logs an `InventoryStockMovement` sale.

### Intake Rules

Manual add-stock and scanner batch-confirm should converge on the same receive behavior for singles:

- Use the existing single identity fields: game, card name, set name, card number, variant, and condition.
- Create or update an `InventoryItem`.
- Generate a barcode for new items.
- Increment quantity on matching items.
- Create an `InventoryStockMovement`.
- Store the employee/source where possible.
- Preserve scanner image and price data when available.

Location handling needs a pilot rule:

- Preferred pilot rule: a tracked single should have one home location. If the same card exists in two places, either consolidate it or create a manager-reviewed exception.
- Future option: if multi-location quantities matter, add a location-level stock table instead of overloading the single `location` field.

### Shopify POS Rules

For pilot singles, Shopify linkage is allowed only for in-person checkout and inventory deduction.

Default rules:

- Use the local Degen barcode as the Shopify variant SKU.
- Make the product available to Point of Sale only.
- Do not make the product available to Online Store, TikTok, Shop, Google, marketplaces, social commerce, or other customer-facing channels.
- Do not auto-publish a single to the website just because it was scanned, received, repriced, or linked.
- Only managers/admins can explicitly mark a product as customer-facing.
- Any "publish outside POS" action must be visible, deliberate, and auditable.

Implementation guardrail:

- Until the code can prove POS-only channel availability, single-item Shopify creation should default to no public publication. Linking to an existing POS-only Shopify variant is safer than creating a new active product without channel controls.
- If the Shopify API path cannot enforce sales-channel availability safely, the rollout should require a manager-side manual Shopify setup step for POS-only products before POS deduction is enabled.

### Manager Workflow

Managers/admins should review pilot data daily at first:

- missing location
- missing condition
- likely duplicates
- scanner low-confidence or manually corrected entries
- cards with stale or missing price when price is shown to customers
- Shopify sync issues for unknown SKUs or products that are not POS-only
- any product accidentally available outside POS

Cleanup should use existing inventory edit and bulk-action surfaces where possible.

### Rollout Plan

Phase 1: tighten workflow

- Make scanner batch-confirmed singles reuse the same single receive logic as manual add-stock.
- Ensure received singles have movement history.
- Add or expose filters for missing location and singles-only lookup if current search is not enough.
- Keep public sales-channel sync out of the pilot.
- Add a POS-only Shopify linking/creation guardrail for singles before enabling POS checkout on pilot cards.
- Keep the existing Shopify order webhook deduction path, but verify it covers POS orders whose line-item SKU matches the Degen barcode.

Phase 2: shop-floor pilot

- Pick one category of cards to start, such as Pokemon binder cards or high-demand case singles.
- Define a small location taxonomy, such as Case A, Case B, Binder 1, Back Stock, Review Bin.
- Train employees on intake and lookup.
- Train employees to use the Degen barcode/SKU product in Shopify POS when checking out a tracked single.
- Run daily cleanup for the first week.

Phase 3: evaluate

- Check how many cards were entered.
- Check missing-location and duplicate rates.
- Ask employees whether lookup answered real customer questions.
- Decide whether to expand to more categories or improve the model first.
- Decide whether any singles should be deliberately promoted from POS-only to online/customer-facing sales channels.

## Risks

- Bad locations are worse than no inventory because employees will trust the lookup and send customers to the wrong place.
- If intake is too slow, employees will stop using it.
- If every bulk common is included, the pilot will become a data-entry sink.
- If scanner confidence is treated as truth without review, wrong card identities will pollute lookup.
- If identical cards exist in multiple locations, the current model can hide location-level quantity differences.
- If Shopify sync creates active/public products by default, internal cleanup data could leak into public selling workflows.
- If POS-only channel controls are wrong, customers could purchase items from the website or other channels before the team intended.
- If employees sell a tracked single as a custom Shopify POS sale instead of using the Degen-barcode SKU, Shopify and local inventory will not deduct the specific item.
- If a Shopify app or sales channel re-publishes products automatically, POS-only guardrails need monitoring beyond the Degen app.

## Verification

Before rollout, verify:

- Manual single receive creates or updates an item and logs a stock movement.
- Scanner batch confirm creates or updates through the same receive logic.
- Duplicate scanned copies of the same card increment quantity rather than creating unnecessary duplicate rows.
- Condition and location are visible from `/inventory`.
- Missing-location entries are easy to find.
- Employee users can search and receive pilot singles but cannot access manager-only destructive actions.
- A Shopify POS order with line-item SKU equal to a local Degen barcode decrements local quantity once and logs a sale movement.
- A repeated Shopify webhook for the same order does not double-decrement local inventory.
- A Shopify POS order with an unknown SKU creates a visible Shopify sync issue.
- Pilot singles are not available on Online Store, TikTok, Shop, Google, marketplaces, social commerce, or other non-POS customer-facing channels by default.
- An explicit manager/admin action is required to publish a product outside POS.

Code verification should include:

- `.\.venv\Scripts\python.exe -m compileall app`
- Focused tests for inventory receive, scanner batch confirm, employee permissions, and lookup filters.
- Full test suite before commit, per repo rule.

## Rollback

The pilot should be reversible:

- Keep changes additive and route-compatible.
- Do not run destructive migrations.
- Do not enable public channel sync for singles during the pilot.
- If pilot data is poor, stop intake by removing employee navigation or permissioning the receive route while keeping lookup read-only.
- If Shopify channel safety is questionable, disable single-item Shopify creation/linking and keep the system lookup-only until channel controls are verified.
- Bad pilot rows can be archived or corrected through existing inventory management tools.

## Open Questions

- What exact card category should the first pilot cover: Pokemon binder cards, case cards, high-value singles, or all employee-requested singles?
- What location taxonomy should employees use?
- Should employees be allowed to edit location after receiving, or should only managers clean that up?
- Is price required for internal lookup, or should price be optional until sale/listing time?
- Should labels be printed during the pilot, or is lookup by name/location enough?
- How should employees flag "lookup says yes but I cannot find it"?
- Who owns the daily cleanup pass during the first week?
- When, if ever, should we build a verified automatic POS-only Shopify product creation flow after managers/admins have proven safe linking to existing POS-only variants?
- Which Shopify sales channels are currently installed and need explicit exclusion for pilot singles?
- What is the exact POS location ID that should hold tracked single inventory?
- Should Shopify POS-only products be Active but POS-only, Draft plus manually added to POS, or another Shopify-supported state after live verification?
- What manager/admin UI should explicitly promote selected products to customer-facing channels?

## Approval Gate

Implementation should not start until Jeffrey approves:

- internal lookup plus POS-only Shopify deduction
- no website/TikTok/Shop/Google/marketplace/customer-facing sales-channel publication by default
- tracked-singles-only intake policy
- required condition and location
- scanner batch-confirm cleanup as the first code change
- Shopify POS linkage guardrail as a required part of the pilot before employees rely on automatic deduction
