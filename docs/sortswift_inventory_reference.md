# SortSwift Inventory Reference

Observed from `https://app.sortswift.com/` and related inventory pages on May 10, 2026.

## Navigation

- Main menu exposes Inventory as a tool group.
- Inventory group includes View Stock, Staging, Add Stock, Swift Add, Picklist, Bulk Lots, Import, Export, Location Summary, Audit, Decklist Search, CSV history, and logs.
- Dashboard shows inventory job health and platform sync status for Shopify, CardTrader, ManaPool, Misprint, eBay, and Square.

## View Stock

- Filter-first inventory browser.
- Filters include game, singles/sealed/all, set, title, card number, comment, location/remark, condition, language, printing, rarity, color, card type, quantity range, price range, stock-added date range, release date, SKU, TCGPlayer ID, and platform sync status.
- Results support card/table views, pagination, sorting, bulk update, label printing, and CSV export.
- Inventory stats are tied to the active search.

## Add Stock

- Search-first receiving screen.
- Search fields include game, set, product/card title, card number, and UPC/barcode.
- Employees can toggle Singles, Sealed, or All.
- Batch defaults include remark/location, comment, and published-on value.
- Product results show image, title, set, UPC state, marketplace IDs, condition/language/printing, quantity stepper, price, floor, cost, current quantity, listing controls, and a single Add to Inventory action.
- Sealed Pokemon search for "Prismatic Evolutions" showed catalog product cards such as booster packs, Elite Trainer Boxes, Pokemon Center ETBs, binder/poster collections, tech sticker collections, and mini tins.

## Swift Add

- Wizard-style flow.
- Step 1 selects game and optional buylist mode.
- Intended as a guided add workflow separate from the denser Add Stock page.

## Staging

- Review queue for imported inventory.
- Filters include product search, review status, pushed status, and import batch.
- Supports bulk update, weighted cost, label printing, export, and reprice.

## Audit And Locations

- Audit mode loads items by game/set and lets staff adjust quantities one item at a time.
- Location summary aggregates quantity and value by location.
- Locations support capacity, refill threshold, verification, CSV export, and location-scoped audit/reprice.

## Degen V1 Mapping

- Keep the employee path simpler than SortSwift's full add-stock grid.
- Build a Pokemon sealed receiving page under `/inventory/sealed`.
- Search existing sealed stock and a small Pokemon sealed catalog.
- Let employees add quantity, unit cost, sell price, location, source, and notes in one action.
- Store sealed products in the existing inventory table with `item_type = sealed`.
- Record every receive action in `inventory_stock_movements` with quantity before/after for auditability.
- Keep broader tools like staging, imports, and location audits as future phases.
