from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from sqlmodel import SQLModel, Session, create_engine, select

from app.inventory.shopify import (
    SHOPIFY_API_VERSION,
    ShopifyVariantRef,
    build_shopify_product_payload,
    get_shopify_inventory_item_location_id,
    get_shopify_primary_location_id,
    push_item_to_shopify,
    resolve_shopify_access_token,
    sync_shopify_inventory_quantity,
    unpublish_non_pos_product_publications,
)
from app.models import (
    INVENTORY_LISTED,
    InventoryItem,
    ShopifySyncIssue,
    ITEM_TYPE_SEALED,
    ITEM_TYPE_SINGLE,
)
from app.shopify_sync import (
    SHOPIFY_SYNC_ISSUE_OPEN,
    SHOPIFY_SYNC_ISSUE_UNLINKED_PRODUCT,
    SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU,
    record_shopify_sync_issue,
)
from app.shopify_sync_worker import sync_inventory_item_to_shopify


class ShopifyInventoryApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_supported_shopify_api_version(self):
        self.assertEqual(SHOPIFY_API_VERSION, "2026-04")

    async def test_product_payload_uses_degen_barcode_as_sku(self):
        item = InventoryItem(
            barcode="DGN-SKU1",
            item_type=ITEM_TYPE_SEALED,
            game="Pokemon",
            card_name="Prismatic Evolutions Booster Bundle",
            quantity=4,
            list_price=39.99,
        )

        payload = build_shopify_product_payload(item)

        variant = payload["product"]["variants"][0]
        self.assertEqual(variant["sku"], "DGN-SKU1")
        self.assertEqual(variant["inventory_quantity"], 4)
        self.assertEqual(variant["price"], "39.99")

    async def test_single_product_payload_is_pos_scoped_not_online_store_published(self):
        item = InventoryItem(
            barcode="DGN-SINGLE1",
            item_type=ITEM_TYPE_SINGLE,
            game="Pokemon",
            card_name="Pikachu",
            condition="NM",
            quantity=1,
            list_price=9.99,
        )

        payload = build_shopify_product_payload(item)

        product = payload["product"]
        self.assertEqual(product["product_type"], "Singles")
        self.assertEqual(product["published_scope"], "global")
        self.assertIsNone(product["published_at"])
        self.assertEqual(product["status"], "active")
        self.assertEqual(product["variants"][0]["sku"], "DGN-SINGLE1")

    async def test_single_product_creation_unpublishes_non_pos_publications(self):
        item = InventoryItem(
            barcode="DGN-SINGLE2",
            item_type=ITEM_TYPE_SINGLE,
            game="Pokemon",
            card_name="Raichu",
            condition="NM",
            quantity=1,
            list_price=12.0,
        )
        create_payload = {
            "product": {
                "id": 111,
                "variants": [{"id": 222, "inventory_item_id": 333, "sku": "DGN-SINGLE2"}],
            }
        }
        publications_payload = {
            "data": {
                "product": {
                    "resourcePublicationsV2": {
                        "nodes": [
                            {
                                "publication": {
                                    "id": "gid://shopify/Publication/pos",
                                    "channels": {
                                        "nodes": [
                                            {
                                                "id": "gid://shopify/Channel/pos",
                                                "handle": "point-of-sale",
                                                "name": "Point of Sale",
                                                "app": {"handle": "point-of-sale", "title": "Point of Sale"},
                                            }
                                        ]
                                    },
                                }
                            },
                            {
                                "publication": {
                                    "id": "gid://shopify/Publication/shop",
                                    "channels": {
                                        "nodes": [
                                            {
                                                "id": "gid://shopify/Channel/shop",
                                                "handle": "shop",
                                                "name": "Shop",
                                                "app": {"handle": "shop", "title": "Shop"},
                                            }
                                        ]
                                    },
                                }
                            },
                        ]
                    }
                }
            }
        }
        unpublish_payload = {
            "data": {
                "publishableUnpublish": {
                    "userErrors": [],
                }
            }
        }

        async with _FakeAsyncClient([create_payload, publications_payload, unpublish_payload]) as client:
            result = await push_item_to_shopify(
                item,
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=client,
            )

        self.assertEqual(result["shopify_product_id"], "111")
        self.assertEqual(result["shopify_variant_id"], "222")
        self.assertEqual(client.posts[0]["url"], "https://degen-test.myshopify.com/admin/api/2026-04/products.json")
        self.assertEqual(client.posts[1]["url"], "https://degen-test.myshopify.com/admin/api/2026-04/graphql.json")
        self.assertEqual(client.posts[2]["json"]["variables"]["input"], [{"publicationId": "gid://shopify/Publication/shop"}])
        self.assertEqual(client.puts, [])

    async def test_single_product_creation_drafts_product_when_publication_cleanup_fails(self):
        item = InventoryItem(
            barcode="DGN-SINGLE3",
            item_type=ITEM_TYPE_SINGLE,
            game="Pokemon",
            card_name="Charmander",
            condition="NM",
            quantity=1,
            list_price=15.0,
        )
        create_payload = {
            "product": {
                "id": 111,
                "variants": [{"id": 222, "inventory_item_id": 333, "sku": "DGN-SINGLE3"}],
            }
        }
        cleanup_failure = {"errors": [{"message": "Access denied for publications"}]}
        draft_payload = {"product": {"id": 111, "status": "draft"}}

        async with _FakeAsyncClient([create_payload, cleanup_failure, draft_payload]) as client:
            result = await push_item_to_shopify(
                item,
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=client,
            )

        self.assertIsNone(result)
        self.assertEqual(client.puts[0]["url"], "https://degen-test.myshopify.com/admin/api/2026-04/products/111.json")
        self.assertEqual(client.puts[0]["json"]["product"]["status"], "draft")

    async def test_unpublish_non_pos_product_publications_reports_user_errors(self):
        publications_payload = {
            "data": {
                "product": {
                    "resourcePublicationsV2": {
                        "nodes": [
                            {
                                "publication": {
                                    "id": "gid://shopify/Publication/shop",
                                    "channels": {
                                        "nodes": [
                                            {
                                                "id": "gid://shopify/Channel/shop",
                                                "handle": "shop",
                                                "name": "Shop",
                                                "app": {"handle": "shop", "title": "Shop"},
                                            }
                                        ]
                                    },
                                }
                            },
                        ]
                    }
                }
            }
        }
        unpublish_payload = {
            "data": {
                "publishableUnpublish": {
                    "userErrors": [{"message": "Publication does not exist"}],
                }
            }
        }

        async with _FakeAsyncClient([publications_payload, unpublish_payload]) as client:
            ok, errors = await unpublish_non_pos_product_publications(
                "111",
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=client,
            )

        self.assertFalse(ok)
        self.assertIn("Publication does not exist", errors[0])

    async def test_shopify_admin_token_falls_back_to_existing_api_key(self):
        self.assertEqual(
            resolve_shopify_access_token(
                SimpleNamespace(shopify_access_token="", shopify_api_key="legacy-token")
            ),
            "legacy-token",
        )

    async def test_primary_location_lookup_uses_supported_api_version(self):
        async with _FakeAsyncClient(
            {"locations": [{"id": 123, "active": True, "name": "Shop"}]}
        ) as client:
            location_id = await get_shopify_primary_location_id(
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=client,
            )

        self.assertEqual(location_id, "123")
        self.assertEqual(client.gets[0]["url"], "https://degen-test.myshopify.com/admin/api/2026-04/locations.json")

    async def test_inventory_item_location_lookup_uses_inventory_levels(self):
        async with _FakeAsyncClient(
            {"inventory_levels": [{"inventory_item_id": 333, "location_id": 999}]}
        ) as client:
            location_id = await get_shopify_inventory_item_location_id(
                "333",
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=client,
            )

        self.assertEqual(location_id, "999")
        self.assertEqual(client.gets[0]["url"], "https://degen-test.myshopify.com/admin/api/2026-04/inventory_levels.json")
        self.assertEqual(client.gets[0]["params"]["inventory_item_ids"], "333")

    async def test_quantity_sync_uses_graphql_inventory_set_quantities_with_idempotency(self):
        item = InventoryItem(
            id=42,
            barcode="DGN-QTY1",
            item_type=ITEM_TYPE_SEALED,
            game="Pokemon",
            card_name="Quantity Box",
            quantity=7,
            shopify_inventory_item_id="30322695",
            shopify_location_id="124656943",
        )

        async with _FakeAsyncClient(
            {
                "data": {
                    "inventorySetQuantities": {
                        "inventoryAdjustmentGroup": {
                            "changes": [{"quantityAfterChange": 7}]
                        },
                        "userErrors": [],
                    }
                }
            }
        ) as client:
            ok, error = await sync_shopify_inventory_quantity(
                item,
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=client,
                idempotency_key="unit-test-key",
            )

        self.assertTrue(ok)
        self.assertIsNone(error)
        self.assertEqual(client.posts[0]["url"], "https://degen-test.myshopify.com/admin/api/2026-04/graphql.json")
        body = client.posts[0]["json"]
        self.assertIn("@idempotent(key: $idempotencyKey)", body["query"])
        self.assertEqual(body["variables"]["idempotencyKey"], "unit-test-key")
        quantity_row = body["variables"]["input"]["quantities"][0]
        self.assertEqual(quantity_row["inventoryItemId"], "gid://shopify/InventoryItem/30322695")
        self.assertEqual(quantity_row["locationId"], "gid://shopify/Location/124656943")
        self.assertEqual(quantity_row["quantity"], 7)
        self.assertIsNone(quantity_row["changeFromQuantity"])

    async def test_quantity_sync_reports_user_errors(self):
        item = InventoryItem(
            id=43,
            barcode="DGN-QTY2",
            item_type=ITEM_TYPE_SEALED,
            game="Pokemon",
            card_name="Quantity Error Box",
            quantity=2,
            shopify_inventory_item_id="30322696",
            shopify_location_id="124656944",
        )

        async with _FakeAsyncClient(
            {
                "data": {
                    "inventorySetQuantities": {
                        "inventoryAdjustmentGroup": None,
                        "userErrors": [{"message": "Location is inactive"}],
                    }
                }
            }
        ) as client:
            ok, error = await sync_shopify_inventory_quantity(
                item,
                store_domain="degen-test.myshopify.com",
                access_token="shpat_test",
                client=client,
                idempotency_key="unit-test-key",
            )

        self.assertFalse(ok)
        self.assertIn("Location is inactive", error)


class ShopifySyncIssueTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_record_unknown_sku_issue_upserts_existing_open_issue(self):
        with Session(self.engine) as session:
            first = record_shopify_sync_issue(
                session,
                issue_type=SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU,
                shopify_sku="DGN-MISSING",
                shopify_title="Missing Shopify Line",
                message="Shopify sold a SKU that does not exist locally.",
                payload={"order_id": "1001"},
            )
            second = record_shopify_sync_issue(
                session,
                issue_type=SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU,
                shopify_sku="DGN-MISSING",
                shopify_title="Missing Shopify Line",
                message="Seen again.",
                payload={"order_id": "1002"},
            )
            session.commit()

            self.assertEqual(first.id, second.id)
            issues = session.exec(select(ShopifySyncIssue)).all()
            self.assertEqual(len(issues), 1)
            self.assertEqual(issues[0].status, SHOPIFY_SYNC_ISSUE_OPEN)
            self.assertEqual(issues[0].message, "Seen again.")

    def test_unlinked_product_issue_preserves_shopify_ids_for_review(self):
        with Session(self.engine) as session:
            issue = record_shopify_sync_issue(
                session,
                issue_type=SHOPIFY_SYNC_ISSUE_UNLINKED_PRODUCT,
                shopify_sku="SHOPIFY-OLD",
                shopify_title="Old Shopify Product",
                message="No local inventory item is linked.",
                shopify_product_id="111",
                shopify_variant_id="222",
                shopify_inventory_item_id="333",
                shopify_location_id="444",
                payload={"price": "9.99"},
            )
            session.commit()
            session.refresh(issue)

            self.assertEqual(issue.shopify_product_id, "111")
            self.assertEqual(issue.shopify_variant_id, "222")
            self.assertEqual(issue.shopify_inventory_item_id, "333")
            self.assertEqual(issue.shopify_location_id, "444")
            self.assertEqual(issue.status, SHOPIFY_SYNC_ISSUE_OPEN)


class ShopifySyncWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_single_without_existing_shopify_variant_can_create_pos_scoped_product(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                item = InventoryItem(
                    barcode="DGN-SINGLE1",
                    item_type=ITEM_TYPE_SINGLE,
                    game="Pokemon",
                    card_name="Pikachu",
                    set_name="Base Set",
                    card_number="58/102",
                    condition="NM",
                    quantity=1,
                    list_price=9.99,
                )
                session.add(item)
                session.commit()
                session.refresh(item)

                with patch("app.shopify_sync_worker.settings") as mocked_settings, patch(
                    "app.shopify_sync_worker.find_shopify_variant_by_sku",
                    new=AsyncMock(return_value=None),
                ) as mocked_find, patch(
                    "app.shopify_sync_worker.push_item_to_shopify",
                    new=AsyncMock(return_value={
                        "shopify_product_id": "111",
                        "shopify_variant_id": "222",
                        "shopify_inventory_item_id": "333",
                        "shopify_sku": "DGN-SINGLE1",
                    }),
                ) as mocked_push, patch(
                    "app.shopify_sync_worker.update_shopify_variant_price",
                    new=AsyncMock(return_value=True),
                ) as mocked_price, patch(
                    "app.shopify_sync_worker.sync_shopify_inventory_quantity",
                    new=AsyncMock(return_value=(True, None)),
                ) as mocked_qty:
                    mocked_settings.shopify_store_domain = "degen-test.myshopify.com"
                    mocked_settings.shopify_access_token = "shpat_test"
                    mocked_settings.shopify_api_key = ""
                    mocked_settings.shopify_location_id = "444"

                    ok, error = await sync_inventory_item_to_shopify(
                        session,
                        item,
                        source="unit-test",
                    )

                self.assertTrue(ok)
                self.assertEqual(error, "")
                mocked_find.assert_awaited_once()
                mocked_push.assert_awaited_once()
                self.assertEqual(mocked_push.await_args.args[0].barcode, "DGN-SINGLE1")
                mocked_price.assert_awaited_once()
                mocked_qty.assert_awaited_once()
                session.refresh(item)
                self.assertEqual(item.shopify_product_id, "111")
                self.assertEqual(item.shopify_variant_id, "222")
                self.assertEqual(item.shopify_inventory_item_id, "333")
                self.assertEqual(item.shopify_location_id, "444")
                self.assertEqual(item.shopify_sku, "DGN-SINGLE1")
                self.assertEqual(item.status, INVENTORY_LISTED)
                self.assertEqual(item.shopify_sync_status, "synced")
        finally:
            engine.dispose()

    async def test_single_with_existing_shopify_variant_can_sync_quantity(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                item = InventoryItem(
                    barcode="DGN-SINGLE2",
                    item_type=ITEM_TYPE_SINGLE,
                    game="Pokemon",
                    card_name="Bulbasaur",
                    condition="NM",
                    location="Case A",
                    quantity=3,
                    list_price=4.99,
                )
                session.add(item)
                session.commit()
                session.refresh(item)

                variant_ref = ShopifyVariantRef(
                    sku="DGN-SINGLE2",
                    product_id="111",
                    variant_id="222",
                    inventory_item_id="333",
                    location_gid="gid://shopify/Location/444",
                    product_handle="pos-only-single",
                    product_status="draft",
                )

                with patch("app.shopify_sync_worker.settings") as mocked_settings, patch(
                    "app.shopify_sync_worker.find_shopify_variant_by_sku",
                    new=AsyncMock(return_value=variant_ref),
                ) as mocked_find, patch(
                    "app.shopify_sync_worker.push_item_to_shopify",
                    new=AsyncMock(return_value={"shopify_product_id": "555", "shopify_variant_id": "666"}),
                ) as mocked_push, patch(
                    "app.shopify_sync_worker.update_shopify_variant_price",
                    new=AsyncMock(return_value=True),
                ) as mocked_price, patch(
                    "app.shopify_sync_worker.sync_shopify_inventory_quantity",
                    new=AsyncMock(return_value=(True, None)),
                ) as mocked_qty:
                    mocked_settings.shopify_store_domain = "degen-test.myshopify.com"
                    mocked_settings.shopify_access_token = "shpat_test"
                    mocked_settings.shopify_api_key = ""
                    mocked_settings.shopify_location_id = "444"

                    ok, error = await sync_inventory_item_to_shopify(
                        session,
                        item,
                        source="unit-test",
                    )

                self.assertTrue(ok)
                self.assertEqual(error, "")
                mocked_find.assert_awaited_once()
                mocked_push.assert_not_awaited()
                mocked_price.assert_awaited_once()
                mocked_qty.assert_awaited_once()
                self.assertEqual(mocked_qty.await_args.kwargs["location_id"], "444")
                self.assertEqual(mocked_qty.await_args.kwargs["access_token"], "shpat_test")

                session.refresh(item)
                self.assertEqual(item.shopify_product_id, "111")
                self.assertEqual(item.shopify_variant_id, "222")
                self.assertEqual(item.shopify_inventory_item_id, "333")
                self.assertEqual(item.shopify_location_id, "444")
                self.assertEqual(item.shopify_sku, "DGN-SINGLE2")
                self.assertEqual(item.shopify_product_handle, "pos-only-single")
                self.assertEqual(item.shopify_product_status, "draft")
                self.assertEqual(item.shopify_sync_status, "synced")
                self.assertEqual(item.status, INVENTORY_LISTED)
        finally:
            engine.dispose()

    async def test_sync_inventory_item_updates_price_quantity_and_status(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                item = InventoryItem(
                    barcode="DGN-WORK1",
                    item_type=ITEM_TYPE_SEALED,
                    game="Pokemon",
                    card_name="Worker Sync Box",
                    quantity=4,
                    list_price=29.99,
                    shopify_product_id="111",
                    shopify_variant_id="222",
                    shopify_inventory_item_id="333",
                    shopify_location_id="444",
                )
                session.add(item)
                session.commit()
                session.refresh(item)

                with patch("app.shopify_sync_worker.settings") as mocked_settings, patch(
                    "app.shopify_sync_worker.update_shopify_variant_price",
                    new=AsyncMock(return_value=True),
                ) as mocked_price, patch(
                    "app.shopify_sync_worker.sync_shopify_inventory_quantity",
                    new=AsyncMock(return_value=(True, None)),
                ) as mocked_qty:
                    mocked_settings.shopify_store_domain = "degen-test.myshopify.com"
                    mocked_settings.shopify_access_token = "shpat_test"
                    mocked_settings.shopify_api_key = ""
                    mocked_settings.shopify_location_id = "444"

                    ok, error = await sync_inventory_item_to_shopify(
                        session,
                        item,
                        source="unit-test",
                    )

                self.assertTrue(ok)
                self.assertEqual(error, "")
                mocked_price.assert_awaited_once()
                mocked_qty.assert_awaited_once()
                session.refresh(item)
                self.assertEqual(item.shopify_sync_status, "synced")
                self.assertIsNone(item.shopify_sync_error)
                self.assertIsNotNone(item.shopify_synced_at)
        finally:
            engine.dispose()

    async def test_sync_inventory_item_uses_legacy_api_key_and_discovers_location(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                item = InventoryItem(
                    barcode="DGN-WORK2",
                    item_type=ITEM_TYPE_SEALED,
                    game="Pokemon",
                    card_name="Worker Sync Fallback Box",
                    quantity=2,
                    list_price=19.99,
                    shopify_product_id="111",
                    shopify_variant_id="222",
                    shopify_inventory_item_id="333",
                )
                session.add(item)
                session.commit()
                session.refresh(item)

                with patch("app.shopify_sync_worker.settings") as mocked_settings, patch(
                    "app.shopify_sync_worker.update_shopify_variant_price",
                    new=AsyncMock(return_value=True),
                ) as mocked_price, patch(
                    "app.shopify_sync_worker.get_shopify_inventory_item_location_id",
                    new=AsyncMock(return_value="999"),
                ) as mocked_location, patch(
                    "app.shopify_sync_worker.sync_shopify_inventory_quantity",
                    new=AsyncMock(return_value=(True, None)),
                ) as mocked_qty:
                    mocked_settings.shopify_store_domain = "degen-test.myshopify.com"
                    mocked_settings.shopify_access_token = ""
                    mocked_settings.shopify_api_key = "legacy-token"
                    mocked_settings.shopify_location_id = ""

                    ok, error = await sync_inventory_item_to_shopify(
                        session,
                        item,
                        source="unit-test",
                    )

                self.assertTrue(ok)
                self.assertEqual(error, "")
                mocked_price.assert_awaited_once()
                mocked_location.assert_awaited_once()
                mocked_qty.assert_awaited_once()
                self.assertEqual(mocked_qty.await_args.kwargs["access_token"], "legacy-token")
                session.refresh(item)
                self.assertEqual(item.shopify_location_id, "999")
        finally:
            engine.dispose()


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, payload):
        self.payloads = list(payload) if isinstance(payload, list) else [payload]
        self.posts = []
        self.gets = []
        self.puts = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def post(self, url, **kwargs):
        self.posts.append({"url": url, **kwargs})
        return _FakeResponse(self.payloads.pop(0))

    async def get(self, url, **kwargs):
        self.gets.append({"url": url, **kwargs})
        return _FakeResponse(self.payloads.pop(0))

    async def put(self, url, **kwargs):
        self.puts.append({"url": url, **kwargs})
        return _FakeResponse(self.payloads.pop(0))


if __name__ == "__main__":
    unittest.main()
