import unittest

from sqlalchemy import inspect
from sqlmodel import SQLModel, create_engine

from app import db
from app.models import ShopifySyncIssue, ShopifySyncJob


class ShopifySyncSchemaTests(unittest.TestCase):
    def test_create_all_includes_shopify_sync_tables_and_inventory_columns(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        try:
            SQLModel.metadata.create_all(engine)
            inspector = inspect(engine)

            inventory_columns = {col["name"] for col in inspector.get_columns("inventory_items")}
            self.assertIn("shopify_inventory_item_id", inventory_columns)
            self.assertIn("shopify_location_id", inventory_columns)
            self.assertIn("shopify_sync_enabled", inventory_columns)
            self.assertIn("shopify_sync_status", inventory_columns)

            self.assertIn(ShopifySyncIssue.__tablename__, inspector.get_table_names())
            self.assertIn(ShopifySyncJob.__tablename__, inspector.get_table_names())
            issue_columns = {col["name"] for col in inspector.get_columns("shopify_sync_issues")}
            self.assertIn("issue_key", issue_columns)
            self.assertIn("shopify_sku", issue_columns)
            self.assertIn("occurrence_count", issue_columns)
        finally:
            engine.dispose()

    def test_additive_migrations_cover_existing_inventory_tables(self):
        expected = {
            "shopify_variant_id",
            "shopify_inventory_item_id",
            "shopify_location_id",
            "shopify_sku",
            "shopify_product_handle",
            "shopify_product_status",
            "shopify_sync_enabled",
            "shopify_synced_at",
            "shopify_sync_status",
            "shopify_sync_error",
            "shopify_last_payload_json",
        }
        self.assertTrue(expected.issubset(db.SQLITE_ADDITIVE_MIGRATIONS["inventory_items"]))
        self.assertTrue(expected.issubset(db.POSTGRES_ADDITIVE_MIGRATIONS["inventory_items"]))

    def test_index_migrations_cover_shopify_sync_lookup_paths(self):
        sqlite_indexes = "\n".join(db.SQLITE_INDEX_MIGRATIONS)
        postgres_indexes = "\n".join(db.POSTGRES_INDEX_MIGRATIONS)
        for sql in (sqlite_indexes, postgres_indexes):
            self.assertIn("idx_shopify_sync_issues_issue_key", sql)
            self.assertIn("idx_shopify_sync_issues_shopify_sku", sql)
            self.assertIn("idx_shopify_sync_jobs_status", sql)
            self.assertIn("idx_inventory_items_shopify_inventory_item_id", sql)


if __name__ == "__main__":
    unittest.main()
