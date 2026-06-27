import unittest

from sqlalchemy import inspect, text
from sqlmodel import SQLModel, create_engine

from app import db
from app.models import InventoryStockMovement, ShopifySyncIssue, ShopifySyncJob


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

            movement_columns = {
                col["name"]: col for col in inspector.get_columns("inventory_stock_movements")
            }
            self.assertIn("dedupe_key", movement_columns)
            self.assertTrue(movement_columns["dedupe_key"]["nullable"])
            self.assertIn("requested_quantity", movement_columns)
            self.assertTrue(movement_columns["requested_quantity"]["nullable"])
            movement_indexes = {
                index["name"]: index
                for index in inspector.get_indexes("inventory_stock_movements")
            }
            self.assertIn("idx_inventory_stock_movements_dedupe_key", movement_indexes)
            self.assertTrue(movement_indexes["idx_inventory_stock_movements_dedupe_key"]["unique"])
        finally:
            engine.dispose()

    def test_stock_movement_model_declares_nullable_unique_dedupe_index(self):
        dedupe_column = InventoryStockMovement.__table__.c.dedupe_key
        requested_quantity_column = InventoryStockMovement.__table__.c.requested_quantity
        self.assertTrue(dedupe_column.nullable)
        self.assertTrue(requested_quantity_column.nullable)
        indexes = {
            index.name: index for index in InventoryStockMovement.__table__.indexes
        }
        self.assertIn("idx_inventory_stock_movements_dedupe_key", indexes)
        self.assertTrue(indexes["idx_inventory_stock_movements_dedupe_key"].unique)

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

    def test_additive_migrations_cover_bank_row_dedupe_key(self):
        self.assertIn("row_dedupe_key", db.SQLITE_ADDITIVE_MIGRATIONS["bank_transactions"])
        self.assertIn("row_dedupe_key", db.POSTGRES_ADDITIVE_MIGRATIONS["bank_transactions"])

    def test_additive_migrations_cover_stock_movement_dedupe_key(self):
        self.assertEqual(
            db.SQLITE_ADDITIVE_MIGRATIONS["inventory_stock_movements"]["dedupe_key"],
            "TEXT",
        )
        self.assertEqual(
            db.POSTGRES_ADDITIVE_MIGRATIONS["inventory_stock_movements"]["dedupe_key"],
            "TEXT",
        )
        self.assertEqual(
            db.SQLITE_ADDITIVE_MIGRATIONS["inventory_stock_movements"]["requested_quantity"],
            "INTEGER",
        )
        self.assertEqual(
            db.POSTGRES_ADDITIVE_MIGRATIONS["inventory_stock_movements"]["requested_quantity"],
            "INTEGER",
        )

    def test_index_migrations_cover_shopify_sync_lookup_paths(self):
        sqlite_indexes = "\n".join(db.SQLITE_INDEX_MIGRATIONS)
        postgres_indexes = "\n".join(db.POSTGRES_INDEX_MIGRATIONS)
        for sql in (sqlite_indexes, postgres_indexes):
            self.assertIn("idx_shopify_sync_issues_issue_key", sql)
            self.assertIn("idx_shopify_sync_issues_shopify_sku", sql)
            self.assertIn("idx_shopify_sync_jobs_status", sql)
            self.assertIn("idx_inventory_items_shopify_inventory_item_id", sql)
            self.assertIn(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_stock_movements_dedupe_key",
                sql,
            )


def test_ensure_sqlite_schema_upgrades_legacy_stock_movement_table(tmp_path):
    test_engine = create_engine(
        f"sqlite:///{(tmp_path / 'legacy-stock-movement.sqlite3').as_posix()}",
        connect_args={"check_same_thread": False},
    )
    original_engine = db.engine
    original_database_url = db.database_url

    try:
        SQLModel.metadata.create_all(test_engine)
        with test_engine.begin() as connection:
            connection.execute(text("DROP TABLE inventory_stock_movements"))
            connection.execute(
                text(
                    """
                    CREATE TABLE inventory_stock_movements (
                        id INTEGER PRIMARY KEY,
                        item_id INTEGER NOT NULL,
                        reason TEXT NOT NULL DEFAULT 'receive',
                        quantity_delta INTEGER NOT NULL,
                        quantity_before INTEGER NOT NULL,
                        quantity_after INTEGER NOT NULL,
                        unit_cost REAL,
                        total_cost REAL,
                        location TEXT,
                        source TEXT,
                        notes TEXT,
                        created_by TEXT,
                        created_at TIMESTAMP NOT NULL
                    )
                    """
                )
            )

        db.engine = test_engine
        db.database_url = str(test_engine.url)
        db.ensure_sqlite_schema()

        inspector = inspect(test_engine)
        columns = {
            column["name"]: column
            for column in inspector.get_columns("inventory_stock_movements")
        }
        indexes = {
            index["name"]: index
            for index in inspector.get_indexes("inventory_stock_movements")
        }

        assert columns["dedupe_key"]["nullable"]
        assert columns["requested_quantity"]["nullable"]
        assert indexes["idx_inventory_stock_movements_dedupe_key"]["column_names"] == [
            "dedupe_key"
        ]
        assert indexes["idx_inventory_stock_movements_dedupe_key"]["unique"]
    finally:
        db.engine = original_engine
        db.database_url = original_database_url
        test_engine.dispose()


if __name__ == "__main__":
    unittest.main()
