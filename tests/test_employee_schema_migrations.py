from sqlalchemy import inspect, text
from sqlmodel import SQLModel, create_engine

from app import db


def test_sqlite_employee_email_migration_declarations():
    employee_migrations = db.SQLITE_ADDITIVE_MIGRATIONS["employeeprofile"]

    assert employee_migrations["email_ciphertext"] == "BLOB"
    assert employee_migrations["email_lookup_hash"] == "TEXT"


def test_postgres_employee_email_migration_declarations():
    employee_migrations = db.POSTGRES_ADDITIVE_MIGRATIONS["employeeprofile"]

    assert employee_migrations["email_ciphertext"] == "BYTEA"
    assert employee_migrations["email_lookup_hash"] == "TEXT"


def test_employee_email_lookup_unique_index_is_declared_for_both_engines():
    expected_statement = (
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_employeeprofile_email_lookup_hash "
        "ON employeeprofile (email_lookup_hash)"
    )

    assert expected_statement in db.SQLITE_INDEX_MIGRATIONS
    assert expected_statement in db.POSTGRES_INDEX_MIGRATIONS


def test_ensure_sqlite_schema_upgrades_legacy_employeeprofile_table(tmp_path):
    test_engine = create_engine(
        f"sqlite:///{(tmp_path / 'legacy-employee.sqlite3').as_posix()}",
        connect_args={"check_same_thread": False},
    )
    original_engine = db.engine
    original_database_url = db.database_url

    try:
        SQLModel.metadata.create_all(test_engine)
        with test_engine.begin() as connection:
            connection.execute(text("DROP TABLE employeeprofile"))
            connection.execute(
                text("CREATE TABLE employeeprofile (user_id INTEGER PRIMARY KEY)")
            )

        db.engine = test_engine
        db.database_url = str(test_engine.url)
        db.ensure_sqlite_schema()

        inspector = inspect(test_engine)
        columns = {
            column["name"]: str(column["type"]).upper()
            for column in inspector.get_columns("employeeprofile")
        }
        indexes = {
            index["name"]: index for index in inspector.get_indexes("employeeprofile")
        }

        assert columns["email_ciphertext"] == "BLOB"
        assert columns["email_lookup_hash"] == "TEXT"
        assert indexes["ix_employeeprofile_email_lookup_hash"]["column_names"] == [
            "email_lookup_hash"
        ]
        assert indexes["ix_employeeprofile_email_lookup_hash"]["unique"]
    finally:
        db.engine = original_engine
        db.database_url = original_database_url
        test_engine.dispose()
