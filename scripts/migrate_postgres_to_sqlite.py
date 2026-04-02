from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy import MetaData, create_engine, select, text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.models import SQLModel  # noqa: F401 - ensure metadata is populated


DEFAULT_SQLITE_PATH = ROOT_DIR / "data" / "degen_live.db"


def build_sqlite_url() -> str:
    sqlite_path = Path(os.getenv("SQLITE_PATH", str(DEFAULT_SQLITE_PATH))).resolve()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{sqlite_path.as_posix()}"


def build_postgres_url() -> str:
    raw_url = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL", "")
    if not raw_url:
        raise RuntimeError("POSTGRES_URL or DATABASE_URL is required")
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    if raw_url.startswith("postgresql://") and "+psycopg" not in raw_url:
        return raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw_url


def clean_value(value):
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


def clean_row(row_mapping):
    return {key: clean_value(value) for key, value in row_mapping.items()}


def main() -> None:
    postgres_url = build_postgres_url()
    sqlite_url = build_sqlite_url()

    postgres_engine = create_engine(postgres_url, pool_pre_ping=True)
    sqlite_engine = create_engine(sqlite_url)

    SQLModel.metadata.create_all(sqlite_engine)

    metadata = MetaData()
    metadata.reflect(bind=postgres_engine)
    table_names = [table.name for table in SQLModel.metadata.sorted_tables]

    with postgres_engine.connect() as postgres_connection, sqlite_engine.begin() as sqlite_connection:
        sqlite_connection.execute(text("PRAGMA foreign_keys=OFF"))

        for table_name in reversed(table_names):
            if table_name not in metadata.tables:
                continue
            sqlite_connection.execute(text(f'DELETE FROM "{table_name}"'))

        for table_name in table_names:
            table = metadata.tables.get(table_name)
            if table is None:
                continue

            rows = postgres_connection.execute(select(table)).mappings().all()
            if not rows:
                print(f"{table_name}: 0 rows")
                continue

            sqlite_connection.execute(table.insert(), [clean_row(row) for row in rows])
            print(f"{table_name}: {len(rows)} rows")

        sqlite_connection.execute(text("PRAGMA foreign_keys=ON"))

    print("Migration complete.")


if __name__ == "__main__":
    main()
