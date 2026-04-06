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
    sqlite_path = os.getenv("SQLITE_PATH", str(DEFAULT_SQLITE_PATH))
    return f"sqlite:///{Path(sqlite_path).resolve().as_posix()}"


def build_postgres_url() -> str:
    raw_url = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL", "")
    if not raw_url:
        raise RuntimeError("POSTGRES_URL or DATABASE_URL is required")
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    if raw_url.startswith("postgresql://") and "+psycopg" not in raw_url:
        return raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw_url


def reset_postgres_sequences(connection, table_name: str) -> None:
    quoted_table_name = table_name.replace('"', '""')
    connection.execute(
        text(
            f"""
            SELECT setval(
                pg_get_serial_sequence(:table_name, 'id'),
                COALESCE((SELECT MAX(id) FROM "{quoted_table_name}"), 1),
                COALESCE((SELECT MAX(id) FROM "{quoted_table_name}"), 0) > 0
            )
            """
        ),
        {"table_name": table_name},
    )


def clean_value(value):
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


def clean_row(row_mapping):
    return {key: clean_value(value) for key, value in row_mapping.items()}


BATCH_SIZE = 500


def main() -> None:
    sqlite_url = build_sqlite_url()
    postgres_url = build_postgres_url()

    sqlite_engine = create_engine(sqlite_url)
    postgres_engine = create_engine(
        postgres_url,
        pool_pre_ping=True,
        connect_args={"options": "-c timezone=UTC"},
    )

    SQLModel.metadata.create_all(postgres_engine)

    metadata = MetaData()
    metadata.reflect(bind=sqlite_engine)

    table_names = [table.name for table in SQLModel.metadata.sorted_tables]

    with postgres_engine.begin() as conn:
        for table_name in reversed(table_names):
            if table_name not in metadata.tables:
                continue
            conn.execute(text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE'))
    print("Tables truncated.")

    with sqlite_engine.connect() as sqlite_connection:
        for table_name in table_names:
            table = metadata.tables.get(table_name)
            if table is None:
                continue

            rows = sqlite_connection.execute(select(table)).mappings().all()
            if not rows:
                print(f"{table_name}: 0 rows")
                continue

            cleaned = [clean_row(row) for row in rows]
            inserted = 0
            for i in range(0, len(cleaned), BATCH_SIZE):
                batch = cleaned[i : i + BATCH_SIZE]
                with postgres_engine.begin() as conn:
                    conn.execute(table.insert(), batch)
                inserted += len(batch)
                if len(cleaned) > BATCH_SIZE:
                    print(f"  {table_name}: {inserted}/{len(cleaned)} rows...", flush=True)

            with postgres_engine.begin() as conn:
                if "id" in table.c:
                    reset_postgres_sequences(conn, table_name)

            print(f"{table_name}: {len(cleaned)} rows")

    print("Migration complete.")
    sqlite_engine.dispose()
    postgres_engine.dispose()


if __name__ == "__main__":
    main()
