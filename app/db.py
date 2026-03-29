from pathlib import Path
from sqlalchemy import text
from sqlmodel import SQLModel, Session, create_engine
from .config import get_settings

settings = get_settings()
database_url = settings.database_url

if database_url.startswith("sqlite:///"):
    db_path = database_url.replace("sqlite:///", "", 1)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}

engine = create_engine(
    database_url,
    echo=False,
    connect_args=connect_args,
)


def ensure_sqlite_columns() -> None:
    if not database_url.startswith("sqlite"):
        return

    required_columns = {
        "discordmessage": {
            "entry_kind": "TEXT",
            "money_in": "REAL",
            "money_out": "REAL",
            "expense_category": "TEXT",
        },
        "watchedchannel": {
            "backfill_enabled": "BOOLEAN DEFAULT 1",
        },
    }

    with engine.begin() as connection:
        for table_name, columns in required_columns.items():
            existing = {
                row[1]
                for row in connection.execute(text(f"PRAGMA table_info({table_name})"))
            }
            for column_name, column_type in columns.items():
                if column_name in existing:
                    continue
                connection.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"ADD COLUMN {column_name} {column_type}"
                    )
                )


def init_db() -> None:
    SQLModel.metadata.create_all(engine)
    ensure_sqlite_columns()

def get_session():
    with Session(engine) as session:
        yield session
