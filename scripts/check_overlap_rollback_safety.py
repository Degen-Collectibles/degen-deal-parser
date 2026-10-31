"""Read-only gate before AND after rollback to the legacy overlap-warning reader.

On Green, retain this file outside /opt/degen/app before reverting, then run it
with /opt/degen/app/.venv/bin/python before and after the rollback deployment.
Exit 2 means warning evidence would be hidden; other errors also block rollback.
Keep or roll forward to the persistent reader, or use a backed-up audited data
correction to restore marker/context and rerun. This script never changes data.
"""
import json

from dotenv import dotenv_values
from sqlalchemy import create_engine, text


def find_rollback_blockers(connection):
    return list(connection.execute(text(
        "SELECT id FROM bank_transactions WHERE discord_overlap_unresolved IS TRUE "
        "AND COALESCE(match_override_note,'') NOT LIKE '%[unresolved-discord-overlap]%' ORDER BY id"
    )).scalars())


def main():
    engine = create_engine(dotenv_values('/opt/degen/web.env')['DATABASE_URL'],
        connect_args={'options': '-c default_transaction_read_only=on -c statement_timeout=60000'})
    try:
        with engine.connect() as connection:
            ids = find_rollback_blockers(connection)
        print(json.dumps({'safe_for_legacy_warning_reader': not ids, 'blocking_bank_ids': ids}))
        return 2 if ids else 0
    finally:
        engine.dispose()


if __name__ == '__main__':
    raise SystemExit(main())
