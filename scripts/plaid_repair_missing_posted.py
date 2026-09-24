"""Restore posted Plaid transactions that the sync dropped (May-Sep 2026 bug).

Until the fix in app/discord/bank_reconciliation.py, a transaction's posted
version was discarded as a duplicate of its own pending row; Plaid then removed
the pending row and the transaction disappeared. The sync cursor has moved on,
so this script re-reads history with /transactions/get (read-only on Plaid's
side) and inserts posted transactions whose Plaid id is not stored yet.

Dry run by default: no database writes, prints what would be restored.
  python scripts/plaid_repair_missing_posted.py --since 2026-05-01
  python scripts/plaid_repair_missing_posted.py --since 2026-05-01 --apply
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlmodel import Session, select  # noqa: E402

from app.db import engine  # noqa: E402
from app.discord.plaid_bank_feed import (  # noqa: E402
    _apply_payloads,
    _next_row_index,
    _payload_from_plaid_transaction,
    _plaid_post,
    decrypt_access_token,
)
from app.models import BankFeedAccount, BankFeedConnection, BankStatementImport, BankTransaction  # noqa: E402


def fetch_posted_transactions(access_token: str, start: date, end: date) -> list[dict[str, Any]]:
    transactions: list[dict[str, Any]] = []
    offset = 0
    while True:
        data = _plaid_post(
            "/transactions/get",
            {
                "access_token": access_token,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "options": {"count": 500, "offset": offset},
            },
            timeout=60.0,
        )
        page = list(data.get("transactions") or [])
        transactions.extend(page)
        offset += len(page)
        if not page or offset >= int(data.get("total_transactions") or 0):
            break
    return [txn for txn in transactions if not txn.get("pending")]


def live_posted_copy_exists(session: Session, payload: dict[str, Any]) -> bool:
    """True when another Plaid link already stored this posted transaction."""
    posted_at = payload.get("posted_at")
    if posted_at is None:
        return False
    rows = session.exec(
        select(BankTransaction).where(
            BankTransaction.account_label == payload["account_label"],
            BankTransaction.amount == payload["amount"],
            BankTransaction.is_removed == False,  # noqa: E712
            BankTransaction.pending == False,  # noqa: E712
        )
    ).all()
    return any(row.posted_at is not None and row.posted_at.date() == posted_at.date() for row in rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", default="2026-05-01", help="first posted date to re-read (YYYY-MM-DD)")
    parser.add_argument("--apply", action="store_true", help="write missing rows (default: dry run)")
    args = parser.parse_args()
    start = date.fromisoformat(args.since)
    end = date.today() + timedelta(days=1)

    summary: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0, 0.0, 0, 0.0])
    inserted_total = 0
    with Session(engine) as session:
        connections = session.exec(
            select(BankFeedConnection).where(BankFeedConnection.provider == "plaid").order_by(BankFeedConnection.id)
        ).all()
        # Dry run tracks what it would add so the second link's copy is not counted twice.
        planned: set[tuple[str, str, float]] = set()
        for connection in connections:
            if connection.status != "active":
                continue
            access_token = decrypt_access_token(connection.access_token_enc)
            posted = fetch_posted_transactions(access_token, start, end)
            known_ids = set(
                session.exec(
                    select(BankTransaction.provider_transaction_id).where(
                        BankTransaction.provider_transaction_id.in_([txn["transaction_id"] for txn in posted])
                    )
                ).all()
            ) if posted else set()
            accounts = {
                account.provider_account_id: account.bank_import_id
                for account in session.exec(
                    select(BankFeedAccount).where(BankFeedAccount.connection_id == connection.id)
                ).all()
            }
            by_import: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for txn in posted:
                if txn["transaction_id"] in known_ids:
                    continue
                import_id = accounts.get(str(txn.get("account_id") or ""))
                import_row = session.get(BankStatementImport, import_id) if import_id else None
                if import_row is None:
                    continue
                payload = _payload_from_plaid_transaction(txn=txn, import_row=import_row, row_index=0)
                key = (payload["account_label"], payload["posted_at"].date().isoformat(), payload["amount"])
                if live_posted_copy_exists(session, payload) or key in planned:
                    continue
                planned.add(key)
                by_import[import_row.id or 0].append(payload)
                bucket = summary[(payload["posted_at"].strftime("%Y-%m"), payload["account_label"])]
                if payload["amount"] > 0:
                    bucket[0] += 1
                    bucket[1] += payload["amount"]
                else:
                    bucket[2] += 1
                    bucket[3] += payload["amount"]
            if args.apply:
                for import_id, payloads in by_import.items():
                    import_row = session.get(BankStatementImport, import_id)
                    next_index = _next_row_index(session, import_id)
                    for offset, payload in enumerate(payloads):
                        payload["row_index"] = next_index + offset
                    inserted, _updated = _apply_payloads(session, import_row, payloads)
                    inserted_total += inserted

    print(f"{'month':8} {'account':30} {'deposits':>9} {'deposit $':>12} {'debits':>7} {'debit $':>12}")
    for (month, account), (dep_n, dep_sum, deb_n, deb_sum) in sorted(summary.items()):
        print(f"{month:8} {account[:30]:30} {dep_n:9d} {dep_sum:12,.2f} {deb_n:7d} {deb_sum:12,.2f}")
    print("mode:", "APPLIED" if args.apply else "dry run (no writes)", "| inserted:", inserted_total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
