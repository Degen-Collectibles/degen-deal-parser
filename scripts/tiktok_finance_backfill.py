"""Backfill TikTok statements, fee breakdowns, payouts and returns.

Dry run by default: pulls everything and reports reconciliation, then rolls
back. With --apply it commits after each statement, so a rerun resumes and
skips statements that already reconcile.

  python scripts/tiktok_finance_backfill.py --since 2024-11-01
  python scripts/tiktok_finance_backfill.py --since 2024-11-01 --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlmodel import Session  # noqa: E402

from app.db import engine, init_db  # noqa: E402
from app.tiktok.tiktok_finance import build_finance_api, sync_tiktok_finance  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", default="2024-11-01", help="start date (YYYY-MM-DD, UTC)")
    parser.add_argument("--apply", action="store_true", help="write to the database (default: dry run)")
    args = parser.parse_args()
    start = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    if args.apply:
        init_db()  # creates the new tiktok_* finance tables if this deploy has not started yet
    with Session(engine) as session:
        api = build_finance_api(session)
        if api is None:
            print("TikTok credentials incomplete; nothing to do.")
            return 1
        summary = sync_tiktok_finance(
            session,
            api,
            start=start,
            end=now + timedelta(days=1),
            now=now,
            commit_each=session.commit if args.apply else None,
        )
        if args.apply:
            session.commit()
        else:
            session.rollback()

    unreconciled = summary.pop("unreconciled")
    errors = summary.pop("errors")
    print("mode:", "APPLIED" if args.apply else "dry run (rolled back)")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    print(f"  unreconciled statements: {len(unreconciled)}" + (f" -> {unreconciled[:20]}" if unreconciled else ""))
    print(f"  statements that errored (retried next sync): {len(errors)}")
    for error in errors[:10]:
        print(f"    {error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
