from sqlmodel import Session, SQLModel, create_engine, select

from app.models import BankFeedConnection, BankTransaction
from app.discord.plaid_bank_feed import sync_plaid_connection


def make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def test_plaid_sync_runs_ledger_agent_after_successful_sync(monkeypatch):
    engine = make_engine()

    def fake_plaid_post(path, payload, *, timeout=30.0):
        assert path == "/transactions/sync"
        return {
            "accounts": [
                {
                    "account_id": "acc_1",
                    "name": "Chase Checking",
                    "type": "depository",
                    "subtype": "checking",
                    "mask": "1234",
                    "balances": {"current": 1000.0, "available": 950.0, "iso_currency_code": "USD"},
                }
            ],
            "added": [
                {
                    "account_id": "acc_1",
                    "transaction_id": "plaid_tx_1",
                    "date": "2026-05-15",
                    "authorized_date": "2026-05-15",
                    "merchant_name": "Amazon Prime Video",
                    "name": "Amazon Prime Video",
                    "amount": 11.99,
                    "category": ["Entertainment"],
                    "payment_channel": "online",
                    "personal_finance_category": {
                        "primary": "ENTERTAINMENT",
                        "detailed": "ENTERTAINMENT_VIDEO",
                    },
                }
            ],
            "modified": [],
            "removed": [],
            "next_cursor": "cursor_after_sync",
            "has_more": False,
        }

    monkeypatch.setattr("app.discord.plaid_bank_feed.decrypt_access_token", lambda _blob: "access-token")
    monkeypatch.setattr("app.discord.plaid_bank_feed._plaid_post", fake_plaid_post)

    with Session(engine) as session:
        connection = BankFeedConnection(
            provider="plaid",
            provider_item_id="item_1",
            access_token_enc=b"encrypted",
            institution_name="Chase",
            status="active",
        )
        session.add(connection)
        session.commit()
        session.refresh(connection)

        result = sync_plaid_connection(session, connection.id or 0)
        row = session.exec(select(BankTransaction).where(BankTransaction.provider_transaction_id == "plaid_tx_1")).one()

    assert result["added"] == 1
    assert result["ledger_agent"]["scanned_count"] == 1
    assert result["ledger_agent"]["auto_reviewed"] == 0
    assert row.expense_category == "meals_entertainment"
    assert row.review_status == "open"


def test_plaid_sync_dedupes_relinked_account_transactions(monkeypatch):
    engine = make_engine()
    responses = [
        {
            "accounts": [
                {
                    "account_id": "acc_old",
                    "name": "Chase Ultimate Rewards",
                    "official_name": "Chase Ultimate Rewards 2024",
                    "type": "credit",
                    "subtype": "credit card",
                    "mask": "2024",
                    "balances": {"current": 100.0, "available": 900.0, "iso_currency_code": "USD"},
                }
            ],
            "added": [
                {
                    "account_id": "acc_old",
                    "transaction_id": "old_plaid_tx",
                    "date": "2026-05-25",
                    "authorized_date": "2026-05-25",
                    "merchant_name": "Taco Bell",
                    "name": "Taco Bell",
                    "amount": 10.37,
                    "category": ["Food and Drink", "Restaurants"],
                    "payment_channel": "in store",
                    "personal_finance_category": {
                        "primary": "FOOD_AND_DRINK",
                        "detailed": "FOOD_AND_DRINK_FAST_FOOD",
                    },
                }
            ],
            "modified": [],
            "removed": [],
            "next_cursor": "cursor_one",
            "has_more": False,
        },
        {
            "accounts": [
                {
                    "account_id": "acc_new",
                    "name": "Chase Ultimate Rewards",
                    "official_name": "Chase Ultimate Rewards 2024",
                    "type": "credit",
                    "subtype": "credit card",
                    "mask": "2024",
                    "balances": {"current": 100.0, "available": 900.0, "iso_currency_code": "USD"},
                }
            ],
            "added": [
                {
                    "account_id": "acc_new",
                    "transaction_id": "new_plaid_tx",
                    "date": "2026-05-25",
                    "authorized_date": "2026-05-25",
                    "merchant_name": "Taco Bell",
                    "name": "Taco Bell",
                    "amount": 10.37,
                    "category": ["Food and Drink", "Restaurants"],
                    "payment_channel": "in store",
                    "personal_finance_category": {
                        "primary": "FOOD_AND_DRINK",
                        "detailed": "FOOD_AND_DRINK_FAST_FOOD",
                    },
                }
            ],
            "modified": [],
            "removed": [],
            "next_cursor": "cursor_two",
            "has_more": False,
        },
    ]

    def fake_plaid_post(path, payload, *, timeout=30.0):
        assert path == "/transactions/sync"
        return responses.pop(0)

    monkeypatch.setattr("app.discord.plaid_bank_feed.decrypt_access_token", lambda _blob: "access-token")
    monkeypatch.setattr("app.discord.plaid_bank_feed._plaid_post", fake_plaid_post)

    with Session(engine) as session:
        first = BankFeedConnection(
            provider="plaid",
            provider_item_id="item_old",
            access_token_enc=b"encrypted",
            institution_name="Chase",
            status="active",
        )
        second = BankFeedConnection(
            provider="plaid",
            provider_item_id="item_new",
            access_token_enc=b"encrypted",
            institution_name="Chase",
            status="active",
        )
        session.add(first)
        session.add(second)
        session.commit()
        session.refresh(first)
        session.refresh(second)

        first_result = sync_plaid_connection(session, first.id or 0)
        second_result = sync_plaid_connection(session, second.id or 0)
        rows = session.exec(select(BankTransaction).where(BankTransaction.is_removed == False)).all()

    assert first_result["added"] == 1
    assert second_result["added"] == 0
    assert [row.description for row in rows] == ["Taco Bell"]


def _chase_checking_account(account_id="acc_chk"):
    return {
        "account_id": account_id,
        "name": "Chase BUS COMPLETE CHK",
        "official_name": "Chase BUS COMPLETE CHK",
        "type": "depository",
        "subtype": "checking",
        "mask": "3833",
        "balances": {"current": 1000.0, "available": 1000.0, "iso_currency_code": "USD"},
    }


def _tiktok_deposit(transaction_id, *, pending, pending_transaction_id=None, account_id="acc_chk"):
    return {
        "account_id": account_id,
        "transaction_id": transaction_id,
        "pending_transaction_id": pending_transaction_id,
        "pending": pending,
        "date": "2026-07-14",
        "authorized_date": "2026-07-14",
        "merchant_name": "Tiktok Shop",
        "name": "Tiktok Shop",
        "amount": -2140.09,
        "category": ["Transfer", "Deposit"],
        "payment_channel": "other",
    }


def _add_connections(monkeypatch, session, responses, *, item_ids=("item_chk",)):
    def fake_plaid_post(path, payload, *, timeout=30.0):
        assert path == "/transactions/sync"
        return responses.pop(0)

    monkeypatch.setattr("app.discord.plaid_bank_feed.decrypt_access_token", lambda _blob: "access-token")
    monkeypatch.setattr("app.discord.plaid_bank_feed._plaid_post", fake_plaid_post)
    connection_ids = []
    for item_id in item_ids:
        connection = BankFeedConnection(
            provider="plaid", provider_item_id=item_id, access_token_enc=b"encrypted",
            institution_name="Chase", status="active",
        )
        session.add(connection)
        session.commit()
        session.refresh(connection)
        connection_ids.append(connection.id or 0)
    return connection_ids


def _sync_response(account, added=(), removed=()):
    return {
        "accounts": [account],
        "added": list(added),
        "modified": [],
        "removed": [{"transaction_id": tx} for tx in removed],
        "next_cursor": "cursor",
        "has_more": False,
    }


def test_plaid_sync_keeps_posted_row_when_its_pending_row_is_removed(monkeypatch):
    # Regression (prod, May-Sep 2026): the posted version of a transaction was
    # dropped as a "duplicate" of its own pending row, then Plaid removed the
    # pending row, so the deposit vanished from the app entirely.
    engine = make_engine()
    account = _chase_checking_account()
    responses = [
        _sync_response(account, added=[_tiktok_deposit("pending_tx", pending=True)]),
        _sync_response(
            account,
            added=[_tiktok_deposit("posted_tx", pending=False, pending_transaction_id="pending_tx")],
            removed=["pending_tx"],
        ),
    ]
    with Session(engine) as session:
        (connection_id,) = _add_connections(monkeypatch, session, responses)
        sync_plaid_connection(session, connection_id)
        sync_plaid_connection(session, connection_id)
        live = session.exec(select(BankTransaction).where(BankTransaction.is_removed == False)).all()  # noqa: E712

    assert [(row.provider_transaction_id, row.pending, round(row.amount, 2)) for row in live] == [("posted_tx", False, 2140.09)]


def test_plaid_sync_still_skips_other_connections_posted_copy(monkeypatch):
    # Two Plaid links to the same Chase account must not double count.
    engine = make_engine()
    responses = [
        _sync_response(_chase_checking_account("acc_a"), added=[_tiktok_deposit("posted_a", pending=False, account_id="acc_a")]),
        _sync_response(_chase_checking_account("acc_b"), added=[_tiktok_deposit("posted_b", pending=False, account_id="acc_b")]),
    ]
    with Session(engine) as session:
        first_id, second_id = _add_connections(monkeypatch, session, responses, item_ids=("item_a", "item_b"))
        sync_plaid_connection(session, first_id)
        sync_plaid_connection(session, second_id)
        live = session.exec(select(BankTransaction).where(BankTransaction.is_removed == False)).all()  # noqa: E712

    assert [row.provider_transaction_id for row in live] == ["posted_a"]
