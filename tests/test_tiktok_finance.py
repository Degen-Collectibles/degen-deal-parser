from datetime import datetime, timedelta, timezone

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.models import TikTokPayment, TikTokReturn, TikTokStatement, TikTokStatementTransaction
from app.tiktok import tiktok_finance as tf

NOW = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)


def make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def statement(statement_id="S1", *, days_ago=2, settlement="90.00", status="PAID"):
    return {
        "id": statement_id, "statement_time": int((NOW - timedelta(days=days_ago)).timestamp()), "currency": "USD",
        "revenue_amount": "100.00", "fee_amount": "-10.00", "shipping_cost_amount": "0.00", "adjustment_amount": "0.00",
        "net_sales_amount": "100.00", "settlement_amount": settlement, "payment_id": "P1", "payment_status": status,
        "payment_time": int(NOW.timestamp()),
    }


def transaction(tx_id, settlement, order_id="O1"):
    return {
        "id": tx_id, "type": "ORDER", "order_id": order_id, "order_create_time": int((NOW - timedelta(days=3)).timestamp()),
        "revenue_amount": "50.00", "fee_tax_amount": "-5.00", "shipping_cost_amount": "0.00", "adjustment_amount": "0.00",
        "settlement_amount": settlement,
        "fee_tax_breakdown": {"fee": {"platform_commission_amount": "-3.00", "affiliate_commission_amount": "-2.00"}, "tax": {}},
    }


class FakeApi:
    def __init__(self, *, statements=None, transactions=None, payments=None, returns=None, cancellations=None, pages=None):
        self.statements = statements or []
        self.transactions = transactions or {}
        self.payments = payments or []
        self.returns = returns or []
        self.cancellations = cancellations or []
        self.pages = pages or {}
        self.calls = []

    def call(self, method, path, *, query=None, body=None, version="202309"):
        self.calls.append(path)
        if path in self.pages:
            return self.pages[path][(query or {}).get("page_token", "")]
        if path == tf.STATEMENTS_PATH:
            return {"statements": self._in_window(self.statements, "statement_time", query)}
        if path.endswith("/statement_transactions"):
            statement_id = path.split("/")[-2]
            items = self.transactions.get(statement_id, [])
            return {"transactions": items, "total_count": len(items), "currency": "USD"}
        if path == tf.PAYMENTS_PATH:
            return {"payments": self._in_window(self.payments, "create_time", query)}
        if path == tf.RETURNS_SEARCH_PATH:
            return {"return_orders": self._in_window(self.returns, "update_time", body)}
        if path == tf.CANCELLATIONS_SEARCH_PATH:
            return {"cancellations": self._in_window(self.cancellations, "update_time", body)}
        raise AssertionError(path)

    @staticmethod
    def _in_window(records, time_field, params):
        # Like TikTok: return only records whose timestamp falls in [*_ge, *_lt).
        lo = next(value for key, value in params.items() if key.endswith("_ge"))
        hi = next(value for key, value in params.items() if key.endswith("_lt"))
        return [record for record in records if lo <= int(record[time_field]) < hi]


def run(session, api):
    return tf.sync_tiktok_finance(session, api, start=NOW - timedelta(days=40), end=NOW + timedelta(days=1), now=NOW)


def test_statement_reconciles_when_transactions_sum_to_settlement():
    api = FakeApi(statements=[statement()], transactions={"S1": [transaction("T1", "45.00"), transaction("T2", "45.00", "O2")]})
    with Session(make_engine()) as session:
        summary = run(session, api)
        session.commit()
        stored = session.exec(select(TikTokStatement)).one()
        txs = session.exec(select(TikTokStatementTransaction)).all()

    assert summary["reconciled"] == 1 and summary["unreconciled"] == []
    assert stored.reconciled and stored.transaction_settlement_sum == 90.0 and stored.fee_amount == -10.0
    assert {tx.order_id for tx in txs} == {"O1", "O2"}
    assert '"platform_commission_amount":"-3.00"' in txs[0].fee_breakdown_json


def test_statement_mismatch_is_flagged_and_rerun_is_idempotent():
    api = FakeApi(statements=[statement()], transactions={"S1": [transaction("T1", "45.00")]})
    engine = make_engine()
    with Session(engine) as session:
        first = run(session, api)
        session.commit()
        run(session, api)
        session.commit()
        assert session.exec(select(TikTokStatement)).one().reconciled is False
        assert len(session.exec(select(TikTokStatementTransaction)).all()) == 1

    assert first["unreconciled"] == ["S1"]


def test_old_paid_reconciled_statement_is_not_refetched_unless_total_changes():
    old = statement(days_ago=30)
    api = FakeApi(statements=[old], transactions={"S1": [transaction("T1", "90.00")]})
    with Session(make_engine()) as session:
        run(session, api)
        session.commit()
        api.calls.clear()
        run(session, api)
        assert not any(path.endswith("/statement_transactions") for path in api.calls)

        api.statements = [statement(days_ago=30, settlement="80.00")]
        api.transactions = {"S1": [transaction("T1", "80.00")]}
        api.calls.clear()
        run(session, api)
        assert any(path.endswith("/statement_transactions") for path in api.calls)
        assert session.exec(select(TikTokStatement)).one().reconciled is True


def test_payment_stores_only_last4_of_bank_account():
    api = FakeApi(payments=[{
        "id": "P1", "create_time": int(NOW.timestamp()), "paid_time": int(NOW.timestamp()), "status": "PAID",
        "amount": {"currency": "USD", "value": "89.11"}, "settlement_amount": {"currency": "USD", "value": "89.11"},
        "reserve_amount": {"currency": "USD", "value": "0"}, "bank_account": "0000123456783833",
    }])
    with Session(make_engine()) as session:
        run(session, api)
        payment = session.exec(select(TikTokPayment)).one()

    assert payment.amount == 89.11 and payment.bank_account_last4 == "3833"
    assert "0000123456783833" not in payment.raw_json and "****3833" in payment.raw_json


def test_returns_and_cancellations_are_stored_with_reason_and_refund():
    refund = {"currency": "USD", "refund_total": "27.50", "refund_subtotal": "25.00", "refund_tax": "2.50", "refund_shipping_fee": "0"}
    api = FakeApi(
        returns=[{"return_id": "R1", "order_id": "O9", "return_status": "RETURN_OR_REFUND_REQUEST_COMPLETE", "return_type": "REFUND",
                  "return_reason": "ecom_order_delivered_refund_reason_damaged", "return_reason_text": "Damaged", "role": "BUYER",
                  "refund_amount": refund, "create_time": int(NOW.timestamp()), "update_time": int(NOW.timestamp()),
                  "return_line_items": [{"product_name": "Booster Box"}]}],
        cancellations=[{"cancel_id": "C1", "order_id": "O8", "cancel_status": "CANCELLATION_REQUEST_COMPLETE", "cancel_type": "CANCEL",
                        "cancel_reason": "ecom_order_cancel_reason_changed_mind", "cancel_reason_text": "Changed mind", "role": "BUYER",
                        "refund_amount": refund, "create_time": int(NOW.timestamp()), "update_time": int(NOW.timestamp()),
                        "cancel_line_items": []}],
    )
    with Session(make_engine()) as session:
        run(session, api)
        rows = {row.record_key: row for row in session.exec(select(TikTokReturn)).all()}

    assert set(rows) == {"return:R1", "cancellation:C1"}
    assert rows["return:R1"].refund_total == 27.5 and rows["return:R1"].reason_text == "Damaged"
    assert rows["cancellation:C1"].initiated_by == "BUYER" and rows["cancellation:C1"].order_id == "O8"


def test_paging_follows_tokens_and_stops_on_repeated_token():
    api = FakeApi(pages={"/p": {"": {"items": [{"id": 1}], "next_page_token": "a"},
                                "a": {"items": [{"id": 2}], "next_page_token": "b"},
                                "b": {"items": [{"id": 3}], "next_page_token": "a"}}})
    items, _ = tf._paged(api, "GET", "/p", "items", query={})
    assert [item["id"] for item in items] == [1, 2, 3]


def test_api_error_code_raises():
    class ErrorHttp:
        def request(self, *args, **kwargs):
            class Response:
                status_code = 200

                @staticmethod
                def json():
                    return {"code": 105005, "message": "Access denied"}
            return Response()

    api = tf.TikTokFinanceApi(app_key="k", app_secret="s", access_token="t", shop_cipher="c", http=ErrorHttp(), pause_seconds=0)
    with pytest.raises(tf.TikTokFinanceError, match="105005"):
        api.call("GET", tf.STATEMENTS_PATH, query={"page_size": 1})
