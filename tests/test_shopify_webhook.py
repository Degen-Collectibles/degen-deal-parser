import asyncio
import base64
import hashlib
import hmac
import json
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select
from starlette.requests import Request

from app.models import (
    INVENTORY_IN_STOCK,
    INVENTORY_SOLD,
    InventoryItem,
    InventoryStockMovement,
    ShopifyOrder,
    ShopifySyncIssue,
    ShopifySyncJob,
)
from app.routers import shopify as shopify_module


WEBHOOK_SECRET = "test-shopify-webhook-secret"
MISSING_ORDER_ID = object()
MISSING_FINANCIAL_STATUS = object()


def _signature(body: bytes) -> str:
    digest = hmac.new(WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _request(
    body: bytes,
    *,
    signature: str | None = None,
    topic: str = "orders/paid",
) -> Request:
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    headers = [
        (b"x-shopify-hmac-sha256", (signature or _signature(body)).encode("ascii")),
        (b"x-shopify-topic", topic.encode("ascii")),
        (b"content-type", b"application/json"),
    ]
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/webhooks/shopify/orders",
            "raw_path": b"/webhooks/shopify/orders",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 443),
        },
        receive,
    )


async def _inline_to_thread(function, /, *args, **kwargs):
    return function(*args, **kwargs)


def _configure_inline_writes(monkeypatch) -> list[tuple[str, object]]:
    invocations = []

    def run_inline(write_operation):
        session = object()
        invocations.append((write_operation.__name__, session))
        return write_operation(session)

    monkeypatch.setattr(shopify_module.asyncio, "to_thread", _inline_to_thread)
    monkeypatch.setattr(shopify_module, "run_write_with_retry", run_inline)
    return invocations


def _database_engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _structured_logs_from(output: str) -> list[dict]:
    records = []
    for line in output.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("action"):
            records.append(record)
    return records


def _structured_logs(capsys) -> list[dict]:
    return _structured_logs_from(capsys.readouterr().out)


def _payload(*, financial_status: str = "paid") -> dict:
    return {
        "id": 123456789,
        "name": "#1042",
        "financial_status": financial_status,
        "line_items": [
            {
                "id": 7,
                "sku": "DGN-000042",
                "quantity": 1,
                "price": "24.99",
            }
        ],
    }


def test_invalid_hmac_remains_unauthorized_and_does_not_write(monkeypatch):
    body = json.dumps(_payload()).encode("utf-8")
    upsert = Mock()
    mark_inventory = Mock()
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        mark_inventory,
    )

    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            shopify_module.shopify_orders_webhook(
                _request(body, signature="definitely-not-a-valid-signature")
            )
        )

    assert caught.value.status_code == 401
    assert caught.value.detail == "Invalid Shopify webhook signature"
    upsert.assert_not_called()
    mark_inventory.assert_not_called()


@pytest.mark.parametrize(
    ("body", "expected_error_type"),
    [
        (b"\xff\xfe", "UnicodeDecodeError"),
        (b'{"id": 123', "JSONDecodeError"),
        (b"[]", "ValueError"),
    ],
)
def test_signed_malformed_payload_returns_400_without_writes(
    monkeypatch,
    capsys,
    body,
    expected_error_type,
):
    upsert = Mock()
    mark_inventory = Mock()
    run_write = Mock()
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        mark_inventory,
    )
    monkeypatch.setattr(shopify_module, "run_write_with_retry", run_write)

    with pytest.raises(HTTPException) as caught:
        asyncio.run(shopify_module.shopify_orders_webhook(_request(body)))

    assert caught.value.status_code == 400
    assert caught.value.detail == "Invalid Shopify payload"
    assert WEBHOOK_SECRET not in str(caught.value.detail)
    run_write.assert_not_called()
    upsert.assert_not_called()
    mark_inventory.assert_not_called()
    failed = next(
        record
        for record in _structured_logs(capsys)
        if record["action"] == "shopify.webhook.failed"
    )
    assert failed["error"] == "Invalid Shopify webhook payload"
    assert failed["error_type"] == expected_error_type


@pytest.mark.parametrize(
    "order_id",
    [MISSING_ORDER_ID, None, "", "   ", True, False],
    ids=["missing", "null", "empty", "whitespace", "true", "false"],
)
def test_signed_object_without_usable_order_id_records_issue_then_returns_400_without_upsert(
    monkeypatch,
    order_id,
):
    payload = _payload()
    if order_id is MISSING_ORDER_ID:
        payload.pop("id")
    else:
        payload["id"] = order_id
    body = json.dumps(payload).encode("utf-8")
    write_invocations = _configure_inline_writes(monkeypatch)
    upsert = Mock()
    mark_inventory = Mock(return_value=0)
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        mark_inventory,
    )

    with pytest.raises(HTTPException) as caught:
        asyncio.run(shopify_module.shopify_orders_webhook(_request(body)))

    assert caught.value.status_code == 400
    assert caught.value.detail == "Invalid Shopify payload"
    upsert.assert_not_called()
    assert [name for name, _session in write_invocations] == [
        "_record_invalid_order_identity"
    ]
    assert mark_inventory.call_args.args[0] is write_invocations[0][1]
    issue_payload = mark_inventory.call_args.args[1]
    assert issue_payload["financial_status"] == "paid"
    if order_id is MISSING_ORDER_ID:
        assert "id" not in issue_payload
    else:
        assert issue_payload["id"] == order_id


@pytest.mark.parametrize(
    "financial_status",
    ["pending", MISSING_FINANCIAL_STATUS],
    ids=["pending", "absent"],
)
def test_invalid_identity_route_records_real_issue_before_nonpaid_status_handling(
    monkeypatch,
    financial_status,
):
    engine = _database_engine()
    try:
        payload = _payload(financial_status="pending")
        payload.pop("id")
        if financial_status is MISSING_FINANCIAL_STATUS:
            payload.pop("financial_status")
        else:
            payload["financial_status"] = financial_status
        body = json.dumps(payload).encode("utf-8")
        upsert = Mock()

        def run_with_database_session(write_operation):
            with Session(engine) as session:
                return write_operation(session)

        monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
        monkeypatch.setattr(shopify_module.asyncio, "to_thread", _inline_to_thread)
        monkeypatch.setattr(
            shopify_module,
            "run_write_with_retry",
            run_with_database_session,
        )
        monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)

        for _attempt in range(2):
            with pytest.raises(HTTPException) as caught:
                asyncio.run(shopify_module.shopify_orders_webhook(_request(body)))
            assert caught.value.status_code == 400
            assert caught.value.detail == "Invalid Shopify payload"

        upsert.assert_not_called()
        with Session(engine) as session:
            issues = session.exec(select(ShopifySyncIssue)).all()
            assert len(issues) == 1
            assert issues[0].issue_type == "invalid_order_identity"
            assert issues[0].occurrence_count == 2
            assert session.exec(select(InventoryStockMovement)).all() == []
            assert session.exec(select(ShopifySyncJob)).all() == []
    finally:
        engine.dispose()


def test_invalid_order_identity_issue_failure_returns_sanitized_503_without_upsert(
    monkeypatch,
    capsys,
):
    payload = _payload()
    payload.pop("id")
    body = json.dumps(payload).encode("utf-8")
    secret_error = "identity issue password=do-not-leak"
    write_invocations = _configure_inline_writes(monkeypatch)
    upsert = Mock(return_value="inserted")
    mark_inventory = Mock(side_effect=RuntimeError(secret_error))
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        mark_inventory,
    )

    with pytest.raises(HTTPException) as caught:
        asyncio.run(shopify_module.shopify_orders_webhook(_request(body)))

    assert caught.value.status_code == 503
    assert caught.value.detail == "Shopify webhook processing temporarily unavailable"
    upsert.assert_not_called()
    assert [name for name, _session in write_invocations] == [
        "_record_invalid_order_identity"
    ]
    assert mark_inventory.call_args.args[0] is write_invocations[0][1]
    captured_output = capsys.readouterr().out
    assert secret_error not in captured_output
    records = _structured_logs_from(captured_output)
    inventory_failed = next(
        record
        for record in records
        if record["action"] == "inventory.sold_marking.failed"
    )
    assert inventory_failed["error"] == "Shopify inventory marking failed"
    assert inventory_failed["error_type"] == "RuntimeError"
    assert inventory_failed["processing_stage"] == "invalid_order_identity"
    webhook_failed = next(
        record
        for record in records
        if record["action"] == "shopify.webhook.failed"
    )
    assert webhook_failed["error"] == "Shopify webhook processing failed"
    assert webhook_failed["error_type"] == "RuntimeError"
    assert webhook_failed["processing_stage"] == "invalid_order_identity"


def test_order_upsert_failure_returns_retryable_503_without_error_leak(
    monkeypatch,
    capsys,
):
    body = json.dumps(_payload()).encode("utf-8")
    secret_error = "database password=do-not-leak"
    write_invocations = _configure_inline_writes(monkeypatch)
    upsert = Mock(side_effect=RuntimeError(secret_error))
    mark_inventory = Mock()
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        mark_inventory,
    )

    with pytest.raises(HTTPException) as caught:
        asyncio.run(shopify_module.shopify_orders_webhook(_request(body)))

    assert caught.value.status_code == 503
    assert caught.value.detail == "Shopify webhook processing temporarily unavailable"
    assert secret_error not in str(caught.value.detail)
    assert [name for name, _session in write_invocations] == ["write_shopify_order"]
    assert upsert.call_args.args[0] is write_invocations[0][1]
    mark_inventory.assert_not_called()
    captured_output = capsys.readouterr().out
    assert secret_error not in captured_output
    failed = next(
        record
        for record in _structured_logs_from(captured_output)
        if record["action"] == "shopify.webhook.failed"
    )
    assert failed["success"] is False
    assert failed["error"] == "Shopify webhook processing failed"
    assert failed["error_type"] == "RuntimeError"
    assert failed["processing_stage"] == "order_upsert"


def test_inventory_failure_after_upsert_returns_retryable_503_and_logs_both_failures(
    monkeypatch,
    capsys,
):
    body = json.dumps(_payload()).encode("utf-8")
    secret_error = "inventory connection token=do-not-leak"
    write_invocations = _configure_inline_writes(monkeypatch)
    upsert = Mock(return_value="inserted")
    mark_inventory = Mock(side_effect=RuntimeError(secret_error))
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        mark_inventory,
    )

    with pytest.raises(HTTPException) as caught:
        asyncio.run(shopify_module.shopify_orders_webhook(_request(body)))

    assert caught.value.status_code == 503
    assert caught.value.detail == "Shopify webhook processing temporarily unavailable"
    assert secret_error not in str(caught.value.detail)
    assert [name for name, _session in write_invocations] == [
        "write_shopify_order",
        "_mark_sold",
    ]
    upsert_session = write_invocations[0][1]
    inventory_session = write_invocations[1][1]
    assert upsert_session is not inventory_session
    assert upsert.call_args.args[0] is upsert_session
    assert mark_inventory.call_args.args[0] is inventory_session
    captured_output = capsys.readouterr().out
    assert secret_error not in captured_output
    records = _structured_logs_from(captured_output)
    actions = [record["action"] for record in records]
    assert "inventory.sold_marking.failed" in actions
    assert "shopify.webhook.failed" in actions
    inventory_failed = next(
        record
        for record in records
        if record["action"] == "inventory.sold_marking.failed"
    )
    assert inventory_failed["error"] == "Shopify inventory marking failed"
    assert inventory_failed["error_type"] == "RuntimeError"
    assert inventory_failed["processing_stage"] == "inventory_marking"
    webhook_failed = next(
        record
        for record in records
        if record["action"] == "shopify.webhook.failed"
    )
    assert webhook_failed["error"] == "Shopify webhook processing failed"
    assert webhook_failed["error_type"] == "RuntimeError"
    assert webhook_failed["processing_stage"] == "inventory_marking"


def test_non_paid_order_is_persisted_and_logged_as_inventory_deferred(
    monkeypatch,
    capsys,
):
    payload = _payload(financial_status="pending")
    body = json.dumps(payload).encode("utf-8")
    write_invocations = _configure_inline_writes(monkeypatch)
    upsert = Mock(return_value="inserted")
    mark_inventory = Mock(return_value=0)
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", upsert)
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        mark_inventory,
    )

    response = asyncio.run(
        shopify_module.shopify_orders_webhook(
            _request(body, topic="orders/create")
        )
    )

    assert response.status_code == 200
    assert [name for name, _session in write_invocations] == [
        "write_shopify_order",
        "_mark_sold",
    ]
    assert write_invocations[0][1] is not write_invocations[1][1]
    assert upsert.call_args.args[0] is write_invocations[0][1]
    assert mark_inventory.call_args.args[0] is write_invocations[1][1]
    received = next(
        record
        for record in _structured_logs(capsys)
        if record["action"] == "shopify.webhook.received"
    )
    assert received["operation"] == "inserted"
    assert received["inventory_marked_count"] == 0
    assert received["inventory_outcome"] == "deferred"


@pytest.mark.parametrize(
    ("marked_count", "expected_outcome"),
    [(2, "marked"), (0, "no_change_or_replay")],
)
def test_paid_order_success_log_distinguishes_inventory_result(
    monkeypatch,
    capsys,
    marked_count,
    expected_outcome,
):
    body = json.dumps(_payload(financial_status="paid")).encode("utf-8")
    _configure_inline_writes(monkeypatch)
    monkeypatch.setattr(shopify_module.settings, "shopify_webhook_secret", WEBHOOK_SECRET)
    monkeypatch.setattr(shopify_module, "upsert_shopify_order", Mock(return_value="updated"))
    monkeypatch.setattr(
        shopify_module,
        "mark_inventory_sold_from_shopify_order",
        Mock(return_value=marked_count),
    )

    response = asyncio.run(shopify_module.shopify_orders_webhook(_request(body)))

    assert response.status_code == 200
    received = next(
        record
        for record in _structured_logs(capsys)
        if record["action"] == "shopify.webhook.received"
    )
    assert received["operation"] == "updated"
    assert received["inventory_marked_count"] == marked_count
    assert received["inventory_outcome"] == expected_outcome


def test_signed_zero_stock_delivery_claim_survives_restock_and_real_replay(
    monkeypatch,
):
    engine = _database_engine()
    try:
        with Session(engine) as session:
            item = InventoryItem(
                barcode="DGN-000042",
                item_type="sealed",
                game="Pokemon",
                card_name="Replay-safe booster box",
                quantity=0,
                status=INVENTORY_SOLD,
            )
            session.add(item)
            session.commit()
            session.refresh(item)
            item_id = item.id

        payload = _payload(financial_status="paid")
        payload["line_items"][0]["quantity"] = 2
        body = json.dumps(payload).encode("utf-8")

        def run_with_database_session(write_operation):
            with Session(engine) as session:
                result = write_operation(session)
                session.commit()
                return result

        monkeypatch.setattr(
            shopify_module.settings,
            "shopify_webhook_secret",
            WEBHOOK_SECRET,
        )
        monkeypatch.setattr(shopify_module.asyncio, "to_thread", _inline_to_thread)
        monkeypatch.setattr(
            shopify_module,
            "run_write_with_retry",
            run_with_database_session,
        )

        first_response = asyncio.run(
            shopify_module.shopify_orders_webhook(_request(body))
        )
        assert first_response.status_code == 200

        with Session(engine) as session:
            item = session.get(InventoryItem, item_id)
            assert item is not None
            assert item.quantity == 0
            movements = session.exec(select(InventoryStockMovement)).all()
            assert len(movements) == 1
            assert movements[0].quantity_delta == 0
            assert movements[0].requested_quantity == 2
            assert len(session.exec(select(ShopifyOrder)).all()) == 1
            assert session.exec(select(ShopifySyncJob)).all() == []

            item.quantity = 5
            item.status = INVENTORY_IN_STOCK
            session.add(item)
            session.commit()

        replay_response = asyncio.run(
            shopify_module.shopify_orders_webhook(_request(body))
        )
        assert replay_response.status_code == 200

        with Session(engine) as session:
            item = session.get(InventoryItem, item_id)
            assert item is not None
            assert item.quantity == 5
            assert len(session.exec(select(InventoryStockMovement)).all()) == 1
            assert len(session.exec(select(ShopifyOrder)).all()) == 1
            assert session.exec(select(ShopifySyncJob)).all() == []
    finally:
        engine.dispose()
