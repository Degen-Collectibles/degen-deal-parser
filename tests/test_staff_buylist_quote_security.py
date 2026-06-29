from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

from app.config import Settings
from app.financial_values import MAX_ABS_MONEY
from app.models import BuylistSubmission, User
from app.routers import team_buylist


TEST_QUOTE_KEY = "buylist-quote-test-key-000000000000000000000001"


class FakeJsonRequest:
    def __init__(self, body: dict, user: User):
        self._body = body
        self.state = SimpleNamespace(current_user=user)
        self.client = SimpleNamespace(host="127.0.0.1")

    async def json(self):
        return deepcopy(self._body)


def _user(user_id: int, role: str = "employee") -> User:
    return User(
        id=user_id,
        username=f"quote-user-{user_id}",
        password_hash="x",
        password_salt="x",
        display_name=f"Quote User {user_id}",
        role=role,
        is_active=True,
    )


def _settings(keys: str = TEST_QUOTE_KEY):
    return SimpleNamespace(
        employee_portal_enabled=True,
        buylist_quote_signing_keys=keys,
        session_secret="separate-session-secret-000000000000000000000001",
    )


def _memory_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine, Session(engine)


def test_settings_exposes_dedicated_buylist_quote_key_ring():
    settings = Settings(BUYLIST_QUOTE_SIGNING_KEYS=f"{TEST_QUOTE_KEY},previous-key")

    assert settings.buylist_quote_signing_keys == f"{TEST_QUOTE_KEY},previous-key"


async def _search_card(monkeypatch, user: User, *, price: float = 10.0):
    async def fake_text_search_cards(query, **kwargs):
        return {
            "status": "MATCHED",
            "candidates": [
                {
                    "id": "card-1",
                    "product_id": "12345",
                    "name": "Trusted Card",
                    "set_name": "Trusted Set",
                    "number": "1/100",
                    "available_variants": [
                        {
                            "name": "Normal",
                            "price": price,
                            "conditions": {"NM": price, "LP": price * 0.8},
                        }
                    ],
                }
            ],
        }

    monkeypatch.setattr(team_buylist, "get_settings", lambda: _settings())
    monkeypatch.setattr(
        team_buylist,
        "_require_team_user",
        lambda request, session: (None, user),
    )
    monkeypatch.setattr(
        team_buylist,
        "get_buylist_config",
        lambda session: deepcopy(team_buylist.DEFAULT_BUYLIST_CONFIG),
    )
    monkeypatch.setattr(team_buylist, "text_search_cards", fake_text_search_cards)
    team_buylist._BUYLIST_SEARCH_CACHE.clear()
    response = await team_buylist.staff_buylist_search(
        SimpleNamespace(),
        q="trusted card",
        game="Pokemon",
        product_type="card",
        session=None,
    )
    assert response.status_code == 200
    return json.loads(response.body.decode("utf-8"))["cards"][0]


def _quote(monkeypatch, user: User, item: dict):
    monkeypatch.setattr(
        team_buylist,
        "_require_team_user",
        lambda request, session: (None, user),
    )
    monkeypatch.setattr(
        team_buylist,
        "get_buylist_config",
        lambda session: deepcopy(team_buylist.DEFAULT_BUYLIST_CONFIG),
    )
    return asyncio.run(
        team_buylist.staff_buylist_quote(
            FakeJsonRequest({"items": [item]}, user),
            session=None,
        )
    )


def _valid_item(card: dict, **overrides) -> dict:
    item = {
        "candidate_token": card["candidate_token"],
        "quantity": 1,
        "condition": "NM",
        "language": "English",
        "variant": "Normal",
    }
    item.update(overrides)
    return item


def test_signed_ten_dollar_candidate_ignores_forged_100000_price_on_save(monkeypatch):
    engine, session = _memory_session()
    try:
        user = _user(101)
        session.add(user)
        session.commit()
        card = asyncio.run(_search_card(monkeypatch, user, price=10.0))
        request = FakeJsonRequest(
            {
                "customer_name": "Customer",
                "payment_view": "cash",
                "totals": {"cash": 100_000.0, "trade": 100_000.0},
                "items": [
                    {
                        "candidate_token": card["candidate_token"],
                        "quantity": 1,
                        "condition": "NM",
                        "language": "English",
                        "variant": "Normal",
                        "id": "forged-card",
                        "name": "Forged identity",
                        "base_market_price": 100_000.0,
                        "market_price": 100_000.0,
                        "condition_market_prices": {"NM": 100_000.0},
                        "cash_offer": 100_000.0,
                        "line_cash": 100_000.0,
                    }
                ],
            },
            user,
        )

        response = asyncio.run(team_buylist.staff_buylist_save(request, session=session))
        body = json.loads(response.body.decode("utf-8"))
        submission = session.get(BuylistSubmission, body["submission_id"])
        line = json.loads(submission.lines_json)[0]

        assert line["id"] == "card-1"
        assert line["name"] == "Trusted Card"
        assert line["base_market_price"] == 10.0
        assert line["unit_cash"] == 5.0
        assert line["line_cash"] == 5.0
        assert "condition_market_prices" not in line
        assert "cash_offer" not in line
        assert line["attestation"]
        assert json.loads(submission.totals_json)["cash"] == 5.0
    finally:
        session.close()
        engine.dispose()


@pytest.mark.parametrize("case", ["missing", "tampered", "wrong_employee", "unknown_variant"])
def test_quote_rejects_invalid_candidate_tokens_and_selections(monkeypatch, case):
    user = _user(201)
    card = asyncio.run(_search_card(monkeypatch, user))
    item = _valid_item(card)
    quote_user = user
    if case == "missing":
        item.pop("candidate_token")
    elif case == "tampered":
        token = item["candidate_token"]
        item["candidate_token"] = ("A" if token[0] != "A" else "B") + token[1:]
    elif case == "wrong_employee":
        quote_user = _user(202)
    elif case == "unknown_variant":
        item["variant"] = "Forged Foil"

    response = _quote(monkeypatch, quote_user, item)
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 400
    assert body == {"ok": False, "error": team_buylist.BUYLIST_QUOTE_INVALID_ERROR}


def test_quote_rejects_expired_candidate_token(monkeypatch):
    user = _user(203)
    card = asyncio.run(_search_card(monkeypatch, user))
    issued_at = int(team_buylist.time.time())
    monkeypatch.setattr(
        team_buylist.time,
        "time",
        lambda: issued_at + team_buylist.BUYLIST_CANDIDATE_TOKEN_MAX_AGE_SECONDS + 1,
    )

    response = _quote(monkeypatch, user, _valid_item(card))

    assert response.status_code == 400
    assert json.loads(response.body)["error"] == team_buylist.BUYLIST_QUOTE_INVALID_ERROR


def test_missing_or_session_reused_signing_key_fails_closed_with_clear_503(monkeypatch):
    user = _user(204)
    monkeypatch.setattr(
        team_buylist,
        "_require_team_user",
        lambda request, session: (None, user),
    )
    for keys, session_secret in (
        ("", "separate-session-secret-000000000000000000000001"),
        (TEST_QUOTE_KEY, TEST_QUOTE_KEY),
    ):
        monkeypatch.setattr(
            team_buylist,
            "get_settings",
            lambda keys=keys, session_secret=session_secret: SimpleNamespace(
                employee_portal_enabled=True,
                buylist_quote_signing_keys=keys,
                session_secret=session_secret,
            ),
        )
        response = asyncio.run(
            team_buylist.staff_buylist_search(
                SimpleNamespace(),
                q="trusted card",
                game="Pokemon",
                product_type="card",
                session=None,
            )
        )
        body = json.loads(response.body.decode("utf-8"))
        assert response.status_code == 503
        assert body["error"] == team_buylist.BUYLIST_QUOTE_UNAVAILABLE_ERROR


def test_cached_search_results_are_rebound_to_each_employee(monkeypatch):
    users = [_user(205), _user(206)]
    current = {"user": users[0]}
    calls = []

    async def fake_text_search_cards(query, **kwargs):
        calls.append(query)
        return {
            "status": "MATCHED",
            "candidates": [
                {
                    "id": "cached-card",
                    "name": "Cached Card",
                    "set_name": "Cached Set",
                    "number": "1",
                    "available_variants": [{"name": "Normal", "price": 10.0}],
                }
            ],
        }

    monkeypatch.setattr(team_buylist, "get_settings", lambda: _settings())
    monkeypatch.setattr(
        team_buylist,
        "_require_team_user",
        lambda request, session: (None, current["user"]),
    )
    monkeypatch.setattr(
        team_buylist,
        "get_buylist_config",
        lambda session: deepcopy(team_buylist.DEFAULT_BUYLIST_CONFIG),
    )
    monkeypatch.setattr(team_buylist, "text_search_cards", fake_text_search_cards)
    team_buylist._BUYLIST_SEARCH_CACHE.clear()

    first = asyncio.run(
        team_buylist.staff_buylist_search(
            SimpleNamespace(), q="cached card", game="Pokemon", product_type="card", session=None
        )
    )
    current["user"] = users[1]
    second = asyncio.run(
        team_buylist.staff_buylist_search(
            SimpleNamespace(), q="cached card", game="Pokemon", product_type="card", session=None
        )
    )
    first_card = json.loads(first.body)["cards"][0]
    second_body = json.loads(second.body)
    second_card = second_body["cards"][0]

    assert calls == ["cached card"]
    assert second_body["cached"] is True
    assert first_card["candidate_token"] != second_card["candidate_token"]
    assert all(
        "candidate_token" not in card
        for _, payload in team_buylist._BUYLIST_SEARCH_CACHE.values()
        for card in payload.get("cards") or []
    )
    assert _quote(monkeypatch, users[1], _valid_item(first_card)).status_code == 400
    assert _quote(monkeypatch, users[1], _valid_item(second_card)).status_code == 200


def test_nonfinite_or_overbound_search_prices_are_not_tokenized(monkeypatch):
    user = _user(207)
    for unsafe_price in (float("inf"), float("nan"), MAX_ABS_MONEY + 1):
        async def fake_text_search_cards(query, **kwargs):
            return {
                "status": "MATCHED",
                "candidates": [
                    {
                        "id": "unsafe",
                        "name": "Unsafe",
                        "available_variants": [{"name": "Normal", "price": unsafe_price}],
                    }
                ],
            }

        monkeypatch.setattr(team_buylist, "get_settings", lambda: _settings())
        monkeypatch.setattr(
            team_buylist,
            "_require_team_user",
            lambda request, session: (None, user),
        )
        monkeypatch.setattr(
            team_buylist,
            "get_buylist_config",
            lambda session: deepcopy(team_buylist.DEFAULT_BUYLIST_CONFIG),
        )
        monkeypatch.setattr(team_buylist, "text_search_cards", fake_text_search_cards)
        team_buylist._BUYLIST_SEARCH_CACHE.clear()
        response = asyncio.run(
            team_buylist.staff_buylist_search(
                SimpleNamespace(), q="unsafe", game="Pokemon", product_type="card", session=None
            )
        )
        assert json.loads(response.body)["cards"] == []


def test_sealed_candidate_quotes_and_saves_from_signed_snapshot(monkeypatch):
    engine, session = _memory_session()
    try:
        user = _user(208)
        session.add(user)
        session.commit()

        async def fake_sealed_search(query, **kwargs):
            return [
                {
                    "external_id": "sealed-1",
                    "name": "Trusted Booster Box",
                    "set_name": "Trusted Set",
                    "kind": "Booster Box",
                    "upc": "123456789012",
                    "game": "Pokemon",
                    "category_id": "3",
                    "market_price": 100.0,
                }
            ], ""

        monkeypatch.setattr(team_buylist, "get_settings", lambda: _settings())
        monkeypatch.setattr(
            team_buylist,
            "_require_team_user",
            lambda request, sess: (None, user),
        )
        monkeypatch.setattr(
            team_buylist,
            "get_buylist_config",
            lambda sess: deepcopy(team_buylist.DEFAULT_BUYLIST_CONFIG),
        )
        monkeypatch.setattr(team_buylist, "_search_buylist_sealed_products", fake_sealed_search)
        team_buylist._BUYLIST_SEARCH_CACHE.clear()
        search = asyncio.run(
            team_buylist.staff_buylist_search(
                SimpleNamespace(),
                q="trusted booster",
                game="Pokemon",
                product_type="sealed",
                session=session,
            )
        )
        card = json.loads(search.body)["cards"][0]
        request = FakeJsonRequest(
            {
                "customer_name": "Customer",
                "payment_view": "cash",
                "items": [
                    {
                        "candidate_token": card["candidate_token"],
                        "quantity": 1,
                        "condition": "Sealed",
                        "language": "",
                        "variant": "Sealed Product",
                    }
                ],
            },
            user,
        )

        response = asyncio.run(team_buylist.staff_buylist_save(request, session=session))
        body = json.loads(response.body)
        submission = session.get(BuylistSubmission, body["submission_id"])
        line = json.loads(submission.lines_json)[0]

        assert line["item_type"] == "sealed"
        assert line["name"] == "Trusted Booster Box"
        assert line["base_market_price"] == 100.0
        assert line["unit_cash"] == 65.0
        assert line["unit_trade"] == 75.0
        assert line["attestation"]
    finally:
        session.close()
        engine.dispose()


def _direct_attested_line(monkeypatch, user: User) -> dict:
    monkeypatch.setattr(team_buylist, "get_settings", lambda: _settings())
    card = {
        "id": "card-approval",
        "product_id": "card-approval",
        "item_type": "card",
        "game": "Pokemon",
        "name": "Approval Card",
        "set_name": "Approval Set",
        "number": "7",
        "variant": "Normal",
        "base_market_price": 10.0,
        "market_price": 10.0,
        "available_variants": [{"name": "Normal", "price": 10.0}],
    }
    snapshot = team_buylist._candidate_snapshot(card)
    token = team_buylist._sign_buylist_payload(
        {
            "version": team_buylist.BUYLIST_QUOTE_TOKEN_VERSION,
            "purpose": "buylist_candidate",
            "source": "search:card",
            "issued_at": int(team_buylist.time.time()),
            "employee_id": user.id,
            "candidate": snapshot,
        }
    )
    quoted = team_buylist._quote_signed_candidate(
        {
            "candidate_token": token,
            "quantity": 1,
            "condition": "NM",
            "language": "English",
            "variant": "Normal",
        },
        employee_id=user.id,
        config=deepcopy(team_buylist.DEFAULT_BUYLIST_CONFIG),
    )
    return team_buylist._stored_buylist_line(quoted)


@pytest.mark.parametrize(
    "mode",
    ["legacy", "missing_attestation", "tampered_line", "tampered_totals"],
)
def test_approval_rejects_unattested_or_tampered_quote_before_inventory(monkeypatch, mode):
    engine, session = _memory_session()
    try:
        monkeypatch.setattr(team_buylist, "get_settings", lambda: _settings())
        actor = _user(209, role="admin")
        submitter = _user(210)
        session.add(actor)
        session.add(submitter)
        if mode == "legacy":
            lines = [
                {
                    "id": "legacy",
                    "name": "Legacy Card",
                    "unit_cash": 5.0,
                    "unit_trade": 6.0,
                    "quantity": 1,
                }
            ]
        else:
            lines = [_direct_attested_line(monkeypatch, submitter)]
            if mode == "missing_attestation":
                lines[0].pop("attestation")
            elif mode == "tampered_line":
                lines[0]["unit_cash"] = 50_000.0
        totals = {"cash": 5.0, "trade": 6.0, "quantity": 1, "items": 1}
        if mode == "tampered_totals":
            totals["cash"] = 50_000.0
        submission = BuylistSubmission(
            submitted_by_user_id=submitter.id,
            customer_name="Customer",
            payment_view="cash",
            status="submitted",
            totals_json=json.dumps(totals, sort_keys=True),
            lines_json=json.dumps(lines, sort_keys=True),
        )
        session.add(submission)
        session.commit()
        session.refresh(submission)
        called = {"receive": False}

        def should_not_receive(*args, **kwargs):
            called["receive"] = True
            raise AssertionError("inventory receive must not run")

        monkeypatch.setattr(
            team_buylist,
            "_permission_gate",
            lambda request, sess, key: (None, actor),
        )
        monkeypatch.setattr(team_buylist, "_receive_submission_inventory", should_not_receive)

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                team_buylist.admin_buylist_submission_approve(
                    FakeJsonRequest({}, actor),
                    submission_id=submission.id,
                    location="Case A",
                    session=session,
                )
            )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail == team_buylist.BUYLIST_ATTESTATION_INVALID_ERROR
        assert called["receive"] is False
        assert session.get(BuylistSubmission, submission.id).status == "submitted"
    finally:
        session.close()
        engine.dispose()


def test_key_ring_rotates_first_key_for_signing_and_keeps_old_key_for_verification(monkeypatch):
    user = _user(211)
    old_key = TEST_QUOTE_KEY
    new_key = "new-buylist-signing-key-000000000000000000000001"
    card = asyncio.run(_search_card(monkeypatch, user))
    monkeypatch.setattr(team_buylist, "get_settings", lambda: _settings(f"{new_key},{old_key}"))

    response = _quote(monkeypatch, user, _valid_item(card))

    assert response.status_code == 200
    line = json.loads(response.body)["lines"][0]
    new_payload = team_buylist._verify_buylist_payload(
        line["attestation"],
        purpose="buylist_line",
        employee_id=user.id,
        max_age_seconds=None,
    )
    assert new_payload["source"] == "server_quote"


def test_quote_rejects_empty_oversized_or_overbound_aggregate(monkeypatch):
    user = _user(212)
    card = asyncio.run(_search_card(monkeypatch, user, price=MAX_ABS_MONEY))
    monkeypatch.setattr(
        team_buylist,
        "_require_team_user",
        lambda request, session: (None, user),
    )
    monkeypatch.setattr(
        team_buylist,
        "get_buylist_config",
        lambda session: deepcopy(team_buylist.DEFAULT_BUYLIST_CONFIG),
    )

    for items in (
        [],
        [_valid_item(card) for _ in range(team_buylist.BUYLIST_MAX_QUOTE_ITEMS + 1)],
        [_valid_item(card), _valid_item(card)],
    ):
        response = asyncio.run(
            team_buylist.staff_buylist_quote(
                FakeJsonRequest({"items": items}, user),
                session=None,
            )
        )
        assert response.status_code == 400
        assert json.loads(response.body)["error"] == team_buylist.BUYLIST_QUOTE_INVALID_ERROR
