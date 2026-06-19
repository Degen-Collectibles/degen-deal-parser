from __future__ import annotations

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select


def _session() -> Session:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def test_parse_tagged_member_name_requires_degen_tag_and_role():
    from scripts.degen_ops_discord_link_employees import DiscordMember, parse_degen_employee_member_name

    member = DiscordMember(
        discord_user_id="111",
        username="andrew",
        display_name="Degen Collectibles | Andrew",
        role_names=["Employees"],
    )
    missing_tag = DiscordMember(
        discord_user_id="222",
        username="other",
        display_name="Andrew",
        role_names=["Employee"],
    )
    missing_role = DiscordMember(
        discord_user_id="333",
        username="other",
        display_name="Degen Collectibles | Andrew",
        role_names=["Customer"],
    )

    assert parse_degen_employee_member_name(member) == "Andrew"
    assert parse_degen_employee_member_name(missing_tag) is None
    assert parse_degen_employee_member_name(missing_role) is None


def test_build_link_plan_uses_authorized_name_aliases():
    from app.models import EmployeeProfile, User
    from scripts.degen_ops_discord_link_employees import DiscordMember, build_link_plan

    with _session() as session:
        session.add(
            User(
                id=1,
                username="axel707",
                password_hash="x",
                password_salt="s",
                display_name="Boss Alex",
                role="admin",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=1))
        session.commit()

        rows = build_link_plan(
            session,
            [
                DiscordMember(
                    discord_user_id="707",
                    username="707axel",
                    display_name="Degen Collectibles | BossAlex",
                    role_names=["Admin"],
                )
            ],
        )

    assert rows[0].action == "link"
    assert rows[0].confidence == "alias"
    assert rows[0].reason == "alias_display_name_match"
    assert rows[0].employee_user_id == 1
    assert rows[0].employee_display_name == "Boss Alex"


def test_build_link_plan_uses_authorized_alias_for_damien_s():
    from app.models import EmployeeProfile, User
    from scripts.degen_ops_discord_link_employees import DiscordMember, build_link_plan

    with _session() as session:
        session.add(
            User(
                id=30,
                username="guyinbacc",
                password_hash="x",
                password_salt="s",
                display_name="Alex",
                role="employee",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=30))
        session.commit()

        rows = build_link_plan(
            session,
            [
                DiscordMember(
                    discord_user_id="999",
                    username=".trs",
                    display_name="Degen Collectibles | Damien S.",
                    role_names=["Employees"],
                )
            ],
        )

    assert rows[0].action == "link"
    assert rows[0].confidence == "alias"
    assert rows[0].reason == "alias_display_name_match"
    assert rows[0].employee_user_id == 30
    assert rows[0].employee_display_name == "Alex"


def test_build_link_plan_links_only_exact_active_unlinked_employee():
    from app.models import EmployeeProfile, User
    from scripts.degen_ops_discord_link_employees import DiscordMember, build_link_plan

    with _session() as session:
        session.add(
            User(
                id=1,
                username="andrew",
                password_hash="x",
                password_salt="s",
                display_name="Andrew",
                role="employee",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=1))
        session.commit()

        rows = build_link_plan(
            session,
            [
                DiscordMember(
                    discord_user_id="111222333444555666",
                    username="andrew.discord",
                    display_name="Degen Collectibles | Andrew",
                    role_names=["Employee"],
                )
            ],
        )

    assert len(rows) == 1
    assert rows[0].action == "link"
    assert rows[0].confidence == "exact"
    assert rows[0].employee_user_id == 1
    assert rows[0].discord_user_id == "111222333444555666"


def test_build_link_plan_needs_review_on_duplicate_employee_names():
    from app.models import EmployeeProfile, User
    from scripts.degen_ops_discord_link_employees import DiscordMember, build_link_plan

    with _session() as session:
        for user_id, username in ((1, "alex1"), (2, "alex2")):
            session.add(
                User(
                    id=user_id,
                    username=username,
                    password_hash="x",
                    password_salt="s",
                    display_name="Alex",
                    role="employee",
                    is_active=True,
                )
            )
            session.add(EmployeeProfile(user_id=user_id))
        session.commit()

        rows = build_link_plan(
            session,
            [
                DiscordMember(
                    discord_user_id="999",
                    username="alex.discord",
                    display_name="Degen Collectibles | Alex",
                    role_names=["Admin"],
                )
            ],
        )

    assert rows[0].action == "needs_review"
    assert rows[0].reason == "multiple_employee_matches"
    assert rows[0].employee_user_id is None


def test_apply_link_plan_updates_only_exact_link_rows():
    from app.models import EmployeeProfile, User
    from scripts.degen_ops_discord_link_employees import (
        DiscordLinkPlanRow,
        apply_link_plan,
    )

    with _session() as session:
        session.add(
            User(
                id=1,
                username="andrew",
                password_hash="x",
                password_salt="s",
                display_name="Andrew",
                role="employee",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=1))
        session.commit()

        applied = apply_link_plan(
            session,
            [
                DiscordLinkPlanRow(
                    action="link",
                    confidence="exact",
                    reason="exact_display_name_match",
                    discord_user_id="111222333444555666",
                    discord_username="andrew.discord",
                    discord_display_name="Degen Collectibles | Andrew",
                    parsed_employee_name="Andrew",
                    employee_user_id=1,
                    employee_display_name="Andrew",
                    current_discord_user_id="",
                ),
                DiscordLinkPlanRow(
                    action="needs_review",
                    confidence="none",
                    reason="multiple_employee_matches",
                    discord_user_id="999",
                    discord_username="alex.discord",
                    discord_display_name="Degen Collectibles | Alex",
                    parsed_employee_name="Alex",
                    employee_user_id=None,
                    employee_display_name="",
                    current_discord_user_id="",
                ),
            ],
            apply=True,
        )
        session.commit()
        profile = session.exec(select(EmployeeProfile).where(EmployeeProfile.user_id == 1)).one()

    assert applied == 1
    assert profile.discord_user_id == "111222333444555666"
    assert profile.discord_username == "andrew.discord"


def test_resolve_bot_token_falls_back_to_existing_discord_token(monkeypatch):
    from scripts.degen_ops_discord_link_employees import resolve_bot_token

    monkeypatch.delenv("DEGEN_OPS_DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "legacy-token")

    assert resolve_bot_token("DEGEN_OPS_DISCORD_BOT_TOKEN") == "legacy-token"


def test_fetch_discord_members_falls_back_to_member_search(monkeypatch):
    import scripts.degen_ops_discord_link_employees as linker

    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload
            self.text = "forbidden" if status_code == 403 else ""

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise AssertionError(f"unexpected status {self.status_code}")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def get(self, url, params=None):
            self.calls.append((url, params or {}))
            if url.endswith("/roles"):
                return FakeResponse(200, [{"id": "1", "name": "Employee"}])
            if url.endswith("/members/search"):
                assert (params or {}).get("query") == "Degen Collectibles"
                return FakeResponse(
                    200,
                    [
                        {
                            "nick": "Degen Collectibles | Andrew",
                            "roles": ["1"],
                            "user": {"id": "111", "username": "andrew.discord"},
                        }
                    ],
                )
            if url.endswith("/members"):
                return FakeResponse(403, {"message": "Missing Access"})
            raise AssertionError(f"unexpected URL {url}")

    monkeypatch.setattr(linker.httpx, "Client", FakeClient)

    members = linker.fetch_discord_members(
        bot_token="token",
        guild_id="guild",
        search_query="Degen Collectibles",
    )

    assert members == [
        linker.DiscordMember(
            discord_user_id="111",
            username="andrew.discord",
            display_name="Degen Collectibles | Andrew",
            role_names=["Employee"],
        )
    ]
