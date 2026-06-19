from __future__ import annotations

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine


def _session() -> Session:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def test_employee_profile_has_discord_identity_fields():
    from app.models import EmployeeProfile

    profile = EmployeeProfile(
        user_id=1,
        discord_user_id="206237952412483584",
        discord_username="jeff",
    )

    assert profile.discord_user_id == "206237952412483584"
    assert profile.discord_username == "jeff"


def test_resolver_denies_unlinked_discord_user():
    from app.degen_ops_discord_auth import resolve_discord_author_scope

    with _session() as session:
        result = resolve_discord_author_scope(
            session=session,
            discord_user_id="999",
            channel_id="chan",
            channel_scopes={"chan": "employee"},
        )

    assert result.allowed is False
    assert result.reason == "discord_user_not_linked"


def test_resolver_maps_active_employee_to_employee_scope():
    from app.degen_ops_discord_auth import resolve_discord_author_scope
    from app.models import EmployeeProfile, User

    with _session() as session:
        session.add(
            User(
                id=1,
                username="emp",
                password_hash="x",
                password_salt="s",
                role="employee",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=1, discord_user_id="111"))
        session.commit()

        result = resolve_discord_author_scope(
            session=session,
            discord_user_id="111",
            channel_id="chan",
            channel_scopes={"chan": "employee"},
        )

    assert result.allowed is True
    assert result.scope == "employee"
    assert result.app_user_id == 1
    assert result.app_role == "employee"


def test_resolver_denies_linked_user_when_channel_scope_map_is_missing():
    from app.degen_ops_discord_auth import resolve_discord_author_scope
    from app.models import EmployeeProfile, User

    with _session() as session:
        session.add(
            User(
                id=5,
                username="unmapped",
                password_hash="x",
                password_salt="s",
                role="employee",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=5, discord_user_id="555"))
        session.commit()

        result = resolve_discord_author_scope(
            session=session,
            discord_user_id="555",
            channel_id="missing-channel",
            channel_scopes={},
        )

    assert result.allowed is False
    assert result.scope is None
    assert result.reason == "channel_not_mapped"


def test_resolver_maps_reviewer_to_employee_scope():
    from app.degen_ops_discord_auth import resolve_discord_author_scope
    from app.models import EmployeeProfile, User

    with _session() as session:
        session.add(
            User(
                id=2,
                username="reviewer",
                password_hash="x",
                password_salt="s",
                role="reviewer",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=2, discord_user_id="222"))
        session.commit()

        result = resolve_discord_author_scope(
            session=session,
            discord_user_id="222",
            channel_id="chan",
            channel_scopes={"chan": "owner"},
        )

    assert result.allowed is True
    assert result.scope == "employee"
    assert result.reason == "db_auth"


def test_resolver_maps_manager_to_manager_scope_and_channel_can_lower_it():
    from app.degen_ops_discord_auth import resolve_discord_author_scope
    from app.models import EmployeeProfile, User

    with _session() as session:
        session.add(
            User(
                id=3,
                username="manager",
                password_hash="x",
                password_salt="s",
                role="manager",
                is_active=True,
            )
        )
        session.add(EmployeeProfile(user_id=3, discord_user_id="333"))
        session.commit()

        owner_channel = resolve_discord_author_scope(
            session=session,
            discord_user_id="333",
            channel_id="owner-chan",
            channel_scopes={"owner-chan": "owner"},
        )
        employee_channel = resolve_discord_author_scope(
            session=session,
            discord_user_id="333",
            channel_id="employee-chan",
            channel_scopes={"employee-chan": "employee"},
        )

    assert owner_channel.allowed is True
    assert owner_channel.scope == "manager"
    assert employee_channel.allowed is True
    assert employee_channel.scope == "employee"


def test_resolver_denies_inactive_linked_user():
    from app.degen_ops_discord_auth import resolve_discord_author_scope
    from app.models import EmployeeProfile, User

    with _session() as session:
        session.add(
            User(
                id=4,
                username="inactive",
                password_hash="x",
                password_salt="s",
                role="employee",
                is_active=False,
            )
        )
        session.add(EmployeeProfile(user_id=4, discord_user_id="444"))
        session.commit()

        result = resolve_discord_author_scope(
            session=session,
            discord_user_id="444",
            channel_id="chan",
            channel_scopes={"chan": "employee"},
        )

    assert result.allowed is False
    assert result.reason == "linked_user_inactive"

