from __future__ import annotations

from dataclasses import dataclass

from sqlmodel import Session, select

from .models import EmployeeProfile, User


SCOPE_SUBSCOPES = {
    "employee": frozenset({"employee"}),
    "tiktok": frozenset({"tiktok"}),
    "partner": frozenset({"employee", "partner"}),
    "manager": frozenset({"employee", "tiktok", "manager"}),
    "owner": frozenset({"employee", "tiktok", "partner", "manager", "owner"}),
}

ROLE_TO_DEGEN_OPS_SCOPE = {
    "employee": "employee",
    "viewer": "employee",
    "manager": "manager",
    "reviewer": "employee",
    "admin": "owner",
}


@dataclass(frozen=True)
class DiscordAuthorScope:
    allowed: bool
    scope: str | None
    reason: str
    app_user_id: int | None = None
    app_role: str = ""
    display_name: str = ""

    def as_audit_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {
            "app_user_id": self.app_user_id,
            "app_role": self.app_role,
        }
        return {key: value for key, value in fields.items() if value not in (None, "")}


def _normalize_discord_user_id(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _intersect_named_scopes(first: str, second: str) -> str | None:
    """Return the narrower named scope, or deny partially overlapping domains."""
    if first in SCOPE_SUBSCOPES[second]:
        return first
    if second in SCOPE_SUBSCOPES[first]:
        return second
    return None


def resolve_discord_author_scope(
    *,
    session: Session,
    discord_user_id: str,
    channel_id: str,
    channel_scopes: dict[str, str] | None = None,
    allow_dm: bool = False,
    is_dm: bool = False,
) -> DiscordAuthorScope:
    normalized_discord_id = _normalize_discord_user_id(discord_user_id)
    if not normalized_discord_id:
        return DiscordAuthorScope(False, None, "discord_user_id_missing")

    profile = session.exec(
        select(EmployeeProfile).where(EmployeeProfile.discord_user_id == normalized_discord_id)
    ).first()
    if profile is None:
        return DiscordAuthorScope(False, None, "discord_user_not_linked")

    user = session.get(User, profile.user_id)
    if user is None:
        return DiscordAuthorScope(False, None, "linked_user_missing")
    display_name = user.display_name or user.username or str(user.id or "")
    app_role = (user.role or "").strip().lower()
    user_scope = ROLE_TO_DEGEN_OPS_SCOPE.get(app_role)
    if not user_scope:
        return DiscordAuthorScope(
            False,
            None,
            "role_not_allowed",
            app_user_id=user.id,
            app_role=app_role,
            display_name=display_name,
        )
    if not user.is_active:
        return DiscordAuthorScope(
            False,
            None,
            "linked_user_inactive",
            app_user_id=user.id,
            app_role=app_role,
            display_name=display_name,
        )

    channel_scopes = channel_scopes or {}
    if is_dm:
        if not allow_dm:
            return DiscordAuthorScope(
                False,
                None,
                "dm_not_allowed",
                app_user_id=user.id,
                app_role=app_role,
                display_name=display_name,
            )
        return DiscordAuthorScope(
            True,
            user_scope,
            "db_auth",
            app_user_id=user.id,
            app_role=app_role,
            display_name=display_name,
        )

    channel_scope = channel_scopes.get(str(channel_id))
    if not channel_scope:
        return DiscordAuthorScope(
            False,
            None,
            "channel_not_mapped",
            app_user_id=user.id,
            app_role=app_role,
            display_name=display_name,
        )
    effective_scope = _intersect_named_scopes(user_scope, channel_scope)
    if effective_scope is None:
        return DiscordAuthorScope(
            False,
            None,
            "incomparable_scopes",
            app_user_id=user.id,
            app_role=app_role,
            display_name=display_name,
        )
    return DiscordAuthorScope(
        True,
        effective_scope,
        "db_auth",
        app_user_id=user.id,
        app_role=app_role,
        display_name=display_name,
    )

