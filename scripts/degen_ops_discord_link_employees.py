from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Iterable

import httpx
from sqlmodel import Session, select


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_ROLE_NAMES = ("Employee", "Employees", "Admin")
DEFAULT_TAG_PREFIX = "Degen Collectibles |"
DEFAULT_NAME_ALIASES = {
    "BossAlex": "Boss Alex",
    "Damien S.": "Alex",
}
DISCORD_API_BASE = "https://discord.com/api/v10"


@dataclass(frozen=True)
class DiscordMember:
    discord_user_id: str
    username: str
    display_name: str
    role_names: list[str]


@dataclass(frozen=True)
class DiscordLinkPlanRow:
    action: str
    confidence: str
    reason: str
    discord_user_id: str
    discord_username: str
    discord_display_name: str
    parsed_employee_name: str
    employee_user_id: int | None
    employee_display_name: str
    current_discord_user_id: str


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def _role_set(role_names: Iterable[str]) -> set[str]:
    return {_normalize_name(role) for role in role_names if str(role or "").strip()}


def parse_name_aliases(value: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for item in str(value or "").split(","):
        if "=" not in item:
            continue
        source, target = item.split("=", 1)
        source = source.strip()
        target = target.strip()
        if source and target:
            aliases[source] = target
    return aliases


def _apply_name_alias(value: str, name_aliases: dict[str, str] | None) -> tuple[str, bool]:
    key = _normalize_name(value)
    for source, target in (name_aliases or {}).items():
        if _normalize_name(source) == key:
            return target, True
    return value, False


def _member_has_allowed_role(member: DiscordMember, role_names: Iterable[str]) -> bool:
    allowed = _role_set(role_names)
    return bool(allowed.intersection(_role_set(member.role_names)))


def parse_degen_employee_member_name(
    member: DiscordMember,
    *,
    tag_prefix: str = DEFAULT_TAG_PREFIX,
    role_names: Iterable[str] = DEFAULT_ROLE_NAMES,
) -> str | None:
    if not _member_has_allowed_role(member, role_names):
        return None

    display_name = str(member.display_name or "").strip()
    prefix = str(tag_prefix or "").strip()
    if not display_name or not prefix:
        return None

    index = display_name.casefold().find(prefix.casefold())
    if index < 0:
        return None

    parsed_name = display_name[index + len(prefix):].strip()
    return parsed_name or None


def _employee_label(user) -> str:
    return str(user.display_name or user.username or "").strip()


def _employee_match_keys(user) -> set[str]:
    return {
        key
        for key in (
            _normalize_name(getattr(user, "display_name", "")),
            _normalize_name(getattr(user, "username", "")),
        )
        if key
    }


def _plan_row(
    *,
    action: str,
    confidence: str,
    reason: str,
    member: DiscordMember,
    parsed_employee_name: str,
    employee_user_id: int | None = None,
    employee_display_name: str = "",
    current_discord_user_id: str = "",
) -> DiscordLinkPlanRow:
    return DiscordLinkPlanRow(
        action=action,
        confidence=confidence,
        reason=reason,
        discord_user_id=str(member.discord_user_id or ""),
        discord_username=str(member.username or ""),
        discord_display_name=str(member.display_name or ""),
        parsed_employee_name=parsed_employee_name,
        employee_user_id=employee_user_id,
        employee_display_name=employee_display_name,
        current_discord_user_id=current_discord_user_id or "",
    )


def build_link_plan(
    session: Session,
    members: Iterable[DiscordMember],
    *,
    tag_prefix: str = DEFAULT_TAG_PREFIX,
    role_names: Iterable[str] = DEFAULT_ROLE_NAMES,
    name_aliases: dict[str, str] | None = None,
) -> list[DiscordLinkPlanRow]:
    from app.models import EmployeeProfile, User

    employee_rows = session.exec(
        select(User, EmployeeProfile)
        .join(EmployeeProfile, EmployeeProfile.user_id == User.id)
        .where(User.is_active == True)  # noqa: E712
    ).all()

    matches_by_name: dict[str, list[tuple[User, EmployeeProfile]]] = defaultdict(list)
    profiles_by_discord_id: dict[str, tuple[User, EmployeeProfile]] = {}
    for user, profile in employee_rows:
        for key in _employee_match_keys(user):
            matches_by_name[key].append((user, profile))
        discord_id = str(profile.discord_user_id or "").strip()
        if discord_id:
            profiles_by_discord_id[discord_id] = (user, profile)

    rows: list[DiscordLinkPlanRow] = []
    for member in members:
        parsed_name = parse_degen_employee_member_name(
            member,
            tag_prefix=tag_prefix,
            role_names=role_names,
        )
        if not parsed_name:
            continue

        matched_name, used_alias = _apply_name_alias(
            parsed_name,
            DEFAULT_NAME_ALIASES if name_aliases is None else name_aliases,
        )
        discord_id = str(member.discord_user_id or "").strip()
        discord_owner = profiles_by_discord_id.get(discord_id) if discord_id else None
        matches = matches_by_name.get(_normalize_name(matched_name), [])

        if not matches:
            rows.append(
                _plan_row(
                    action="needs_review",
                    confidence="none",
                    reason="no_employee_match",
                    member=member,
                    parsed_employee_name=parsed_name,
                )
            )
            continue

        unique_matches = {int(user.id): (user, profile) for user, profile in matches if user.id is not None}
        matches = list(unique_matches.values())
        if len(matches) > 1:
            rows.append(
                _plan_row(
                    action="needs_review",
                    confidence="none",
                    reason="multiple_employee_matches",
                    member=member,
                    parsed_employee_name=parsed_name,
                )
            )
            continue

        user, profile = matches[0]
        employee_user_id = int(user.id) if user.id is not None else None
        employee_display_name = _employee_label(user)
        current_discord_user_id = str(profile.discord_user_id or "").strip()

        if discord_owner and discord_owner[0].id != user.id:
            rows.append(
                _plan_row(
                    action="needs_review",
                    confidence="none",
                    reason="discord_id_already_linked_to_other_employee",
                    member=member,
                    parsed_employee_name=parsed_name,
                    employee_user_id=employee_user_id,
                    employee_display_name=employee_display_name,
                    current_discord_user_id=current_discord_user_id,
                )
            )
            continue

        if current_discord_user_id and current_discord_user_id != discord_id:
            rows.append(
                _plan_row(
                    action="needs_review",
                    confidence="none",
                    reason="employee_already_linked_to_other_discord",
                    member=member,
                    parsed_employee_name=parsed_name,
                    employee_user_id=employee_user_id,
                    employee_display_name=employee_display_name,
                    current_discord_user_id=current_discord_user_id,
                )
            )
            continue

        if current_discord_user_id == discord_id:
            rows.append(
                _plan_row(
                    action="skip",
                    confidence="exact",
                    reason="already_linked",
                    member=member,
                    parsed_employee_name=parsed_name,
                    employee_user_id=employee_user_id,
                    employee_display_name=employee_display_name,
                    current_discord_user_id=current_discord_user_id,
                )
            )
            continue

        rows.append(
            _plan_row(
                action="link",
                confidence="alias" if used_alias else "exact",
                reason="alias_display_name_match" if used_alias else "exact_display_name_match",
                member=member,
                parsed_employee_name=parsed_name,
                employee_user_id=employee_user_id,
                employee_display_name=employee_display_name,
                current_discord_user_id=current_discord_user_id,
            )
        )

    return rows


def apply_link_plan(
    session: Session,
    rows: Iterable[DiscordLinkPlanRow],
    *,
    apply: bool = False,
) -> int:
    if not apply:
        return 0

    from app.models import EmployeeProfile

    now = datetime.now(timezone.utc)
    applied = 0
    for row in rows:
        if row.action != "link" or row.confidence not in {"exact", "alias"} or row.employee_user_id is None:
            continue

        discord_id = str(row.discord_user_id or "").strip()
        if not discord_id:
            continue

        profile = session.get(EmployeeProfile, row.employee_user_id)
        if profile is None:
            continue

        current_discord_id = str(profile.discord_user_id or "").strip()
        if current_discord_id and current_discord_id != discord_id:
            continue

        existing = session.exec(
            select(EmployeeProfile).where(EmployeeProfile.discord_user_id == discord_id)
        ).first()
        if existing is not None and existing.user_id != row.employee_user_id:
            continue

        profile.discord_user_id = discord_id
        profile.discord_username = str(row.discord_username or "").strip() or None
        profile.discord_linked_at = now
        profile.updated_at = now
        session.add(profile)
        applied += 1

    return applied


def _discord_get(client: httpx.Client, path: str, **params) -> httpx.Response:
    response = client.get(f"{DISCORD_API_BASE}{path}", params={k: v for k, v in params.items() if v is not None})
    if response.status_code == 429:
        retry_after = float(response.json().get("retry_after", 1.0))
        time.sleep(min(max(retry_after, 0.25), 5.0))
        response = client.get(f"{DISCORD_API_BASE}{path}", params={k: v for k, v in params.items() if v is not None})
    if response.status_code == 401:
        raise RuntimeError("Discord rejected the bot token.")
    if response.status_code == 403:
        raise RuntimeError(
            "Discord denied the request. The bot likely needs guild access and Server Members Intent to list members."
        )
    response.raise_for_status()
    return response


def _discord_member_from_payload(
    raw_member: dict,
    *,
    role_names_by_id: dict[str, str],
) -> DiscordMember:
    user = raw_member.get("user") or {}
    username = str(user.get("username") or user.get("global_name") or "")
    return DiscordMember(
        discord_user_id=str(user.get("id") or ""),
        username=username,
        display_name=str(raw_member.get("nick") or user.get("global_name") or username),
        role_names=[
            role_names_by_id.get(str(role_id), str(role_id))
            for role_id in raw_member.get("roles") or []
        ],
    )


def fetch_discord_members(
    *,
    bot_token: str,
    guild_id: str,
    timeout_seconds: float = 30.0,
    search_query: str = "Degen Collectibles",
) -> list[DiscordMember]:
    token = str(bot_token or "").strip()
    guild = str(guild_id or "").strip()
    if not token:
        raise ValueError("bot_token is required")
    if not guild:
        raise ValueError("guild_id is required")

    headers = {
        "Authorization": f"Bot {token}",
        "User-Agent": "DegenOpsEmployeeLinker/1.0",
    }
    with httpx.Client(headers=headers, timeout=timeout_seconds) as client:
        role_payload = _discord_get(client, f"/guilds/{guild}/roles").json()
        role_names_by_id = {
            str(role.get("id") or ""): str(role.get("name") or "")
            for role in role_payload
        }

        members: list[DiscordMember] = []
        after = "0"
        try:
            while True:
                payload = _discord_get(
                    client,
                    f"/guilds/{guild}/members",
                    limit=1000,
                    after=after,
                ).json()
                if not payload:
                    break

                for raw_member in payload:
                    member = _discord_member_from_payload(raw_member, role_names_by_id=role_names_by_id)
                    members.append(member)
                    if member.discord_user_id:
                        after = member.discord_user_id

                if len(payload) < 1000:
                    break
        except RuntimeError as exc:
            if "Discord denied the request" not in str(exc) or not search_query:
                raise
            payload = _discord_get(
                client,
                f"/guilds/{guild}/members/search",
                query=search_query,
                limit=1000,
            ).json()
            members = [
                _discord_member_from_payload(raw_member, role_names_by_id=role_names_by_id)
                for raw_member in payload
            ]

    return members


def summarize_plan(rows: Iterable[DiscordLinkPlanRow]) -> dict[str, object]:
    row_list = list(rows)
    return {
        "candidate_count": len(row_list),
        "actions": dict(Counter(row.action for row in row_list)),
        "reasons": dict(Counter(row.reason for row in row_list)),
    }


def build_report(
    *,
    members: list[DiscordMember],
    rows: list[DiscordLinkPlanRow],
    applied: int,
    dry_run: bool,
    guild_id: str,
) -> dict[str, object]:
    summary = summarize_plan(rows)
    return {
        "dry_run": dry_run,
        "guild_id": guild_id,
        "member_count": len(members),
        "candidate_count": summary["candidate_count"],
        "actions": summary["actions"],
        "reasons": summary["reasons"],
        "applied": applied,
        "rows": [asdict(row) for row in rows],
    }


def format_report_markdown(report: dict[str, object]) -> str:
    actions = report.get("actions") or {}
    reasons = report.get("reasons") or {}
    lines = [
        "# Degen Ops Discord Employee Link Report",
        "",
        f"- Mode: {'dry-run' if report.get('dry_run') else 'apply'}",
        f"- Guild ID: {report.get('guild_id')}",
        f"- Discord members scanned: {report.get('member_count')}",
        f"- Tagged employee candidates: {report.get('candidate_count')}",
        f"- Rows applied: {report.get('applied')}",
        f"- Actions: {json.dumps(actions, sort_keys=True)}",
        f"- Reasons: {json.dumps(reasons, sort_keys=True)}",
        "",
        "| Action | Reason | Discord | Parsed Name | Employee | Current Link |",
        "|---|---|---|---|---|---|",
    ]
    for row in report.get("rows") or []:
        lines.append(
            "| {action} | {reason} | {discord} | {parsed} | {employee} | {current} |".format(
                action=str(row.get("action") or ""),
                reason=str(row.get("reason") or ""),
                discord=f"{row.get('discord_display_name') or ''} ({row.get('discord_user_id') or ''})",
                parsed=str(row.get("parsed_employee_name") or ""),
                employee=f"{row.get('employee_display_name') or ''} ({row.get('employee_user_id') or ''})",
                current=str(row.get("current_discord_user_id") or ""),
            )
        )
    return "\n".join(lines)


def _csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(value or "").split(",") if item.strip())


def resolve_bot_token(token_env: str) -> str:
    token = os.environ.get(token_env, "").strip()
    if token:
        return token
    if token_env == "DEGEN_OPS_DISCORD_BOT_TOKEN":
        return os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    return ""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply Discord member to employee profile links."
    )
    parser.add_argument(
        "--guild-id",
        default=os.environ.get("DEGEN_OPS_DISCORD_GUILD_ID") or os.environ.get("DISCORD_GUILD_ID") or "",
        help="Discord guild/server ID. Defaults to DEGEN_OPS_DISCORD_GUILD_ID or DISCORD_GUILD_ID.",
    )
    parser.add_argument(
        "--token-env",
        default="DEGEN_OPS_DISCORD_BOT_TOKEN",
        help="Environment variable containing the bot token. The token is never printed.",
    )
    parser.add_argument(
        "--role-names",
        default=os.environ.get("DEGEN_OPS_DISCORD_LINK_ROLE_NAMES") or ",".join(DEFAULT_ROLE_NAMES),
        help="Comma-separated Discord role names that qualify for linking.",
    )
    parser.add_argument(
        "--tag-prefix",
        default=os.environ.get("DEGEN_OPS_DISCORD_EMPLOYEE_NICK_PREFIX") or DEFAULT_TAG_PREFIX,
        help="Nickname/display-name prefix before the employee name.",
    )
    parser.add_argument(
        "--name-aliases",
        default=os.environ.get("DEGEN_OPS_DISCORD_NAME_ALIASES")
        or ",".join(f"{source}={target}" for source, target in DEFAULT_NAME_ALIASES.items()),
        help="Comma-separated parsed-name aliases, for example BossAlex=Boss Alex.",
    )
    parser.add_argument("--apply", action="store_true", help="Write exact link rows to EmployeeProfile.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of markdown.")
    parser.add_argument("--output", default="", help="Optional report file path.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = resolve_bot_token(args.token_env)
    if not token:
        raise SystemExit(f"Missing Discord bot token env var: {args.token_env}")
    if not args.guild_id:
        raise SystemExit("Missing Discord guild ID. Pass --guild-id or set DEGEN_OPS_DISCORD_GUILD_ID.")

    from app.db import managed_session

    search_query = str(args.tag_prefix or "").split("|", 1)[0].strip() or DEFAULT_TAG_PREFIX.split("|", 1)[0].strip()
    members = fetch_discord_members(
        bot_token=token,
        guild_id=args.guild_id,
        search_query=search_query,
    )
    with managed_session() as session:
        rows = build_link_plan(
            session,
            members,
            tag_prefix=args.tag_prefix,
            role_names=_csv(args.role_names),
            name_aliases=parse_name_aliases(args.name_aliases),
        )
        applied = apply_link_plan(session, rows, apply=args.apply)
        if args.apply:
            session.commit()

    report = build_report(
        members=members,
        rows=rows,
        applied=applied,
        dry_run=not args.apply,
        guild_id=args.guild_id,
    )
    body = json.dumps(report, indent=2, sort_keys=True) if args.json else format_report_markdown(report)
    if args.output:
        Path(args.output).write_text(body + "\n", encoding="utf-8")
    print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
