from __future__ import annotations

import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "audit-hardening-email-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "audit-hardening-token-hmac-key")
os.environ.setdefault("SESSION_SECRET", "audit-hardening-session-secret-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "audit-hardening-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class _FakeRequest:
    def __init__(self, current_user, *, netloc: str = "testserver"):
        self.state = SimpleNamespace(current_user=current_user)
        self.scope = {"session": {}}
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(
            path="/team/admin/invites/issue",
            scheme="https",
            netloc=netloc,
        )


class TeamPortalAuditHardeningTests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app import rate_limit
        from app.db import seed_employee_portal_defaults
        from app.models import User

        cfg.get_settings.cache_clear()
        rate_limit.reset()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = User(
            id=1,
            username="audit-admin",
            password_hash="x",
            password_salt="x",
            display_name="Audit Admin",
            role="admin",
            is_active=True,
        )
        self.employee = User(
            id=2,
            username="audit-employee",
            password_hash="x",
            password_salt="x",
            display_name="Audit Employee",
            role="employee",
            is_active=True,
        )
        self.session.add_all([self.admin, self.employee])
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_supply_submit_uses_request_alerts_without_legacy_telegram(self):
        from app.routers import team

        request = SimpleNamespace(
            state=SimpleNamespace(current_user=self.employee),
            client=SimpleNamespace(host="testclient"),
        )

        with patch("app.tiktok.tiktok_alerts._send_telegram") as telegram_send, patch.object(
            team, "send_supply_request_alert"
        ) as request_alert:
            response = asyncio.run(
                team.team_supply_post(
                    request,
                    title="Bubble mailers",
                    description="running low",
                    urgency="high",
                    session=self.session,
                )
            )

        self.assertEqual(response.status_code, 303)
        request_alert.assert_called_once()
        telegram_send.assert_not_called()

    def test_permission_catalog_has_no_decoy_action_keys_and_seed_prunes_legacy_rows(self):
        from app import permissions as perms
        from app.db import DEFAULT_ROLE_PERMISSIONS, seed_employee_portal_defaults
        from app.models import RolePermission

        decoys = {
            "action.supply_request.approve",
            "action.pii.reveal",
            "action.password.reset_issued",
            "action.employee.terminate",
            "action.employee.purge",
        }
        self.assertTrue(decoys.isdisjoint(set(perms.RESOURCE_KEYS)))
        self.assertTrue(
            decoys.isdisjoint({resource_key for _, resource_key, _ in DEFAULT_ROLE_PERMISSIONS})
        )

        for key in decoys:
            self.session.add(
                RolePermission(role="admin", resource_key=key, is_allowed=True)
            )
        self.session.commit()

        seed_employee_portal_defaults(self.session)

        stale = self.session.exec(
            select(RolePermission).where(RolePermission.resource_key.in_(decoys))
        ).all()
        self.assertEqual(stale, [])

    def test_manual_invite_uses_public_base_url_not_request_host(self):
        from app.routers import team_admin_invites

        settings = SimpleNamespace(public_base_url="https://team.example.test")
        request = _FakeRequest(self.admin, netloc="evil.example")

        with patch.object(team_admin_invites, "get_settings", return_value=settings, create=True):
            response = asyncio.run(
                team_admin_invites.admin_invites_issue(
                    request,
                    role="employee",
                    email_hint="invite@example.test",
                    session=self.session,
                )
            )

        body = response.body.decode("utf-8")
        self.assertIn("https://team.example.test/team/invite/accept/", body)
        self.assertNotIn("evil.example/team/invite/accept/", body)

    def test_admin_reset_link_uses_public_base_url_not_request_host(self):
        from app.routers import team_admin_employees

        settings = SimpleNamespace(public_base_url="https://team.example.test")
        request = _FakeRequest(self.admin, netloc="evil.example")

        with patch.object(team_admin_employees, "get_settings", return_value=settings):
            response = asyncio.run(
                team_admin_employees.admin_employee_reset_password(
                    request,
                    self.employee.id,
                    session=self.session,
                )
            )

        body = response.body.decode("utf-8")
        self.assertIn("https://team.example.test/team/password/reset/", body)
        self.assertNotIn("evil.example/team/password/reset/", body)

    def test_supply_deny_does_not_record_denier_as_approver(self):
        from app.models import SupplyRequest
        from app.routers.team_admin_supply import _transition

        row = SupplyRequest(
            submitted_by_user_id=self.employee.id,
            title="Label rolls",
            description="out",
            urgency="normal",
            status="submitted",
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)

        _transition(
            self.session,
            request_id=row.id,
            actor=self.admin,
            new_status="denied",
            action="supply.denied",
            request=_FakeRequest(self.admin),
        )

        self.session.expire_all()
        refreshed = self.session.get(SupplyRequest, row.id)
        self.assertEqual(refreshed.status, "denied")
        self.assertIsNone(refreshed.approved_by_user_id)

    def test_team_dashboard_supply_count_matches_submitted_queue_only(self):
        from app.models import SupplyRequest
        from app.routers import team

        self.session.add_all(
            [
                SupplyRequest(
                    submitted_by_user_id=self.employee.id,
                    title="Submitted",
                    status="submitted",
                ),
                SupplyRequest(
                    submitted_by_user_id=self.employee.id,
                    title="Legacy pending",
                    status="pending",
                ),
            ]
        )
        self.session.commit()

        captured = {}

        def fake_template_response(request, template, context):
            captured.update(context)
            return SimpleNamespace(status_code=200, body=b"")

        request = _FakeRequest(self.admin)
        with patch.object(team.templates, "TemplateResponse", side_effect=fake_template_response), patch.object(
            team, "_employee_dashboard_pay_summary", return_value={}
        ), patch.object(
            team, "_active_announcements_for", return_value=[]
        ):
            team.team_dashboard(request, session=self.session)

        self.assertEqual(captured["supply_queue_count"], 1)

    def test_sms_and_email_fingerprints_are_hmac_keyed(self):
        from app.team import email as email_mod
        from app.team import sms as sms_mod

        settings_a = SimpleNamespace(employee_token_hmac_key="fingerprint-key-a")
        settings_b = SimpleNamespace(employee_token_hmac_key="fingerprint-key-b")

        with patch.object(email_mod, "get_settings", return_value=settings_a):
            email_a = email_mod.email_address_fingerprint("person@example.test")
        with patch.object(email_mod, "get_settings", return_value=settings_b):
            email_b = email_mod.email_address_fingerprint("person@example.test")
        with patch.object(sms_mod, "get_settings", return_value=settings_a):
            phone_a = sms_mod.sms_phone_fingerprint("+15550100123")
        with patch.object(sms_mod, "get_settings", return_value=settings_b):
            phone_b = sms_mod.sms_phone_fingerprint("+15550100123")

        self.assertNotEqual(email_a, email_b)
        self.assertNotEqual(phone_a, phone_b)
        self.assertRegex(email_a, r"^[0-9a-f]{16}$")
        self.assertRegex(phone_a, r"^[0-9a-f]{16}$")


def test_alert_email_dry_run_is_visible(capsys, monkeypatch):
    from app.team import request_alerts
    from app.team.email import EmailSendResult

    def fake_send_email(**kwargs):
        return EmailSendResult(provider="dry_run", status="dry_run", dry_run=True)

    monkeypatch.setattr(request_alerts, "send_email", fake_send_email)

    result = request_alerts._send_alert_email(
        subject="Dry run",
        body="body",
        settings=SimpleNamespace(
            team_request_alert_email_enabled=True,
            team_request_alert_email_to="ops@example.test",
        ),
    )

    assert result.dry_run is True
    assert "email alert skipped: dry_run" in capsys.readouterr().out


def test_password_reset_sms_helper_removed() -> None:
    from app.routers import team

    assert not hasattr(team, "_try_send_password_reset_sms")
