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
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "invite-sms-email-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "invite-sms-token-hmac")
os.environ.setdefault("SESSION_SECRET", "invite-sms-session-secret-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "invite-sms-admin-password")


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
    def __init__(self, current_user):
        self.state = SimpleNamespace(current_user=current_user)
        self.scope = {"session": {}}
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(
            path="/team/admin/employees/2",
            scheme="http",
            netloc="testserver",
        )


class EmployeeInviteSmsTests(unittest.TestCase):
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
            username="sms-admin",
            password_hash="x",
            password_salt="x",
            display_name="SMS Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(self.admin)
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def _request(self):
        return _FakeRequest(self.admin)

    def _settings(self):
        return SimpleNamespace(
            public_base_url="https://team.example.test",
            sms_provider="dry_run",
            sms_from_number="",
            sms_twilio_account_sid="",
            sms_twilio_auth_token="",
            sms_twilio_messaging_service_sid="",
            sms_timeout_seconds=1,
        )

    def _draft_with_phone(self, phone: str):
        from app.auth import create_draft_employee
        from app.models import EmployeeProfile
        from app.team.pii import encrypt_pii

        draft = create_draft_employee(
            self.session,
            created_by_user_id=self.admin.id,
            display_name="Invite SMS Person",
        )
        profile = self.session.get(EmployeeProfile, draft.id)
        profile.phone_enc = encrypt_pii(phone)
        self.session.add(profile)
        self.session.commit()
        return draft

    def test_text_invite_creates_unique_token_and_sends_safe_sms(self):
        from app.models import AuditLog, InviteToken
        from app.routers import team_admin_employees as mod
        from app.team.sms import SmsSendResult

        draft = self._draft_with_phone("(555) 867-5309")
        sent: dict[str, str] = {}

        def fake_send_sms(*, to_phone, body, settings=None):
            sent["to_phone"] = to_phone
            sent["body"] = body
            return SmsSendResult(provider="dry_run", status="dry_run", dry_run=True)

        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_sms", side_effect=fake_send_sms
        ):
            response = asyncio.run(
                mod.admin_employee_text_invite(
                    self._request(),
                    draft.id,
                    session=self.session,
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(sent["to_phone"], "+15558675309")
        self.assertIn("https://team.example.test/team/invite/accept/", sent["body"])

        invites = list(
            self.session.exec(
                select(InviteToken).where(InviteToken.target_user_id == draft.id)
            ).all()
        )
        self.assertEqual(len(invites), 1)

        audit_rows = list(
            self.session.exec(
                select(AuditLog).where(AuditLog.target_user_id == draft.id)
            ).all()
        )
        actions = {row.action for row in audit_rows}
        self.assertIn("pii.use_for_invite_sms", actions)
        self.assertIn("invite.issued_for_draft", actions)
        self.assertIn("invite.text_dry_run", actions)
        details_blob = "\n".join(row.details_json for row in audit_rows)
        self.assertNotIn("https://team.example.test/team/invite/accept/", details_blob)
        self.assertNotIn("5558675309", details_blob)
        self.assertIn("***-***-5309", details_blob)

        text_audit = next(row for row in audit_rows if row.action == "invite.text_dry_run")
        details = json.loads(text_audit.details_json)
        self.assertTrue(details["dry_run"])
        self.assertTrue(details["success"])
        self.assertEqual(details["phone"], "***-***-5309")
        self.assertIn("phone_fingerprint", details)

    def test_text_invite_rejects_invalid_phone_without_issuing_token(self):
        from app.models import AuditLog, InviteToken
        from app.routers import team_admin_employees as mod

        draft = self._draft_with_phone("not a phone")
        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_sms"
        ) as send_sms:
            response = asyncio.run(
                mod.admin_employee_text_invite(
                    self._request(),
                    draft.id,
                    session=self.session,
                )
            )

        self.assertEqual(response.status_code, 303)
        send_sms.assert_not_called()
        invites = list(
            self.session.exec(
                select(InviteToken).where(InviteToken.target_user_id == draft.id)
            ).all()
        )
        self.assertEqual(invites, [])
        failure = self.session.exec(
            select(AuditLog).where(
                AuditLog.target_user_id == draft.id,
                AuditLog.action == "invite.text_failed",
            )
        ).first()
        self.assertIsNotNone(failure)
        self.assertEqual(json.loads(failure.details_json)["reason"], "invalid_phone")

    def test_employee_list_exposes_text_invite_for_drafts_with_phone(self):
        from app.routers.team_admin_employees import admin_employees_list

        draft = self._draft_with_phone("555-867-5309")
        response = admin_employees_list(
            self._request(),
            q=None,
            flash=None,
            show_inactive=None,
            session=self.session,
        )

        self.assertEqual(response.status_code, 200)
        html = response.body.decode("utf-8")
        self.assertIn(f"/team/admin/employees/{draft.id}/text-invite", html)
        self.assertIn("Text invite", html)


class PasswordResetEmailTests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app import rate_limit
        from app.db import seed_employee_portal_defaults

        cfg.get_settings.cache_clear()
        rate_limit.reset()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _request(self):
        return SimpleNamespace(
            client=SimpleNamespace(host="testclient"),
            headers={},
            url=SimpleNamespace(scheme="http", netloc="testserver"),
        )

    def _settings(self):
        return SimpleNamespace(
            employee_portal_enabled=True,
            public_base_url="https://team.example.test",
            sms_provider="twilio",
            sms_from_number="+15552022027",
            sms_twilio_account_sid="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            sms_twilio_auth_token="test-token",
            sms_twilio_messaging_service_sid="",
            sms_timeout_seconds=1,
            password_reset_email_provider="smtp",
            password_reset_email_from="team@example.test",
            password_reset_email_from_name="Degen Team",
            password_reset_smtp_host="smtp.example.test",
            password_reset_smtp_port=587,
            password_reset_smtp_username="smtp-user",
            password_reset_smtp_password="smtp-pass",
            password_reset_smtp_starttls=True,
            password_reset_smtp_ssl=False,
            password_reset_email_timeout_seconds=1,
        )

    def _active_employee_with_contact(
        self,
        username: str,
        *,
        email: str = "reset@example.com",
        phone: str = "(555) 867-5309",
    ):
        from app.auth import hash_password
        from app.models import EmployeeProfile, User
        from app.team.pii import email_lookup_hash, encrypt_pii

        password_hash, salt = hash_password("OldPassword1!")
        employee = User(
            username=username,
            password_hash=password_hash,
            password_salt=salt,
            display_name="Reset SMS Person",
            role="employee",
            is_active=True,
        )
        self.session.add(employee)
        self.session.commit()
        self.session.refresh(employee)
        profile = EmployeeProfile(user_id=employee.id)
        if phone:
            profile.phone_enc = encrypt_pii(phone)
        if email:
            profile.email_ciphertext = encrypt_pii(email)
            profile.email_lookup_hash = email_lookup_hash(email)
        self.session.add(profile)
        self.session.commit()
        return employee

    def test_forgot_password_emails_reset_link_without_manager_queue(self):
        from app.models import AuditLog, PasswordResetToken
        from app.routers import team as mod
        from app.team.email import EmailSendResult

        employee = self._active_employee_with_contact("email-reset-ok")
        sent: dict[str, str] = {}

        def fake_send_email(*, to_email, subject, body, settings=None):
            sent["to_email"] = to_email
            sent["subject"] = subject
            sent["body"] = body
            return EmailSendResult(provider="smtp", status="sent", message_id="MSG123")

        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_email", side_effect=fake_send_email, create=True
        ), patch.object(
            mod, "send_sms"
        ) as send_sms, patch.object(
            mod, "send_password_reset_manager_request_alert"
        ) as alert_mock:
            response = asyncio.run(
                mod.team_password_forgot_post(
                    self._request(),
                    identifier=employee.username,
                    session=self.session,
                )
            )

        send_sms.assert_not_called()
        alert_mock.assert_not_called()
        self.assertEqual(response.status_code, 303)
        self.assertEqual(sent["to_email"], "reset@example.com")
        self.assertEqual(sent["subject"], "Reset your Degen Team password")
        self.assertIn("https://team.example.test/team/password/reset/", sent["body"])

        tokens = list(
            self.session.exec(
                select(PasswordResetToken).where(PasswordResetToken.user_id == employee.id)
            ).all()
        )
        self.assertEqual(len(tokens), 1)
        self.assertIsNone(tokens[0].used_at)

        audit_rows = list(
            self.session.exec(
                select(AuditLog).where(AuditLog.target_user_id == employee.id)
            ).all()
        )
        actions = {row.action for row in audit_rows}
        self.assertIn("password.reset_email_sent", actions)
        self.assertNotIn("password.reset_manager_request", actions)
        details_blob = "\n".join(row.details_json for row in audit_rows)
        self.assertNotIn("/team/password/reset/", details_blob)
        self.assertNotIn("reset@example.com", details_blob)
        self.assertIn("r***@example.com", details_blob)

    def test_forgot_password_does_not_email_reset_link_without_public_base_url(self):
        from app.models import AuditLog, PasswordResetToken
        from app.routers import team as mod
        from app.team.email import EmailSendResult

        employee = self._active_employee_with_contact("email-reset-no-base-url")
        settings = self._settings()
        settings.public_base_url = ""

        def fake_send_email(*, to_email, subject, body, settings=None):
            return EmailSendResult(provider="smtp", status="sent", message_id="MSG123")

        with patch.object(mod, "get_settings", return_value=settings), patch.object(
            mod, "send_email", side_effect=fake_send_email, create=True
        ) as send_email, patch.object(
            mod, "send_sms"
        ) as send_sms:
            response = asyncio.run(
                mod.team_password_forgot_post(
                    self._request(),
                    identifier=employee.username,
                    session=self.session,
                )
            )

        send_email.assert_not_called()
        send_sms.assert_not_called()
        self.assertEqual(response.status_code, 303)
        tokens = list(
            self.session.exec(
                select(PasswordResetToken).where(PasswordResetToken.user_id == employee.id)
            ).all()
        )
        self.assertEqual(tokens, [])

        manager_request = self.session.exec(
            select(AuditLog).where(
                AuditLog.target_user_id == employee.id,
                AuditLog.action == "password.reset_manager_request",
            )
        ).one()
        self.assertIn("missing_public_base_url", manager_request.details_json)

    def test_failed_reset_email_revokes_undelivered_token_and_queues_manager(self):
        from app.models import AuditLog, PasswordResetToken
        from app.routers import team as mod
        from app.team.email import EmailSendResult

        employee = self._active_employee_with_contact("email-reset-fail")

        def fake_send_email(*, to_email, subject, body, settings=None):
            return EmailSendResult(
                provider="smtp",
                status="smtp_error",
                error="Mailbox unavailable",
            )

        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_email", side_effect=fake_send_email, create=True
        ), patch.object(
            mod, "send_sms"
        ) as send_sms, patch.object(
            mod, "send_password_reset_manager_request_alert"
        ) as alert_mock:
            response = asyncio.run(
                mod.team_password_forgot_post(
                    self._request(),
                    identifier=employee.username,
                    session=self.session,
                )
            )

        send_sms.assert_not_called()
        self.assertEqual(response.status_code, 303)
        tokens = list(
            self.session.exec(
                select(PasswordResetToken).where(PasswordResetToken.user_id == employee.id)
            ).all()
        )
        self.assertEqual(len(tokens), 1)
        self.assertIsNotNone(tokens[0].used_at)

        audit_rows = list(
            self.session.exec(
                select(AuditLog).where(AuditLog.target_user_id == employee.id)
            ).all()
        )
        actions = {row.action for row in audit_rows}
        self.assertIn("password.reset_email_failed", actions)
        self.assertIn("password.reset_manager_request", actions)
        manager_request = next(
            row for row in audit_rows if row.action == "password.reset_manager_request"
        )
        alert_mock.assert_called_once_with(
            request_id=manager_request.id,
            employee_name=employee.display_name or employee.username,
            employee_username=employee.username,
            reason="email_delivery_unavailable",
        )
        details_blob = "\n".join(row.details_json for row in audit_rows)
        self.assertNotIn("/team/password/reset/", details_blob)
        self.assertNotIn("reset@example.com", details_blob)

    def test_forgot_password_does_not_fall_back_to_sms_when_email_unavailable(self):
        from app.models import AuditLog, PasswordResetToken
        from app.routers import team as mod

        employee = self._active_employee_with_contact(
            "email-reset-no-email",
            email="",
            phone="(555) 867-5309",
        )

        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_sms"
        ) as send_sms:
            response = asyncio.run(
                mod.team_password_forgot_post(
                    self._request(),
                    identifier=employee.username,
                    session=self.session,
                )
            )

        send_sms.assert_not_called()
        self.assertEqual(response.status_code, 303)
        tokens = list(
            self.session.exec(
                select(PasswordResetToken).where(PasswordResetToken.user_id == employee.id)
            ).all()
        )
        self.assertEqual(tokens, [])
        manager_request = self.session.exec(
            select(AuditLog).where(
                AuditLog.target_user_id == employee.id,
                AuditLog.action == "password.reset_manager_request",
            )
        ).first()
        self.assertIsNotNone(manager_request)

    def test_legacy_sms_reset_helper_still_revokes_failed_sms_token(self):
        from app.models import AuditLog, PasswordResetToken
        from app.routers import team as mod
        from app.team.sms import SmsSendResult

        employee = self._active_employee_with_contact(
            "sms-helper-fail",
            email="",
            phone="(555) 867-5309",
        )

        def fake_send_sms(*, to_phone, body, settings=None):
            return SmsSendResult(
                provider="twilio",
                status="http_400",
                error="The destination number is blocked.",
            )

        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_sms", side_effect=fake_send_sms
        ):
            delivered = mod._try_send_password_reset_sms(
                self.session,
                request=self._request(),
                user=employee,
                probe_hash="probe-hash",
            )

        self.assertFalse(delivered)
        tokens = list(
            self.session.exec(
                select(PasswordResetToken).where(PasswordResetToken.user_id == employee.id)
            ).all()
        )
        self.assertEqual(len(tokens), 1)
        self.assertIsNotNone(tokens[0].used_at)

        audit_rows = list(
            self.session.exec(
                select(AuditLog).where(AuditLog.target_user_id == employee.id)
            ).all()
        )
        actions = {row.action for row in audit_rows}
        self.assertIn("password.reset_sms_failed", actions)
        details_blob = "\n".join(row.details_json for row in audit_rows)
        self.assertNotIn("/team/password/reset/", details_blob)
        self.assertNotIn("5558675309", details_blob)


if __name__ == "__main__":
    unittest.main()
