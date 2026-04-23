"""Wave 4 — admin employee mgmt, invites, supply queue, PII reveal."""
from __future__ import annotations

import importlib
import json
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")
os.environ.setdefault("SESSION_SECRET", "test-secret-wave4-" + "x" * 32)


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _load_app():
    from app import config as cfg
    cfg.get_settings.cache_clear()
    import app.main as app_main
    importlib.reload(app_main)
    return app_main


class _W4Harness:
    def _setup(self):
        from app import rate_limit
        rate_limit.reset()
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.app_main = _load_app()
        from app.db import get_session as real_get_session

        def _override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _override
        from fastapi.testclient import TestClient
        self.client = TestClient(self.app_main.app)

    def _teardown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in ("_patcher_shared", "_patcher_main"):
            patcher = getattr(self, attr, None)
            if patcher:
                patcher.stop()
                setattr(self, attr, None)

    def _login(self, *, role: str, user_id: int = 100, username: str = "admin_t"):
        from app import shared
        from app.auth import hash_password
        from app.models import User
        import app.main as app_main

        password = "TestPass!234"
        existing = self.session.get(User, user_id)
        if existing is None:
            password_hash, password_salt = hash_password(password)
            user = User(
                id=user_id,
                username=username,
                password_hash=password_hash,
                password_salt=password_salt,
                display_name=username,
                role=role,
                is_active=True,
                session_version=1,
            )
            self.session.add(user)
            self.session.commit()
            self.session.refresh(user)
        else:
            password_hash, password_salt = hash_password(password)
            existing.username = username
            existing.password_hash = password_hash
            existing.password_salt = password_salt
            existing.display_name = username
            existing.role = role
            existing.is_active = True
            existing.session_version = int(getattr(existing, "session_version", 0) or 1)
            self.session.add(existing)
            self.session.commit()
            self.session.refresh(existing)
            user = existing

        self._patcher_shared = patch.object(shared, "get_request_user", return_value=user)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=user)
        self._patcher_main.start()

        csrf = self._csrf()
        response = self.client.post(
            "/team/login",
            data={
                "username": username,
                "password": password,
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        assert response.status_code == 303, response.text[:300]
        return user

    def _csrf(self) -> str:
        marker = 'name="csrf_token" value="'
        for path in ("/team/login", "/team/password/forgot"):
            r = self.client.get(path, follow_redirects=False)
            if marker in r.text:
                idx = r.text.index(marker) + len(marker)
                end = r.text.index('"', idx)
                return r.text[idx:end]
        raise AssertionError("no csrf token rendered")

    def _seed_employee(self, *, user_id: int = 500, username: str = "tgt", role: str = "employee"):
        from app.models import EmployeeProfile, User
        from app.pii import encrypt_pii
        from datetime import date

        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        self.session.add(u)
        p = EmployeeProfile(
            user_id=user_id,
            phone_enc=encrypt_pii("555-867-5309"),
            address_enc=encrypt_pii(json.dumps({"street": "1 Main St", "city": "Town", "state": "CA", "zip": "90210"})),
            legal_name_enc=encrypt_pii("Jane Q Test"),
            hire_date=date(2024, 1, 15),
        )
        self.session.add(p)
        self.session.commit()
        return u


class EmployeeListTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_admin_can_list_employees(self):
        self._login(role="admin", user_id=101, username="adm1")
        self._seed_employee(user_id=501, username="emp501")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        self.assertIn("emp501", r.text)

    def test_manager_can_list_employees(self):
        self._login(role="manager", user_id=102, username="mgr1")
        self._seed_employee(user_id=502, username="emp502")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)

    def test_manager_list_hides_add_employee_and_schedulable_toggle(self):
        self._login(role="manager", user_id=104, username="mgr_list")
        emp = self._seed_employee(user_id=503, username="emp503")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn('href="/team/admin/employees/new"', r.text)
        self.assertNotIn(
            f'action="/team/admin/employees/{emp.id}/schedulable-toggle"',
            r.text,
        )

    def test_admin_list_still_shows_add_employee_and_schedulable_toggle(self):
        self._login(role="admin", user_id=105, username="adm_list")
        emp = self._seed_employee(user_id=504, username="emp504")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        self.assertIn('href="/team/admin/employees/new"', r.text)
        self.assertIn(
            f'action="/team/admin/employees/{emp.id}/schedulable-toggle"',
            r.text,
        )

    def test_manager_list_hides_admin_only_controls(self):
        self._login(role="manager", user_id=104, username="mgr_hidden")
        self._seed_employee(user_id=504, username="emp504")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn('action="/team/admin/employees/504/schedulable-toggle"', r.text)
        self.assertNotIn('href="/team/admin/employees/new"', r.text)

    def test_admin_list_shows_edit_controls(self):
        self._login(role="admin", user_id=105, username="adm_list")
        self._seed_employee(user_id=505, username="emp505")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        self.assertIn('action="/team/admin/employees/505/schedulable-toggle"', r.text)
        self.assertIn('href="/team/admin/employees/new"', r.text)

    def test_employee_cannot_list(self):
        self._login(role="employee", user_id=103, username="emp103")
        r = self.client.get("/team/admin/employees", follow_redirects=False)
        self.assertEqual(r.status_code, 403)


class DetailAndRevealTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_detail_masks_pii_by_default(self):
        self._login(role="admin", user_id=201, username="adm2")
        emp = self._seed_employee(user_id=601, username="emp601")
        r = self.client.get(f"/team/admin/employees/{emp.id}")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("555-867-5309", r.text)
        self.assertNotIn("1 Main St", r.text)
        self.assertIn("Redacted", r.text)

    def test_reveal_writes_audit_and_shows_plaintext(self):
        from app.models import AuditLog
        self._login(role="admin", user_id=202, username="adm3")
        emp = self._seed_employee(user_id=602, username="emp602")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reveal",
            data={"field": "phone", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn("555-867-5309", r.text)
        # Audit row present.
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "pii.reveal")
        ).all())
        self.assertEqual(len(rows), 1)
        self.assertIn("phone", rows[0].details_json)

    def test_reveal_without_csrf_rejected_and_no_audit(self):
        from app.models import AuditLog
        self._login(role="admin", user_id=203, username="adm4")
        emp = self._seed_employee(user_id=603, username="emp603")
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reveal",
            data={"field": "phone"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "pii.reveal")
        ).all())
        self.assertEqual(len(rows), 0)

    def test_manager_cannot_reveal_pii(self):
        self._login(role="manager", user_id=204, username="mgr2")
        emp = self._seed_employee(user_id=604, username="emp604")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reveal",
            data={"field": "phone", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_manager_detail_hides_forbidden_controls(self):
        self._login(role="manager", user_id=205, username="mgr_detail")
        emp = self._seed_employee(user_id=605, username="emp605")
        r = self.client.get(f"/team/admin/employees/{emp.id}")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(
            f'action="/team/admin/employees/{emp.id}/profile-update"',
            r.text,
        )
        self.assertNotIn(
            f'action="/team/admin/employees/{emp.id}/pii-update"',
            r.text,
        )
        self.assertNotIn(
            f'action="/team/admin/employees/{emp.id}/reveal"',
            r.text,
        )
        self.assertNotIn(
            f'action="/team/admin/employees/{emp.id}/reset-password"',
            r.text,
        )
        self.assertNotIn(
            f'href="/team/admin/employees/{emp.id}/terminate"',
            r.text,
        )
        self.assertNotIn(
            f'href="/team/admin/employees/{emp.id}/purge"',
            r.text,
        )

    def test_admin_detail_still_shows_allowed_controls(self):
        self._login(role="admin", user_id=206, username="adm_detail")
        emp = self._seed_employee(user_id=606, username="emp606")
        r = self.client.get(f"/team/admin/employees/{emp.id}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(
            f'action="/team/admin/employees/{emp.id}/profile-update"',
            r.text,
        )
        self.assertIn(
            f'action="/team/admin/employees/{emp.id}/pii-update"',
            r.text,
        )
        self.assertIn(
            f'action="/team/admin/employees/{emp.id}/reveal"',
            r.text,
        )
        self.assertIn(
            f'action="/team/admin/employees/{emp.id}/reset-password"',
            r.text,
        )
        self.assertIn(
            f'href="/team/admin/employees/{emp.id}/terminate"',
            r.text,
        )
        self.assertIn(
            f'href="/team/admin/employees/{emp.id}/purge"',
            r.text,
        )

    def test_manager_detail_hides_invite_button_for_draft_employee(self):
        self._login(role="manager", user_id=207, username="mgr_inv")
        from app.auth import create_draft_employee

        draft = create_draft_employee(
            self.session,
            created_by_user_id=207,
            display_name="Drafty",
            legal_name="Draft Person",
            role="employee",
        )
        r = self.client.get(f"/team/admin/employees/{draft.id}")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(
            f'action="/team/admin/employees/{draft.id}/send-invite"',
            r.text,
        )

    def test_admin_detail_shows_invite_button_for_draft_employee(self):
        self._login(role="admin", user_id=208, username="adm_inv")
        from app.auth import create_draft_employee

        draft = create_draft_employee(
            self.session,
            created_by_user_id=208,
            display_name="Drafty",
            legal_name="Draft Person",
            role="employee",
        )
        r = self.client.get(f"/team/admin/employees/{draft.id}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(
            f'action="/team/admin/employees/{draft.id}/send-invite"',
            r.text,
        )

    def test_manager_detail_hides_admin_only_controls(self):
        self._login(role="manager", user_id=205, username="mgr_detail")
        emp = self._seed_employee(user_id=605, username="emp605")
        r = self.client.get(f"/team/admin/employees/{emp.id}")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(f'action="/team/admin/employees/{emp.id}/reveal"', r.text)
        self.assertNotIn(f'action="/team/admin/employees/{emp.id}/profile-update"', r.text)
        self.assertNotIn(f'action="/team/admin/employees/{emp.id}/pii-update"', r.text)
        self.assertNotIn(f'action="/team/admin/employees/{emp.id}/reset-password"', r.text)
        self.assertNotIn(f'href="/team/admin/employees/{emp.id}/terminate"', r.text)
        self.assertNotIn(f'href="/team/admin/employees/{emp.id}/purge"', r.text)

    def test_admin_detail_shows_allowed_controls(self):
        self._login(role="admin", user_id=206, username="adm_detail")
        emp = self._seed_employee(user_id=606, username="emp606")
        r = self.client.get(f"/team/admin/employees/{emp.id}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(f'action="/team/admin/employees/{emp.id}/reveal"', r.text)
        self.assertIn(f'action="/team/admin/employees/{emp.id}/profile-update"', r.text)
        self.assertIn(f'action="/team/admin/employees/{emp.id}/pii-update"', r.text)
        self.assertIn(f'action="/team/admin/employees/{emp.id}/reset-password"', r.text)
        self.assertIn(f'href="/team/admin/employees/{emp.id}/terminate"', r.text)
        self.assertIn(f'href="/team/admin/employees/{emp.id}/purge"', r.text)


class DraftInviteControlTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_manager_detail_hides_send_invite_controls_for_draft(self):
        from app.auth import create_draft_employee

        self._login(role="manager", user_id=260, username="mgr_invite")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=260,
            display_name="Draft Invitee",
            legal_name="Draft Invitee",
            role="employee",
        )
        r = self.client.get(f"/team/admin/employees/{draft.id}")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(f'action="/team/admin/employees/{draft.id}/send-invite"', r.text)

    def test_admin_detail_shows_send_invite_controls_for_draft(self):
        from app.auth import create_draft_employee

        self._login(role="admin", user_id=261, username="adm_invite")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=261,
            display_name="Draft Invitee",
            legal_name="Draft Invitee",
            role="employee",
        )
        r = self.client.get(f"/team/admin/employees/{draft.id}")
        self.assertEqual(r.status_code, 200)
        self.assertIn(f'action="/team/admin/employees/{draft.id}/send-invite"', r.text)


class ResetPasswordTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_admin_reset_shows_link_once_and_audits(self):
        from app.models import AuditLog
        self._login(role="admin", user_id=301, username="adm5")
        emp = self._seed_employee(user_id=701, username="emp701")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reset-password",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn("/team/password/reset/", r.text)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_issued")
        ).all())
        self.assertGreaterEqual(len(rows), 1)


class TerminateAndPurgeTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_terminate_sets_inactive_and_audits(self):
        from app.models import AuditLog, User
        self._login(role="admin", user_id=401, username="adm6")
        emp = self._seed_employee(user_id=801, username="emp801")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/terminate",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        follow = self.client.get(r.headers["location"])
        self.assertEqual(follow.status_code, 200)
        self.session.expire_all()
        refreshed = self.session.get(User, emp.id)
        self.assertFalse(refreshed.is_active)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.terminated")
        ).all())
        self.assertEqual(len(rows), 1)

    def test_purge_wrong_username_rejected_and_preserves_pii(self):
        from app.models import AuditLog, EmployeeProfile
        self._login(role="admin", user_id=402, username="adm7")
        emp = self._seed_employee(user_id=802, username="emp802")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/purge",
            data={"csrf_token": csrf, "confirm_username": "wrong"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 400)
        self.session.expire_all()
        p = self.session.get(EmployeeProfile, emp.id)
        self.assertIsNotNone(p.phone_enc)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.purged")
        ).all())
        self.assertEqual(len(rows), 0)

    def test_purge_correct_username_wipes_pii_and_audits(self):
        from app.models import AuditLog, EmployeeProfile, User
        self._login(role="admin", user_id=403, username="adm8")
        emp = self._seed_employee(user_id=803, username="emp803")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/purge",
            data={"csrf_token": csrf, "confirm_username": "emp803"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        follow = self.client.get(r.headers["location"])
        self.assertEqual(follow.status_code, 200)
        self.session.expire_all()
        p = self.session.get(EmployeeProfile, emp.id)
        self.assertIsNone(p.phone_enc)
        self.assertIsNone(p.address_enc)
        self.assertIsNone(p.legal_name_enc)
        refreshed = self.session.get(User, emp.id)
        self.assertFalse(refreshed.is_active)
        purge_rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.purged")
        ).all())
        self.assertEqual(len(purge_rows), 1)
        # AuditLog as a whole should remain readable (not wiped).
        all_rows = list(self.session.exec(select(AuditLog)).all())
        self.assertGreaterEqual(len(all_rows), 1)


class InviteTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_issue_invite_shows_url_once_and_audits(self):
        from app.models import AuditLog, InviteToken
        self._login(role="admin", user_id=501, username="adm_inv")
        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/invites/issue",
            data={"csrf_token": csrf, "role": "employee", "email_hint": "new@example.com"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn("/team/invite/accept/", r.text)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "invite.issued")
        ).all())
        self.assertEqual(len(rows), 1)
        tokens = list(self.session.exec(select(InviteToken)).all())
        self.assertEqual(len(tokens), 1)

    def test_revoke_marks_used_and_audits(self):
        from app.auth import generate_invite_token
        from app.models import AuditLog, InviteToken

        self._login(role="admin", user_id=502, username="adm_rev")
        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=502
        )
        row = self.session.exec(select(InviteToken)).first()
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/invites/{row.id}/revoke",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(InviteToken, row.id)
        self.assertIsNotNone(refreshed.used_at)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "invite.revoked")
        ).all())
        self.assertEqual(len(rows), 1)

    def test_invites_list_renders_with_naive_expires_at(self):
        """Regression: SQLite returns `expires_at` as tz-naive, while
        `utcnow()` is tz-aware. The invites list page previously
        exploded with `TypeError: can't compare offset-naive and
        offset-aware datetimes`. It must not anymore."""
        from datetime import datetime, timedelta, timezone
        from app.models import InviteToken

        self._login(role="admin", user_id=503, username="adm_tz")

        # Build a tz-naive "now" the way SQLite hands it back (by stripping
        # tzinfo from an aware UTC datetime).
        naive_now = datetime.now(timezone.utc).replace(tzinfo=None)

        expired_row = InviteToken(
            token_hash="a" * 64,
            role="employee",
            email_hint="old@example.com",
            created_by_user_id=503,
            expires_at=naive_now - timedelta(days=1),
        )
        fresh_row = InviteToken(
            token_hash="b" * 64,
            role="employee",
            email_hint="new@example.com",
            created_by_user_id=503,
            expires_at=naive_now + timedelta(hours=6),
        )
        self.session.add(expired_row)
        self.session.add(fresh_row)
        self.session.commit()

        r = self.client.get("/team/admin/invites")
        self.assertEqual(r.status_code, 200)
        self.assertIn("expired", r.text)
        self.assertIn("outstanding", r.text)


class SupplyQueueTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def _seed_request(self, submitted_by: int = 901, title: str = "Tape") -> int:
        from app.models import SupplyRequest
        row = SupplyRequest(
            submitted_by_user_id=submitted_by,
            title=title,
            description="need more",
            urgency="normal",
            status="submitted",
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row.id

    def test_manager_sees_pending(self):
        self._login(role="manager", user_id=601, username="mgr_s")
        self._seed_request(submitted_by=601, title="Envelopes")
        r = self.client.get("/team/admin/supply")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Envelopes", r.text)

    def test_approve_transitions_and_audits(self):
        from app.models import AuditLog, SupplyRequest
        self._login(role="manager", user_id=602, username="mgr_a")
        rid = self._seed_request(submitted_by=602, title="Boxes")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/supply/{rid}/approve",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        row = self.session.get(SupplyRequest, rid)
        self.assertEqual(row.status, "approved")
        self.assertEqual(row.approved_by_user_id, 602)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "supply.approved")
        ).all())
        self.assertEqual(len(rows), 1)

    def test_deny_and_mark_ordered(self):
        from app.models import AuditLog, SupplyRequest
        self._login(role="admin", user_id=603, username="adm_s")
        r1 = self._seed_request(submitted_by=603, title="Sleeves")
        csrf = self._csrf()
        self.client.post(
            f"/team/admin/supply/{r1}/approve",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.client.post(
            f"/team/admin/supply/{r1}/mark-ordered",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.session.expire_all()
        row = self.session.get(SupplyRequest, r1)
        self.assertEqual(row.status, "ordered")

        r2 = self._seed_request(submitted_by=603, title="Tape rolls")
        self.client.post(
            f"/team/admin/supply/{r2}/deny",
            data={"csrf_token": csrf, "notes": "not budgeted"},
            follow_redirects=False,
        )
        self.session.expire_all()
        row2 = self.session.get(SupplyRequest, r2)
        self.assertEqual(row2.status, "denied")
        self.assertIn("not budgeted", row2.notes)

    def test_manager_cannot_terminate(self):
        self._login(role="manager", user_id=604, username="mgr_t")
        self._seed_employee(user_id=904, username="emp904")
        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/employees/904/terminate",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)


if __name__ == "__main__":
    unittest.main()
