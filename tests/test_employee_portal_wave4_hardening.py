"""Wave 4.5 — hardening tests for Wave 4 admin employee portal surface.

Covers:
- CSRF enforcement on all destructive admin routes.
- Fernet decrypt failure on reveal → audited + sanitized error (no 500).
- Manager denied on profile-update (admin.employees.edit gate).
- admin.supply.* seeding is reviewer=True (MAJ-1 migration).
- Password reset split: admin-issued writes password.reset_issued.
- Reset consume round-trip: new password authenticates.
- Invite consume round-trip: new user can log in.
- Purge idempotency: second purge is a no-op with no duplicate audit row.
"""
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
os.environ.setdefault("SESSION_SECRET", "test-secret-wave45-" + "x" * 32)


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


class _Harness:
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

    def _seed_employee(self, *, user_id: int = 500, username: str = "tgt"):
        from app.models import EmployeeProfile, User
        from app.pii import encrypt_pii
        from datetime import date

        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role="employee",
            is_active=True,
        )
        self.session.add(u)
        p = EmployeeProfile(
            user_id=user_id,
            phone_enc=encrypt_pii("555-867-5309"),
            legal_name_enc=encrypt_pii("Jane Q Test"),
            hire_date=date(2024, 1, 15),
        )
        self.session.add(p)
        self.session.commit()
        return u


# ---------------------------------------------------------------------------
# CSRF-missing rejections on destructive admin routes
# ---------------------------------------------------------------------------

class CSRFEnforcementTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_terminate_without_csrf_rejected(self):
        self._login(role="admin", user_id=11, username="a1")
        emp = self._seed_employee(user_id=1101, username="emp1101")
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/terminate",
            data={},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_purge_without_csrf_rejected(self):
        self._login(role="admin", user_id=12, username="a2")
        emp = self._seed_employee(user_id=1102, username="emp1102")
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/purge",
            data={"confirm_username": "emp1102"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_reset_password_without_csrf_rejected(self):
        self._login(role="admin", user_id=13, username="a3")
        emp = self._seed_employee(user_id=1103, username="emp1103")
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reset-password",
            data={},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_invite_issue_without_csrf_rejected(self):
        self._login(role="admin", user_id=14, username="a4")
        r = self.client.post(
            "/team/admin/invites/issue",
            data={"role": "employee"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_invite_revoke_without_csrf_rejected(self):
        from app.auth import generate_invite_token
        self._login(role="admin", user_id=15, username="a5")
        generate_invite_token(self.session, role="employee", created_by_user_id=15)
        from app.models import InviteToken
        row = self.session.exec(select(InviteToken)).first()
        r = self.client.post(
            f"/team/admin/invites/{row.id}/revoke",
            data={},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_supply_approve_without_csrf_rejected(self):
        from app.models import SupplyRequest
        self._login(role="admin", user_id=16, username="a6")
        req = SupplyRequest(
            submitted_by_user_id=16,
            title="Pens",
            description="",
            urgency="normal",
            status="submitted",
        )
        self.session.add(req)
        self.session.commit()
        self.session.refresh(req)
        r = self.client.post(
            f"/team/admin/supply/{req.id}/approve",
            data={},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)


# ---------------------------------------------------------------------------
# admin.employees.edit — manager denied on profile-update
# ---------------------------------------------------------------------------

class ProfileUpdateGateTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_manager_denied_profile_update(self):
        self._login(role="manager", user_id=20, username="mgr_x")
        emp = self._seed_employee(user_id=1201, username="emp1201")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/profile-update",
            data={"csrf_token": csrf, "display_name": "Should Not Save"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_admin_allowed_profile_update(self):
        from app.models import User
        self._login(role="admin", user_id=21, username="adm_x")
        emp = self._seed_employee(user_id=1202, username="emp1202")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/profile-update",
            data={"csrf_token": csrf, "display_name": "Saved Name"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        self.assertEqual(self.session.get(User, emp.id).display_name, "Saved Name")


# ---------------------------------------------------------------------------
# Fernet decrypt failure on reveal → pii.reveal_failed audit + no 500
# ---------------------------------------------------------------------------

class RevealDecryptFailureTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_corrupted_blob_audits_failed_and_no_500(self):
        from app.models import AuditLog, EmployeeProfile, User
        self._login(role="admin", user_id=30, username="adm_dec")
        # Seed employee whose phone_enc is syntactically valid-looking bytes
        # but was encrypted with a DIFFERENT key → InvalidToken on decrypt.
        other = Fernet(Fernet.generate_key())
        bad_blob = other.encrypt(b"this will not decrypt")
        u = User(
            id=1301, username="emp1301", password_hash="x", password_salt="x",
            display_name="e", role="employee", is_active=True,
        )
        self.session.add(u)
        self.session.add(EmployeeProfile(user_id=1301, phone_enc=bad_blob))
        self.session.commit()
        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/employees/1301/reveal",
            data={"field": "phone", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn("Reveal failed", r.text)
        reveal_rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "pii.reveal")
        ).all())
        failed_rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "pii.reveal_failed")
        ).all())
        self.assertEqual(len(reveal_rows), 1)  # Phase 1 committed
        self.assertEqual(len(failed_rows), 1)  # Phase 2 failure audited
        self.assertIn("invalid_token", failed_rows[0].details_json)


# ---------------------------------------------------------------------------
# MAJ-1: supply perms seed for reviewer
# ---------------------------------------------------------------------------

class SupplyPermsSeedTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_reviewer_has_admin_supply_view_and_approve(self):
        from app.auth import has_permission
        from app.models import User
        reviewer = User(
            id=9001, username="rev1", password_hash="x", password_salt="x",
            display_name="rev", role="reviewer", is_active=True,
        )
        self.assertTrue(has_permission(self.session, reviewer, "admin.supply.view"))
        self.assertTrue(has_permission(self.session, reviewer, "admin.supply.approve"))

    def test_reviewer_upgrade_migration_fires_on_stale_rows(self):
        """Older deployment: reviewer=False for supply keys — migration must flip."""
        from app.db import seed_employee_portal_defaults
        from app.models import RolePermission
        # Force stale state.
        for key in ("admin.supply.view", "admin.supply.approve"):
            row = self.session.exec(
                select(RolePermission).where(
                    RolePermission.role == "reviewer",
                    RolePermission.resource_key == key,
                )
            ).first()
            row.is_allowed = False
            self.session.add(row)
        self.session.commit()
        seed_employee_portal_defaults(self.session)
        self.session.expire_all()
        for key in ("admin.supply.view", "admin.supply.approve"):
            row = self.session.exec(
                select(RolePermission).where(
                    RolePermission.role == "reviewer",
                    RolePermission.resource_key == key,
                )
            ).first()
            self.assertTrue(row.is_allowed, f"reviewer {key} should be True")


# ---------------------------------------------------------------------------
# MIN-2: admin-issued reset writes password.reset_issued
# ---------------------------------------------------------------------------

class PasswordResetActionSplitTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_admin_issued_reset_uses_reset_issued_action(self):
        from app.auth import generate_password_reset_token
        from app.models import AuditLog, User
        target = User(
            id=7001, username="target1", password_hash="x", password_salt="x",
            display_name="t", role="employee", is_active=True,
        )
        admin_u = User(
            id=7002, username="admin1", password_hash="x", password_salt="x",
            display_name="a", role="admin", is_active=True,
        )
        self.session.add(target)
        self.session.add(admin_u)
        self.session.commit()
        generate_password_reset_token(
            self.session, user_id=7001, issued_by_user_id=7002
        )
        issued = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_issued")
        ).all())
        requested = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_requested")
        ).all())
        self.assertEqual(len(issued), 1)
        self.assertEqual(len(requested), 0)

    def test_self_serve_reset_uses_reset_requested_action(self):
        from app.auth import generate_password_reset_token
        from app.models import AuditLog, User
        u = User(
            id=7003, username="self1", password_hash="x", password_salt="x",
            display_name="s", role="employee", is_active=True,
        )
        self.session.add(u)
        self.session.commit()
        generate_password_reset_token(
            self.session, user_id=7003, issued_by_user_id=None
        )
        issued = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_issued")
        ).all())
        requested = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_requested")
        ).all())
        self.assertEqual(len(issued), 0)
        self.assertEqual(len(requested), 1)


# ---------------------------------------------------------------------------
# End-to-end: admin reset → user consumes → new password authenticates
# ---------------------------------------------------------------------------

class ResetConsumeRoundTripTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_admin_reset_then_consume_then_login(self):
        from app.auth import (
            authenticate_user,
            consume_password_reset_token,
            generate_password_reset_token,
            hash_password,
        )
        from app.models import User
        old_hash, old_salt = hash_password("OldPassword1!")
        target = User(
            id=8001, username="rtuser", password_hash=old_hash,
            password_salt=old_salt, display_name="r", role="employee",
            is_active=True,
        )
        admin_u = User(
            id=8002, username="rtadmin", password_hash="x", password_salt="x",
            display_name="a", role="admin", is_active=True,
        )
        self.session.add(target)
        self.session.add(admin_u)
        self.session.commit()
        raw = generate_password_reset_token(
            self.session, user_id=8001, issued_by_user_id=8002
        )
        new_password = "BrandNewSecret9!"
        consume_password_reset_token(self.session, raw, new_password=new_password)
        self.session.expire_all()
        # Old password no longer works.
        self.assertIsNone(
            authenticate_user(self.session, username="rtuser", password="OldPassword1!")
        )
        # New password authenticates.
        logged_in = authenticate_user(
            self.session, username="rtuser", password=new_password
        )
        self.assertIsNotNone(logged_in)
        self.assertEqual(logged_in.id, 8001)

    def test_reset_consume_bumps_session_version(self):
        from app.auth import (
            consume_password_reset_token,
            generate_password_reset_token,
            hash_password,
        )
        from app.models import User

        old_hash, old_salt = hash_password("OldPassword1!")
        target = User(
            id=8003,
            username="resetver",
            password_hash=old_hash,
            password_salt=old_salt,
            display_name="resetver",
            role="employee",
            is_active=True,
            session_version=2,
        )
        self.session.add(target)
        self.session.commit()

        raw = generate_password_reset_token(
            self.session, user_id=8003, issued_by_user_id=None
        )
        consume_password_reset_token(self.session, raw, new_password="BrandNewSecret9!")
        self.session.expire_all()
        refreshed = self.session.get(User, 8003)
        self.assertEqual(refreshed.session_version, 3)


# ---------------------------------------------------------------------------
# End-to-end: admin invite → new user accepts → authenticates
# ---------------------------------------------------------------------------

class InviteConsumeRoundTripTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_invite_issued_then_consumed_then_login(self):
        from app.auth import (
            authenticate_user,
            consume_invite_token,
            generate_invite_token,
        )
        from app.models import User
        admin_u = User(
            id=8501, username="invadmin", password_hash="x", password_salt="x",
            display_name="a", role="admin", is_active=True,
        )
        self.session.add(admin_u)
        self.session.commit()
        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=8501,
        )
        new_user = consume_invite_token(
            self.session, raw,
            new_username="newhire",
            new_password="StrongPassw0rd!",
        )
        self.assertEqual(new_user.role, "employee")
        self.session.expire_all()
        logged_in = authenticate_user(
            self.session, username="newhire", password="StrongPassw0rd!"
        )
        self.assertIsNotNone(logged_in)


# ---------------------------------------------------------------------------
# Purge idempotency — second purge on already-wiped row is a no-op
# ---------------------------------------------------------------------------

class PurgeIdempotencyTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_second_purge_no_duplicate_audit_and_pii_still_none(self):
        from app.models import AuditLog, User
        self._login(role="admin", user_id=40, username="adm_purge")
        from app.models import User
        admin_row = self.session.get(User, 40)
        admin_row.password_hash = "x"
        admin_row.password_salt = "x"
        admin_row.session_version = 1
        self.session.add(admin_row)
        self.session.commit()
        emp = self._seed_employee(user_id=1401, username="emp1401")
        csrf = self._csrf()
        r1 = self.client.post(
            f"/team/admin/employees/{emp.id}/purge",
            data={"csrf_token": csrf, "confirm_username": "emp1401"},
            follow_redirects=False,
        )
        self.assertEqual(r1.status_code, 303)
        follow1 = self.client.get(r1.headers["location"])
        self.assertEqual(follow1.status_code, 200)
        # Second purge: should still "succeed" but not add new PII (already None).
        r2 = self.client.post(
            f"/team/admin/employees/{emp.id}/purge",
            data={"csrf_token": csrf, "confirm_username": "emp1401"},
            follow_redirects=False,
        )
        # Either 303 (noop) or a redirect — but MUST NOT 500.
        self.assertIn(r2.status_code, (303, 400, 409))
        self.assertEqual(r2.status_code, 303)
        self.assertTrue(r2.headers.get("location", "").endswith("flash=PII+purged."))
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.purged")
        ).all())
        self.assertEqual(len(rows), 1)

    def test_terminate_revokes_session_version_and_deactivates_user(self):
        from app.models import AuditLog, EmployeeProfile, User

        self._login(role="admin", user_id=40, username="adm_term")
        admin_row = self.session.get(User, 40)
        admin_row.password_hash = "x"
        admin_row.password_salt = "x"
        admin_row.session_version = 1
        self.session.add(admin_row)
        employee = self._seed_employee(user_id=1402, username="emp1402")
        user_row = self.session.get(User, employee.id)
        user_row.session_version = 1
        self.session.add(user_row)
        self.session.commit()

        csrf = self._csrf()
        r_term = self.client.post(
            f"/team/admin/employees/{employee.id}/terminate",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r_term.status_code, 303)
        follow = self.client.get(r_term.headers["location"])
        self.assertEqual(follow.status_code, 200)

        self.session.expire_all()
        refreshed = self.session.get(User, employee.id)
        self.assertFalse(refreshed.is_active)
        self.assertEqual(refreshed.session_version, 2)
        profile = self.session.get(EmployeeProfile, employee.id)
        self.assertIsNotNone(profile.termination_date)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.terminated")
        ).all())
        self.assertEqual(len(rows), 1)
        self.assertIn('"session_version": 2', rows[0].details_json)


class PublicFlowErrorSanitizationTests(unittest.TestCase, _Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_invite_accept_hides_internal_error_codes(self):
        from app.auth import generate_invite_token

        from app.auth import create_draft_employee

        self._login(role="admin", user_id=41, username="adm_invite_sanitize")
        draft = create_draft_employee(
            self.session,
            role="employee",
            created_by_user_id=41,
            legal_name="Draft Employee",
        )
        token = generate_invite_token(
            self.session,
            role="employee",
            created_by_user_id=41,
            target_user_id=draft.id,
        )
        user_row = self.session.get(type(draft), draft.id)
        user_row.is_active = True
        user_row.password_hash = "x"
        user_row.password_salt = "x"
        self.session.add(user_row)
        self.session.commit()

        r = self.client.post(
            f"/team/invite/accept/{token}",
            data={
                "csrf_token": self._csrf(),
                "new_username": "newhire1450",
                "new_password": "StrongPassw0rd!",
                "preferred_name": "New Hire",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        location = r.headers.get("location", "")
        self.assertIn("error=Invite", location)
        self.assertIn("invalid", location)
        self.assertIn("expired", location)
        self.assertNotIn("invite_target_already_registered", location)

    def test_reset_consume_hides_internal_inactive_error_code(self):
        from app.auth import generate_password_reset_token
        from app.models import User

        self._login(role="admin", user_id=42, username="adm_reset_sanitize")
        employee = self._seed_employee(user_id=1451, username="emp1451")
        user_row = self.session.get(User, employee.id)
        user_row.is_active = False
        self.session.add(user_row)
        self.session.commit()
        token = generate_password_reset_token(self.session, user_id=employee.id)

        r = self.client.post(
            f"/team/password/reset/{token}",
            data={
                "csrf_token": self._csrf(),
                "new_password": "StrongPassw0rd!",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        location = r.headers.get("location", "")
        self.assertIn("error=Reset", location)
        self.assertIn("invalid", location)
        self.assertIn("expired", location)
        self.assertNotIn("reset_user_inactive", location)


if __name__ == "__main__":
    unittest.main()
