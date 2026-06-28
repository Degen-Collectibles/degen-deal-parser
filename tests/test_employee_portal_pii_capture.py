"""PII capture tests — invite flow, email uniqueness, and admin reveal.

These tests cover the fields that were added to the onboarding + reveal
surface (email, legal_name, phone, address, emergency_contact). They
deliberately re-use the wave3/wave4 harness patterns so CSRF + session
handling is consistent with the rest of the portal suite.
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
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-pii")
os.environ.setdefault("SESSION_SECRET", "test-secret-pii-" + "x" * 32)


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


class _PIIHarness:
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

        # Always stub get_request_user in both shared and main. Tests that
        # want a logged-in user call _login() which re-patches with a real
        # User. Tests that don't call _login() still need the middleware to
        # short-circuit to None instead of trying to hit managed_session()
        # against the real (conftest) sqlite — which doesn't contain the
        # user ids this test harness creates in its own in-memory engine.
        from app import shared
        import app.main as app_main

        self._default_user_patcher_shared = patch.object(
            shared, "get_request_user", return_value=None
        )
        self._default_user_patcher_shared.start()
        self._default_user_patcher_main = patch.object(
            app_main, "get_request_user", return_value=None
        )
        self._default_user_patcher_main.start()

    def _teardown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in (
            "_patcher_shared",
            "_patcher_main",
            "_default_user_patcher_shared",
            "_default_user_patcher_main",
        ):
            patcher = getattr(self, attr, None)
            if patcher:
                patcher.stop()
                setattr(self, attr, None)

    def _login(self, *, role: str, user_id: int = 200, username: str = "admin_pii"):
        from app import shared
        from app.models import User
        import app.main as app_main

        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        # Stop the default (None) stubs so this test can have a real user.
        for attr in ("_default_user_patcher_shared", "_default_user_patcher_main"):
            p = getattr(self, attr, None)
            if p:
                p.stop()
                setattr(self, attr, None)
        self._patcher_shared = patch.object(shared, "get_request_user", return_value=u)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=u)
        self._patcher_main.start()
        if self.session.get(User, user_id) is None:
            self.session.add(u)
            self.session.commit()
        return u

    def _csrf(self) -> str:
        marker = 'name="csrf_token" value="'
        for path in ("/team/login", "/team/password/forgot"):
            r = self.client.get(path, follow_redirects=False)
            if marker in r.text:
                idx = r.text.index(marker) + len(marker)
                end = r.text.index('"', idx)
                return r.text[idx:end]
        raise AssertionError("no csrf token rendered")


class InvitePIICaptureTests(unittest.TestCase, _PIIHarness):
    """The onboarding form collects a handful of PII fields. They must all
    round-trip through encryption and be retrievable via admin reveal."""

    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def _issue_invite(self):
        from app.auth import generate_invite_token, hash_password
        from app.models import User

        ph, salt = hash_password("AdminPass1!")
        admin = User(
            username="adm_pii",
            password_hash=ph,
            password_salt=salt,
            display_name="A",
            role="admin",
            is_active=True,
        )
        self.session.add(admin)
        self.session.commit()
        self.session.refresh(admin)
        return generate_invite_token(
            self.session, role="employee", created_by_user_id=admin.id
        )

    def test_invite_accept_encrypts_every_field(self):
        """Every field submitted through the onboarding form must be stored
        encrypted (ciphertext ≠ plaintext), and email must populate the
        lookup hash."""
        from app.models import EmployeeProfile, User
        from app.team.pii import decrypt_pii, email_lookup_hash

        raw = self._issue_invite()
        csrf = self._csrf()
        r = self.client.post(
            f"/team/invite/accept/{raw}",
            data={
                "new_username": "jane_doe",
                "new_password": "StrongPass9#xy",
                "email": "Jane.Doe@Example.COM",
                "legal_name": "Jane Elizabeth Doe",
                "preferred_name": "Jane",
                "phone": "+1 408-555-0199",
                "address_street": "123 Route 1",
                "address_city": "San Jose",
                "address_state": "CA",
                "address_zip": "95112",
                "emergency_contact_name": "John Doe",
                "emergency_contact_phone": "+1 408-555-0100",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303, r.text)

        u = self.session.exec(select(User).where(User.username == "jane_doe")).first()
        self.assertIsNotNone(u)
        self.assertEqual(u.display_name, "Jane")  # preferred_name wins
        prof = self.session.get(EmployeeProfile, u.id)
        self.assertIsNotNone(prof)

        # All ciphertext fields are opaque (not containing plaintext).
        self.assertIsNotNone(prof.email_ciphertext)
        self.assertNotIn(b"Jane", prof.email_ciphertext or b"")
        self.assertNotIn(b"Example", prof.email_ciphertext or b"")
        self.assertIsNotNone(prof.legal_name_enc)
        self.assertNotIn(b"Jane Elizabeth", prof.legal_name_enc or b"")
        self.assertIsNotNone(prof.phone_enc)
        self.assertIsNotNone(prof.address_enc)
        self.assertNotIn(b"95112", prof.address_enc or b"")
        self.assertIsNotNone(prof.emergency_contact_name_enc)
        self.assertIsNotNone(prof.emergency_contact_phone_enc)

        # Email lookup hash is normalized (lowercased) and matches what
        # email_lookup_hash() would return.
        self.assertIsNotNone(prof.email_lookup_hash)
        self.assertEqual(prof.email_lookup_hash, email_lookup_hash("jane.doe@example.com"))

        # Round-trip decryption works.
        self.assertEqual(decrypt_pii(prof.email_ciphertext), "jane.doe@example.com")
        self.assertEqual(decrypt_pii(prof.legal_name_enc), "Jane Elizabeth Doe")
        self.assertEqual(decrypt_pii(prof.phone_enc), "+1 408-555-0199")
        self.assertEqual(decrypt_pii(prof.emergency_contact_name_enc), "John Doe")
        addr = json.loads(decrypt_pii(prof.address_enc))
        self.assertEqual(addr["zip"], "95112")
        self.assertEqual(addr["city"], "San Jose")

        # Onboarding complete timestamp is set.
        self.assertIsNotNone(prof.onboarding_completed_at)

    def test_invite_accept_drops_duplicate_email_after_logout(self):
        """If two invitees claim the same email, the second account still
        onboards but the conflicting email is dropped."""
        from app.auth import generate_invite_token
        from app.models import User

        # First invite — succeeds.
        raw1 = self._issue_invite()
        csrf = self._csrf()
        r1 = self.client.post(
            f"/team/invite/accept/{raw1}",
            data={
                "new_username": "first_user",
                "new_password": "StrongPass9#xy",
                "email": "dup@example.com",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r1.status_code, 303, r1.text)
        self.client.cookies.clear()

        # Issue a second invite using the newly-seeded admin (already in DB).
        admin = self.session.exec(select(User).where(User.username == "adm_pii")).first()
        raw2 = generate_invite_token(
            self.session, role="employee", created_by_user_id=admin.id
        )
        csrf = self._csrf()
        r2 = self.client.post(
            f"/team/invite/accept/{raw2}",
            data={
                "new_username": "second_user",
                "new_password": "StrongPass9#xy",
                "email": "DUP@Example.com",  # same email, different case
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        # Should onboard successfully but drop the conflicting email.
        self.assertEqual(r2.status_code, 303, r2.text)
        second = self.session.exec(
            select(User).where(User.username == "second_user")
        ).first()
        self.assertIsNotNone(second)
        self.assertIn("banner=Email+not+saved", r2.headers.get("location", ""))

    def test_invite_accept_drops_duplicate_email_but_still_onboards(self):
        from app.models import EmployeeProfile, User
        from app.team.pii import decrypt_pii, email_lookup_hash, encrypt_pii

        existing = User(
            id=9100,
            username="existing_email_owner",
            password_hash="x",
            password_salt="x",
            display_name="Existing",
            role="employee",
            is_active=True,
        )
        existing_profile = EmployeeProfile(
            user_id=9100,
            email_ciphertext=encrypt_pii("dup@example.com"),
            email_lookup_hash=email_lookup_hash("dup@example.com"),
        )
        self.session.add(existing)
        self.session.add(existing_profile)
        self.session.commit()

        raw = self._issue_invite()
        csrf = self._csrf()
        r = self.client.post(
            f"/team/invite/accept/{raw}",
            data={
                "new_username": "email_drop_user",
                "new_password": "StrongPass9#xy",
                "preferred_name": "Drop",
                "email": "DUP@example.com",
                "phone": "5551112222",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303, r.text)
        self.assertIn("banner=Email+not+saved", r.headers.get("location", ""))

        onboarded = self.session.exec(
            select(User).where(User.username == "email_drop_user")
        ).first()
        self.assertIsNotNone(onboarded)
        profile = self.session.get(EmployeeProfile, onboarded.id)
        self.assertEqual(decrypt_pii(profile.phone_enc), "5551112222")
        self.assertFalse(profile.email_ciphertext)
        self.assertFalse(profile.email_lookup_hash)


class AdminEmailRevealTests(unittest.TestCase, _PIIHarness):
    """Admin reveal of the `email` field must decrypt + audit."""

    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def _seed_employee_with_email(self, *, user_id: int = 777, email: str = "reveal@example.com"):
        from app.models import EmployeeProfile, User
        from app.team.pii import encrypt_pii, email_lookup_hash

        u = User(
            id=user_id,
            username="target",
            password_hash="x",
            password_salt="x",
            display_name="Target",
            role="employee",
            is_active=True,
        )
        prof = EmployeeProfile(
            user_id=user_id,
            email_ciphertext=encrypt_pii(email),
            email_lookup_hash=email_lookup_hash(email),
        )
        self.session.add(u)
        self.session.add(prof)
        self.session.commit()
        return u

    def test_admin_can_reveal_email_and_audit_is_written(self):
        from app.models import AuditLog

        self._login(role="admin", user_id=601, username="adm_reveal")
        target = self._seed_employee_with_email()

        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{target.id}/reveal",
            data={"field": "email", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertIn("reveal@example.com", r.text)

        # Audit row written with action=pii.reveal, field=email.
        audits = self.session.exec(
            select(AuditLog).where(AuditLog.action == "pii.reveal")
        ).all()
        self.assertTrue(audits, "pii.reveal audit row missing")
        details = json.loads(audits[-1].details_json or "{}")
        self.assertEqual(details.get("field"), "email")
        self.assertEqual(audits[-1].target_user_id, target.id)

    def test_reveal_rejects_unknown_field(self):
        self._login(role="admin", user_id=602, username="adm_reveal2")
        target = self._seed_employee_with_email(user_id=778)
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{target.id}/reveal",
            data={"field": "ssn", "csrf_token": csrf},
            follow_redirects=False,
        )
        # Unknown field is explicitly rejected with 400, not quietly ignored.
        self.assertEqual(r.status_code, 400)


class ProfileSelfEditEmailTests(unittest.TestCase, _PIIHarness):
    """Employees can update their own email. The update must re-encrypt
    and refresh the lookup hash; attempting to change to an already-taken
    email must fail gracefully."""

    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    _CURRENT_PASSWORD = "OldPassword1!"

    def _seed_employee(
        self,
        *,
        user_id: int,
        username: str,
        email: str,
        phone: str = "",
    ):
        from app.auth import hash_password
        from app.models import EmployeeProfile, User
        from app.team.pii import encrypt_pii, email_lookup_hash

        password_hash, password_salt = hash_password(self._CURRENT_PASSWORD)
        u = User(
            id=user_id,
            username=username,
            password_hash=password_hash,
            password_salt=password_salt,
            display_name=username,
            role="employee",
            is_active=True,
        )
        prof = EmployeeProfile(
            user_id=user_id,
            email_ciphertext=encrypt_pii(email),
            email_lookup_hash=email_lookup_hash(email),
        )
        if phone:
            prof.phone_enc = encrypt_pii(phone)
        self.session.add(u)
        self.session.add(prof)
        self.session.commit()
        return u

    def _post_profile(
        self,
        *,
        email: str,
        preferred_name: str = "self_edit",
        phone: str = "",
        current_password: str | None = None,
    ):
        csrf = self._csrf()
        data = {
            "preferred_name": preferred_name,
            "email": email,
            "phone": phone,
            "csrf_token": csrf,
        }
        if current_password is not None:
            data["current_password"] = current_password
        return self.client.post(
            "/team/profile",
            data=data,
            follow_redirects=False,
        )

    def _assert_sensitive_profile_unchanged(
        self,
        *,
        user_id: int,
        email_ciphertext: bytes,
        email_lookup_hash: str,
        display_name: str,
        phone: str = "",
    ) -> None:
        from app.models import EmployeeProfile, User
        from app.team.pii import decrypt_pii

        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, user_id)
        db_user = self.session.get(User, user_id)
        self.assertIsNotNone(profile)
        self.assertIsNotNone(db_user)
        self.assertEqual(profile.email_ciphertext, email_ciphertext)
        self.assertEqual(profile.email_lookup_hash, email_lookup_hash)
        self.assertEqual(db_user.display_name, display_name)
        if phone:
            self.assertEqual(decrypt_pii(profile.phone_enc), phone)

    def test_employee_can_update_own_email(self):
        from app.models import AuditLog, EmployeeProfile
        from app.team.pii import decrypt_pii, email_lookup_hash

        emp = self._seed_employee(
            user_id=900, username="self_edit", email="old@example.com"
        )
        self._login(role="employee", user_id=900, username="self_edit")
        r = self._post_profile(
            preferred_name="Edited",
            email="New@Example.COM",
            current_password=self._CURRENT_PASSWORD,
        )
        self.assertIn(r.status_code, (200, 303), r.text)

        self.session.expire_all()
        prof = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(decrypt_pii(prof.email_ciphertext), "new@example.com")
        self.assertEqual(prof.email_lookup_hash, email_lookup_hash("new@example.com"))
        audit = self.session.exec(
            select(AuditLog).where(
                AuditLog.target_user_id == emp.id,
                AuditLog.action == "profile.self_update",
            )
        ).one()
        self.assertTrue(
            {"preferred_name", "email"}.issubset(
                set(json.loads(audit.details_json)["fields"])
            )
        )

    def test_email_change_requires_current_password_before_any_profile_mutation(self):
        from app.models import EmployeeProfile

        emp = self._seed_employee(
            user_id=901,
            username="missing_password",
            email="old@example.com",
            phone="555-0101",
        )
        profile = self.session.get(EmployeeProfile, emp.id)
        old_ciphertext = profile.email_ciphertext
        old_hash = profile.email_lookup_hash
        self._login(role="employee", user_id=emp.id, username=emp.username)

        response = self._post_profile(
            email="attacker@example.com",
            preferred_name="Tampered Name",
            phone="555-9999",
        )

        self.assertIn(response.status_code, (303, 400), response.text)
        self.assertIn("password", response.headers.get("location", "").lower())
        self._assert_sensitive_profile_unchanged(
            user_id=emp.id,
            email_ciphertext=old_ciphertext,
            email_lookup_hash=old_hash,
            display_name=emp.username,
            phone="555-0101",
        )

    def test_email_change_rejects_wrong_current_password_before_any_profile_mutation(self):
        from app.models import EmployeeProfile

        emp = self._seed_employee(
            user_id=902,
            username="wrong_password",
            email="old@example.com",
            phone="555-0102",
        )
        profile = self.session.get(EmployeeProfile, emp.id)
        old_ciphertext = profile.email_ciphertext
        old_hash = profile.email_lookup_hash
        self._login(role="employee", user_id=emp.id, username=emp.username)

        response = self._post_profile(
            email="attacker@example.com",
            preferred_name="Tampered Name",
            phone="555-9999",
            current_password="WrongPassword1!",
        )

        self.assertIn(response.status_code, (303, 400), response.text)
        self.assertIn("password", response.headers.get("location", "").lower())
        self._assert_sensitive_profile_unchanged(
            user_id=emp.id,
            email_ciphertext=old_ciphertext,
            email_lookup_hash=old_hash,
            display_name=emp.username,
            phone="555-0102",
        )

    def test_clearing_email_requires_current_password(self):
        from app.models import EmployeeProfile

        emp = self._seed_employee(
            user_id=903, username="clear_requires_password", email="old@example.com"
        )
        profile = self.session.get(EmployeeProfile, emp.id)
        old_ciphertext = profile.email_ciphertext
        old_hash = profile.email_lookup_hash
        self._login(role="employee", user_id=emp.id, username=emp.username)

        response = self._post_profile(email="", preferred_name=emp.username)

        self.assertIn(response.status_code, (303, 400), response.text)
        self._assert_sensitive_profile_unchanged(
            user_id=emp.id,
            email_ciphertext=old_ciphertext,
            email_lookup_hash=old_hash,
            display_name=emp.username,
        )

    def test_employee_can_clear_email_with_current_password(self):
        from app.models import EmployeeProfile

        emp = self._seed_employee(
            user_id=904, username="clear_with_password", email="old@example.com"
        )
        self._login(role="employee", user_id=emp.id, username=emp.username)

        response = self._post_profile(
            email="",
            preferred_name=emp.username,
            current_password=self._CURRENT_PASSWORD,
        )

        self.assertIn(response.status_code, (200, 303), response.text)
        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, emp.id)
        self.assertIsNone(profile.email_ciphertext)
        self.assertIsNone(profile.email_lookup_hash)

    def test_email_change_rejects_non_single_or_malformed_mailboxes_before_mutation(self):
        from app.models import EmployeeProfile

        emp = self._seed_employee(
            user_id=905,
            username="invalid_email",
            email="old@example.com",
            phone="555-0105",
        )
        profile = self.session.get(EmployeeProfile, emp.id)
        old_ciphertext = profile.email_ciphertext
        old_hash = profile.email_lookup_hash
        self._login(role="employee", user_id=emp.id, username=emp.username)

        invalid_addresses = (
            "victim@example.com,attacker@example.com",
            "victim@example.com\r\nBcc: attacker@example.com",
            "not-an-email",
            "Attacker <attacker@example.com>",
        )
        for invalid_email in invalid_addresses:
            with self.subTest(invalid_email=repr(invalid_email)):
                response = self._post_profile(
                    email=invalid_email,
                    preferred_name="Tampered Name",
                    phone="555-9999",
                    current_password=self._CURRENT_PASSWORD,
                )
                self.assertIn(response.status_code, (303, 400), response.text)
                self.assertIn("email", response.headers.get("location", "").lower())
                self._assert_sensitive_profile_unchanged(
                    user_id=emp.id,
                    email_ciphertext=old_ciphertext,
                    email_lookup_hash=old_hash,
                    display_name=emp.username,
                    phone="555-0105",
                )

    def test_unchanged_email_allows_ordinary_profile_edit_without_password(self):
        from app.models import EmployeeProfile, User

        emp = self._seed_employee(
            user_id=906, username="ordinary_edit", email="same@example.com"
        )
        profile = self.session.get(EmployeeProfile, emp.id)
        old_ciphertext = profile.email_ciphertext
        self._login(role="employee", user_id=emp.id, username=emp.username)

        response = self._post_profile(
            email="same@example.com",
            preferred_name="Ordinary Edit",
        )

        self.assertIn(response.status_code, (200, 303), response.text)
        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, emp.id)
        db_user = self.session.get(User, emp.id)
        self.assertEqual(profile.email_ciphertext, old_ciphertext)
        self.assertEqual(db_user.display_name, "Ordinary Edit")

    def test_profile_form_explains_current_password_for_email_changes(self):
        emp = self._seed_employee(
            user_id=907, username="profile_form", email="same@example.com"
        )
        self._login(role="employee", user_id=emp.id, username=emp.username)

        response = self.client.get("/team/profile")

        self.assertEqual(response.status_code, 200)
        self.assertIn('name="current_password"', response.text)
        self.assertIn("email", response.text.lower())


if __name__ == "__main__":
    unittest.main()
