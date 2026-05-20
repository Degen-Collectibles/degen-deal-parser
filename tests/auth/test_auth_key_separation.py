from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "wave-g-auth-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "wave-g-auth-hmac-key")
os.environ.setdefault("SESSION_SECRET", "wave-g-auth-session-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "wave-g-auth-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class AuthKeySeparationTests(unittest.TestCase):
    def setUp(self):
        self.engine = _fresh_engine()
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def test_boot_migration_fills_empty_salts_idempotently(self):
        from app import auth
        from app.models import User

        old_secret = auth.settings.session_secret
        h1, _ = auth.hash_password("LegacyPassword123!", salt=old_secret)
        h2, _ = auth.hash_password("AnotherPassword123!", salt=old_secret)
        self.session.add_all(
            [
                User(
                    id=1,
                    username="legacy-one",
                    password_hash=h1,
                    password_salt="",
                    role="employee",
                    is_active=True,
                ),
                User(
                    id=2,
                    username="legacy-two",
                    password_hash=h2,
                    password_salt="",
                    role="employee",
                    is_active=True,
                ),
            ]
        )
        self.session.commit()

        self.assertEqual(auth.migrate_empty_password_salts(self.session), 2)
        one = self.session.get(User, 1)
        two = self.session.get(User, 2)
        self.assertTrue(one.password_salt)
        self.assertTrue(two.password_salt)
        self.assertTrue(auth.verify_password("LegacyPassword123!", one.password_hash, salt=one.password_salt))
        self.assertTrue(auth.verify_password("AnotherPassword123!", two.password_hash, salt=two.password_salt))
        salts = (one.password_salt, two.password_salt)

        self.assertEqual(auth.migrate_empty_password_salts(self.session), 0)
        self.session.expire_all()
        self.assertEqual((self.session.get(User, 1).password_salt, self.session.get(User, 2).password_salt), salts)

    def test_verify_password_fails_on_empty_salt_post_migration(self):
        from app.auth import AuthError, hash_password, verify_password

        password_hash, _salt = hash_password("Password12345!", salt="old-secret")
        with self.assertRaises(AuthError):
            verify_password("Password12345!", password_hash, salt="")

    def test_token_hmac_requires_explicit_key_outside_tests(self):
        from app import auth

        with patch.object(auth.settings, "employee_token_hmac_key", ""):
            with self.assertRaises(ValueError):
                auth._token_hmac_key()

    def test_token_hmac_works_with_explicit_key(self):
        from app import auth

        with patch.object(auth.settings, "employee_token_hmac_key", "explicit-token-key"), \
             patch.object(auth.settings, "session_secret", "different-session-key"):
            first = auth._token_lookup_hmac("abc123")
            second = auth._token_lookup_hmac("abc123")
        self.assertEqual(first, second)

    def test_rotating_session_secret_does_not_invalidate_invites(self):
        from app import auth
        from app.models import InviteToken, User

        self.session.add(
            User(
                id=1,
                username="invite-admin",
                password_hash="x",
                password_salt="x",
                role="admin",
                is_active=True,
            )
        )
        self.session.commit()
        with patch.object(auth.settings, "employee_token_hmac_key", "stable-token-key"), \
             patch.object(auth.settings, "session_secret", "old-session-key"):
            raw = auth.generate_invite_token(
                self.session,
                role="employee",
                created_by_user_id=1,
            )
        with patch.object(auth.settings, "employee_token_hmac_key", "stable-token-key"), \
             patch.object(auth.settings, "session_secret", "new-session-key"):
            row = auth._find_token_row(self.session, InviteToken, raw)
        self.assertIsNotNone(row)
