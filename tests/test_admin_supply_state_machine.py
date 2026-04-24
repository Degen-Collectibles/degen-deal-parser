from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

from cryptography.fernet import Fernet
from fastapi import HTTPException
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "wave-g-supply-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "wave-g-supply-hmac-key")
os.environ.setdefault("SESSION_SECRET", "wave-g-supply-session-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "wave-g-supply-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class SupplyStateMachineTests(unittest.TestCase):
    def setUp(self):
        from app.db import seed_employee_portal_defaults
        from app.models import User

        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.actor = User(
            id=1,
            username="supply-admin",
            password_hash="x",
            password_salt="x",
            display_name="Supply Admin",
            role="admin",
            is_active=True,
        )
        self.other_actor = User(
            id=2,
            username="supply-other",
            password_hash="x",
            password_salt="x",
            display_name="Supply Other",
            role="admin",
            is_active=True,
        )
        self.session.add_all([self.actor, self.other_actor])
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def _request(self):
        return SimpleNamespace(client=SimpleNamespace(host="testclient"))

    def _seed_request(self, *, status="submitted"):
        from app.models import SupplyRequest

        row = SupplyRequest(
            submitted_by_user_id=self.actor.id,
            title=f"Supplies {status}",
            description="need it",
            urgency="normal",
            status=status,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _transition(self, session, request_id, actor, target, action):
        from app.routers.team_admin_supply import _transition

        return _transition(
            session,
            request_id=request_id,
            actor=actor,
            new_status=target,
            action=action,
            request=self._request(),
        )

    def test_denied_request_cannot_be_approved(self):
        row = self._seed_request(status="denied")
        with self.assertRaises(HTTPException) as cm:
            self._transition(self.session, row.id, self.actor, "approved", "supply.approved")
        self.assertEqual(cm.exception.status_code, 409)

    def test_ordered_request_cannot_revert_to_submitted(self):
        row = self._seed_request(status="ordered")
        with self.assertRaises(HTTPException) as cm:
            self._transition(self.session, row.id, self.actor, "submitted", "supply.submitted")
        self.assertEqual(cm.exception.status_code, 409)

    def test_concurrent_approvals_only_one_succeeds(self):
        from app.models import SupplyRequest, User

        row = self._seed_request(status="submitted")
        s1 = Session(self.engine)
        s2 = Session(self.engine)
        try:
            s1.get(SupplyRequest, row.id)
            s2.get(SupplyRequest, row.id)
            actor1 = s1.get(User, self.actor.id)
            actor2 = s2.get(User, self.other_actor.id)
            self._transition(s1, row.id, actor1, "approved", "supply.approved")
            with self.assertRaises(HTTPException) as cm:
                self._transition(s2, row.id, actor2, "approved", "supply.approved")
            self.assertEqual(cm.exception.status_code, 409)
        finally:
            s1.close()
            s2.close()

    def test_valid_transition_submitted_to_approved_sets_approver(self):
        from app.models import SupplyRequest

        row = self._seed_request(status="submitted")
        self._transition(self.session, row.id, self.actor, "approved", "supply.approved")
        self.session.expire_all()
        refreshed = self.session.get(SupplyRequest, row.id)
        self.assertEqual(refreshed.status, "approved")
        self.assertEqual(refreshed.approved_by_user_id, self.actor.id)

    def test_valid_transition_approved_to_ordered_updates_approver(self):
        from app.models import SupplyRequest

        row = self._seed_request(status="approved")
        row.approved_by_user_id = self.actor.id
        self.session.add(row)
        self.session.commit()
        self._transition(self.session, row.id, self.other_actor, "ordered", "supply.ordered")
        self.session.expire_all()
        refreshed = self.session.get(SupplyRequest, row.id)
        self.assertEqual(refreshed.status, "ordered")
        self.assertEqual(refreshed.approved_by_user_id, self.other_actor.id)
