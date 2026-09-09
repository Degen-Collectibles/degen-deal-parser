"""Consent is independent of account access and is enforced before SMS."""
import json
import unittest
from unittest.mock import patch

from sqlmodel import Session, select

from tests.test_employee_portal_wave3 import _PortalHarness
from app.models import AuditLog, EmployeeProfile
from app.team.pii import encrypt_pii
from app.team.sms_consent import CONSENT_ACTION, consent_context, record_consent


class SmsConsentTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()
        self.user = self._login_as("manager")
        self.session.add(self.user)
        self.profile = EmployeeProfile(user_id=self.user.id, phone_enc=encrypt_pii("2025550123"))
        self.session.add(self.profile)
        self.session.commit()

    def tearDown(self):
        self.client.close()
        self._teardown_portal()

    def _grant(self, commit=True):
        state = consent_context(self.session, self.user.id)
        record_consent(self.session, user_id=self.user.id, opted_in=True,
                       version=state["version"], phone_binding=state["phone_binding"])
        if commit:
            self.session.commit()

    def _post(self, **overrides):
        state = consent_context(self.session, self.user.id)
        data = dict(action="subscribe", sms_opt_in="yes", consent_version=state["version"],
                    phone_binding=state["phone_binding"], csrf_token=self._csrf())
        data.update(overrides)
        return self.client.post("/team/profile/sms", data=data, follow_redirects=False)

    def test_form_is_optional_unchecked_and_separate(self):
        response = self.client.get("/team/profile")
        self.assertEqual(response.status_code, 200)
        import re
        checkbox = re.search(r'<input[^>]+id="sms-opt-in"[^>]*>', response.text).group()
        self.assertNotIn("checked", checkbox)
        self.assertNotIn("required", checkbox)
        self.assertIn('action="/team/profile/sms"', response.text)
        self.assertIn('action="/team/profile"', response.text)
        self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])

    def test_phone_alone_never_sends_but_portal_notice_survives(self):
        from app.team.team_notifications import notify_employee
        with patch("app.team.sms_outbox.send_sms") as sender:
            row = notify_employee(self.session, user_id=self.user.id, actor_user_id=None,
                                  kind="schedule", title="Schedule", body="Private content")
            self.session.commit()
        sender.assert_not_called()
        self.assertEqual(json.loads(row.details_json)["sms"]["status"], "consent_required")

    def test_unchecked_post_does_not_grant_consent(self):
        self.assertEqual(self._post(sms_opt_in="").status_code, 303)
        self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])

    def test_explicit_opt_in_records_version_self_actor_and_no_plain_phone(self):
        self.assertEqual(self._post().status_code, 303)
        state = consent_context(self.session, self.user.id)
        self.assertTrue(state["opted_in"])
        row = self.session.exec(select(AuditLog).where(AuditLog.action == CONSENT_ACTION)).one()
        self.assertEqual(row.actor_user_id, self.user.id)
        self.assertNotIn("2025550123", row.details_json)
        self.assertEqual(json.loads(row.details_json)["version"], state["version"])

    def test_withdrawal_blocks_sms_and_keeps_profile_access(self):
        self._grant()
        self.assertEqual(self._post(action="unsubscribe", sms_opt_in="").status_code, 303)
        self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])
        self.assertEqual(self.client.get("/team/profile").status_code, 200)

    def test_other_role_cannot_opt_in(self):
        self.user.role = "employee"
        self.session.add(self.user)
        self.session.commit()
        self.assertEqual(self._post().status_code, 400)
        self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])

    def test_stale_phone_or_terms_cannot_be_submitted(self):
        self.assertEqual(self._post(phone_binding="old").status_code, 400)
        self.assertEqual(self._post(consent_version="old").status_code, 400)
        self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])

    def test_phone_change_and_restore_never_revives_old_consent(self):
        self._grant()
        for phone in ("2025550199", "2025550123"):
            self.profile.phone_enc = encrypt_pii(phone)
            self.session.add(self.profile)
            self.session.commit()
            self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])

    def test_rollback_does_not_persist_consent(self):
        self._grant(commit=False)
        self.session.rollback()
        with Session(self.engine) as other:
            self.assertFalse(consent_context(other, self.user.id)["opted_in"])

    def test_missing_csrf_cannot_change_preference(self):
        self.assertEqual(self._post(csrf_token="").status_code, 403)
        self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])

    def test_consented_notice_contains_no_private_body(self):
        from app.team.team_notifications import notify_employee
        from app.team.sms import SmsSendResult
        self._grant()
        from app.config import get_settings
        with patch.object(get_settings(), "sms_operational_alerts_enabled", True), patch("app.team.sms_outbox.send_sms", return_value=SmsSendResult("dry_run", "dry_run", dry_run=True)) as sender:
            notify_employee(self.session, user_id=self.user.id, actor_user_id=None,
                            kind="schedule", title="Private salary", body="Sensitive details")
        sender.assert_not_called()
        from app.models import SmsOutbox
        queued = self.session.exec(select(SmsOutbox)).one()
        self.assertEqual(queued.status, "queued")
        self.assertNotIn("Sensitive details", str(queued))

    def test_consent_collection_does_not_activate_delivery(self):
        from app.config import get_settings
        from app.team.team_notifications import notify_employee
        self._grant()
        with patch.object(get_settings(), "sms_operational_alerts_enabled", False), patch("app.team.sms_outbox.send_sms") as sender:
            row = notify_employee(self.session, user_id=self.user.id, actor_user_id=None,
                                  kind="schedule", title="Schedule", body="Details")
        sender.assert_not_called()
        self.assertEqual(json.loads(row.details_json)["sms"]["status"], "pilot_not_enabled")

    def test_notification_status_requires_consent_not_just_phone(self):
        response = self.client.get("/team/notifications")
        self.assertIn("Optional SMS subscription", response.text)
        self.assertIn('<span class="pt-check-icon">Off</span>', response.text)
        self._grant()
        response = self.client.get("/team/notifications")
        self.assertIn('<span class="pt-check-icon">On</span>', response.text)

    def test_role_removal_blocks_previously_consented_recipient(self):
        self._grant()
        self.user.role = "employee"
        self.session.add(self.user)
        self.session.commit()
        self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])
