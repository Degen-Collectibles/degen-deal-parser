import base64
import hashlib
import hmac
from datetime import timedelta
from unittest import TestCase
from unittest.mock import patch

from sqlmodel import Session, select
from tests import test_sms_consent as consent_tests
from app.config import get_settings
from app.models import SmsOutbox, SmsSuppression, SmsInboundEvent, SmsDispatchLease, utcnow
from app.team.sms import SmsSendResult
from app.team.sms_consent import consent_context, record_consent
from app.team.sms_outbox import dispatch_one
from app.team.team_notifications import notify_employee


SID = "SM" + "a" * 32
ACCOUNT = "AC" + "b" * 32


class SmsDeliveryTests(TestCase):
    setUp = consent_tests.SmsConsentTests.setUp
    tearDown = consent_tests.SmsConsentTests.tearDown
    _setup_portal = consent_tests.SmsConsentTests._setup_portal
    _teardown_portal = consent_tests.SmsConsentTests._teardown_portal
    _stop_user_patchers = consent_tests.SmsConsentTests._stop_user_patchers
    _login_as = consent_tests.SmsConsentTests._login_as
    _grant = consent_tests.SmsConsentTests._grant

    def enabled(self):
        return patch.multiple(get_settings(), sms_operational_alerts_enabled=True,
            sms_dispatcher_enabled=True, sms_webhooks_enabled=True, sms_provider="twilio",
            sms_twilio_account_sid=ACCOUNT, sms_twilio_auth_token="test-token",
            sms_from_number="+18005550100", sms_twilio_messaging_service_sid="",
            sms_callback_base_url="https://example.com", public_base_url="https://example.com")

    def queue(self, commit=True):
        self._grant()
        with patch.object(get_settings(), "sms_operational_alerts_enabled", True):
            notify_employee(self.session, user_id=self.user.id, actor_user_id=None,
                kind="schedule", title="Private", body="Sensitive salary details")
        if commit:
            self.session.commit()
        return self.session.exec(select(SmsOutbox).order_by(SmsOutbox.id.desc())).first()

    def row(self):
        self.session.expire_all()
        return self.session.exec(select(SmsOutbox).order_by(SmsOutbox.id.desc())).first()

    def post(self, path, data, signature=True):
        value = "https://example.com" + path + "".join(k + data[k] for k in sorted(data))
        sig = base64.b64encode(hmac.new(b"test-token", value.encode(), hashlib.sha1).digest()).decode()
        return self.client.post(path, data=data, headers={"X-Twilio-Signature":sig if signature else "invalid"})

    def inbound(self, event, sid=SID):
        return self.post("/webhooks/twilio/inbound", dict(AccountSid=ACCOUNT,
            MessageSid=sid, From="+12025550123", To="+18005550100", Body=event, OptOutType=event))

    def test_rollback_removes_intent_without_network(self):
        with patch("app.team.sms_outbox.send_sms") as sender:
            self.queue(commit=False)
            self.session.rollback()
        sender.assert_not_called()
        self.assertIsNone(self.row())

    def test_post_commit_dispatch_once_and_generic_body(self):
        self.queue()
        with self.enabled(), patch("app.team.sms_outbox.send_sms", return_value=SmsSendResult("twilio", "queued", SID)) as sender:
            self.assertTrue(dispatch_one(self.engine))
            self.assertFalse(dispatch_one(self.engine))
        self.assertEqual(sender.call_count, 1)
        body = sender.call_args.kwargs["body"]
        self.assertIn("STOP", body)
        self.assertNotIn("Sensitive", body)
        self.assertEqual(self.row().status, "accepted")
        self.assertIn(self.row().attempt_token, sender.call_args.kwargs["status_callback"])

    def test_all_delivery_flags_fail_closed(self):
        self.queue()
        with patch("app.team.sms_outbox.send_sms") as sender:
            for field in ("sms_operational_alerts_enabled", "sms_dispatcher_enabled", "sms_webhooks_enabled"):
                with self.enabled(), patch.object(get_settings(), field, False):
                    self.assertFalse(dispatch_one(self.engine))
        sender.assert_not_called()

    def test_withdrawal_before_dispatch_cancels(self):
        self.queue()
        record_consent(self.session, user_id=self.user.id, opted_in=False)
        self.session.commit()
        with self.enabled(), patch("app.team.sms_outbox.send_sms") as sender:
            dispatch_one(self.engine)
        sender.assert_not_called()
        self.assertEqual(self.row().status, "cancelled")

    def test_expired_queue_never_sends(self):
        row = self.queue()
        row.expires_at = utcnow() - timedelta(seconds=1)
        self.session.add(row)
        self.session.commit()
        with self.enabled(), patch("app.team.sms_outbox.send_sms") as sender:
            dispatch_one(self.engine)
        sender.assert_not_called()
        self.assertEqual(self.row().status, "expired")

    def test_429_retries_are_bounded(self):
        self.queue()
        with self.enabled(), patch("app.team.sms_outbox.send_sms", return_value=SmsSendResult("twilio", "http_429", error="20429")) as sender:
            for attempt in range(3):
                dispatch_one(self.engine)
                row = self.row()
                self.assertEqual(row.attempts, attempt + 1, str(row))
                row.next_attempt_at = utcnow() - timedelta(seconds=1)
                self.session.add(row)
                self.session.commit()
            dispatch_one(self.engine)
        self.assertEqual(sender.call_count, 3)
        self.assertEqual(self.row().status, "failed")

    def test_timeout_is_unknown_never_retried(self):
        self.queue()
        with self.enabled(), patch("app.team.sms_outbox.send_sms", return_value=SmsSendResult("twilio", "transport_error", error="timeout")) as sender:
            dispatch_one(self.engine)
            dispatch_one(self.engine)
        self.assertEqual(sender.call_count, 1)
        self.assertEqual(self.row().status, "unknown")

    def test_stale_inflight_becomes_unknown(self):
        row = self.queue()
        row.status, row.attempts = "dispatching", 1
        row.updated_at = utcnow() - timedelta(minutes=4)
        self.session.add(row)
        self.session.commit()
        with self.enabled(), patch("app.team.sms_outbox.send_sms") as sender:
            dispatch_one(self.engine)
        sender.assert_not_called()
        self.assertEqual(self.row().status, "unknown")

    def test_other_dispatcher_lease_prevents_send(self):
        self.queue()
        self.session.add(SmsDispatchLease(owner="other", until=utcnow()+timedelta(minutes=1)))
        self.session.commit()
        with self.enabled(), patch("app.team.sms_outbox.send_sms") as sender:
            self.assertFalse(dispatch_one(self.engine))
        sender.assert_not_called()

    def test_signed_receipt_reconciles_unknown_and_cannot_regress(self):
        row = self.queue()
        row.status, row.attempt_token = "unknown", "abc"
        self.session.add(row)
        self.session.commit()
        data = dict(AccountSid=ACCOUNT, MessageSid=SID, To="+12025550123", MessageStatus="delivered")
        with self.enabled():
            self.assertEqual(self.post("/webhooks/twilio/status/abc", data, False).status_code, 403)
            self.assertEqual(self.post("/webhooks/twilio/status/abc", data).status_code, 204)
            data["MessageStatus"] = "sent"
            self.assertEqual(self.post("/webhooks/twilio/status/abc", data).status_code, 204)
        self.assertEqual(self.row().status, "delivered")
        self.assertEqual(self.row().provider_sid, SID)

    def test_wrong_account_or_recipient_rejected(self):
        row = self.queue()
        row.attempt_token = "abc"
        self.session.add(row)
        self.session.commit()
        data = dict(AccountSid="wrong", MessageSid=SID, To="+12025550123", MessageStatus="sent")
        with self.enabled():
            self.assertEqual(self.post("/webhooks/twilio/status/abc", data).status_code, 403)
            data.update(AccountSid=ACCOUNT, To="+12025550199")
            self.assertEqual(self.post("/webhooks/twilio/status/abc", data).status_code, 400)

    def test_stop_deduplicates_cancels_queue_start_requires_fresh_consent(self):
        self.queue()
        with self.enabled():
            self.assertEqual(self.inbound("STOP").status_code, 200)
            self.assertEqual(self.inbound("STOP").status_code, 200)
            self.session.expire_all()
            self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])
            with self.assertRaises(ValueError):
                self._grant()
            self.assertEqual(self.inbound("START", "SM"+"c"*32).status_code, 200)
            self.session.expire_all()
            self.assertFalse(consent_context(self.session, self.user.id)["opted_in"])
            self._grant()
            self.assertTrue(consent_context(self.session, self.user.id)["opted_in"])
        self.assertEqual(self.row().status, "cancelled")
        self.assertEqual(len(self.session.exec(select(SmsInboundEvent)).all()), 2)

    def test_advanced_optout_help_does_not_duplicate_provider_reply(self):
        with self.enabled():
            self.assertEqual(self.inbound("HELP").text, "<Response/>")
        self.assertEqual(len(self.session.exec(select(SmsSuppression)).all()), 0)

    def test_webhooks_disabled_reject_even_signed(self):
        with self.enabled(), patch.object(get_settings(), "sms_webhooks_enabled", False):
            self.assertEqual(self.inbound("STOP").status_code, 503)

    def test_admin_log_is_role_protected_and_masked(self):
        self.queue()
        self.assertEqual(self.client.get("/team/admin/sms").status_code, 403)
        self._login_as("admin")
        response = self.client.get("/team/admin/sms")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Delivery paused", response.text)
        self.assertNotIn("2025550123", response.text)
        self.assertNotIn("Sensitive salary", response.text)

    def test_callback_arrives_before_send_returns(self):
        self.queue()
        def provider(**kwargs):
            path = kwargs["status_callback"].removeprefix("https://example.com")
            response = self.post(path, dict(AccountSid=ACCOUNT, MessageSid=SID,
                To="+12025550123", MessageStatus="delivered"))
            self.assertEqual(response.status_code, 204)
            return SmsSendResult("twilio", "queued", SID)
        with self.enabled(), patch("app.team.sms_outbox.send_sms", side_effect=provider):
            dispatch_one(self.engine)
        self.assertEqual(self.row().status, "delivered")

    def test_provider_21610_blocks_future_enrollment_and_delivery(self):
        self.queue()
        with self.enabled(), patch("app.team.sms_outbox.send_sms", return_value=SmsSendResult("twilio", "http_400", error="21610")):
            dispatch_one(self.engine)
        self.session.expire_all()
        self.assertTrue(consent_context(self.session, self.user.id)["provider_blocked"])
        self.assertEqual(self.row().status, "failed")

    def test_phone_change_cancels_old_intent(self):
        from app.team.pii import encrypt_pii
        self.queue()
        self.profile.phone_enc = encrypt_pii("2025550199")
        self.session.add(self.profile)
        self.session.commit()
        with self.enabled(), patch("app.team.sms_outbox.send_sms") as sender:
            dispatch_one(self.engine)
        sender.assert_not_called()
        self.assertEqual(self.row().status, "cancelled")

    def test_invalid_signature_and_duplicate_fields_do_not_suppress(self):
        with self.enabled():
            data = dict(AccountSid=ACCOUNT, MessageSid=SID, From="+12025550123", To="+18005550100", Body="STOP")
            self.assertEqual(self.post("/webhooks/twilio/inbound", data, False).status_code, 403)
            response = self.client.post("/webhooks/twilio/inbound", content="Body=STOP&Body=START",
                headers={"Content-Type":"application/x-www-form-urlencoded"})
            self.assertEqual(response.status_code, 400)
        self.assertEqual(len(self.session.exec(select(SmsSuppression)).all()), 0)

    def test_help_without_advanced_optout_and_paused_delivery(self):
        data = dict(AccountSid=ACCOUNT, MessageSid=SID, From="+12025550123", To="+18005550100", Body="HELP")
        with self.enabled(), patch.object(get_settings(), "sms_operational_alerts_enabled", False):
            self.assertEqual(self.post("/webhooks/twilio/inbound", data).text, "<Response/>")
        data["MessageSid"] = "SM" + "d" * 32
        with self.enabled():
            self.assertIn("info@degencollectibles.com", self.post("/webhooks/twilio/inbound", data).text)

    def test_ten_recent_notifications_defer_next_recipient_send(self):
        first = self.queue()
        for i in range(10):
            self.session.add(SmsOutbox(notification_id=1000+i, user_id=self.user.id,
                phone_binding=first.phone_binding, phone_fingerprint=first.phone_fingerprint,
                phone_label=first.phone_label, status="accepted", attempts=1, expires_at=utcnow()+timedelta(hours=1)))
        self.session.commit()
        with self.enabled(), patch("app.team.sms_outbox.send_sms") as sender:
            self.assertFalse(dispatch_one(self.engine))
        sender.assert_not_called()
        self.session.refresh(first)
        self.assertEqual(first.error_code, "rate_limited")
