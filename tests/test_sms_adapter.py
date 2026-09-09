from unittest import TestCase
from unittest.mock import patch, MagicMock
import httpx
from app.config import Settings
from app.team.sms import send_sms


class SmsAdapterTests(TestCase):
    def config(self):
        return Settings(_env_file=None).model_copy(update=dict(sms_provider="twilio", sms_twilio_account_sid="AC"+"b"*32,
            sms_twilio_auth_token="test-token", sms_from_number="+18005550100"))

    def send(self, post):
        with patch("app.team.sms.httpx.Client") as client:
            client.return_value.__enter__.return_value.post = post
            result = send_sms(to_phone="+12025550123", body="Generic notice", settings=self.config(),
                status_callback="https://example.com/webhooks/twilio/status/abc")
        return result

    def test_status_callback_is_passed_and_sid_checked(self):
        post = MagicMock(return_value=httpx.Response(201, json={"sid":"SM"+"a"*32,"status":"queued"}))
        self.assertTrue(self.send(post).success)
        self.assertIn("StatusCallback", post.call_args.kwargs["data"])

    def test_success_without_message_sid_is_unknown(self):
        for body in ({}, [], {"sid":"bad"}):
            self.assertEqual(self.send(MagicMock(return_value=httpx.Response(201, json=body))).status, "unknown")

    def test_only_preconnection_errors_are_retryable(self):
        for exc, status in ((httpx.ConnectError("private"),"not_connected"),
                            (httpx.ReadTimeout("private"),"transport_error"),
                            (httpx.WriteError("private"),"transport_error")):
            result = self.send(MagicMock(side_effect=exc))
            self.assertEqual(result.status, status)
            self.assertNotIn("private", result.error)

    def test_provider_errors_store_code_not_private_text(self):
        result = self.send(MagicMock(return_value=httpx.Response(400, json={"code":21610,"message":"private phone"})))
        self.assertEqual(result.error, "21610")
