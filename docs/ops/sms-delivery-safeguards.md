# SMS delivery safeguards

## Problem and approved scope

Operational SMS previously ran before the business transaction committed.
The approved follow-up adds durable post-commit dispatch, visible delivery
states, and signed STOP/START/HELP handling. Only opted-in active admins and
managers qualify. No live enablement, credentials, Twilio configuration,
customer campaigns, payroll contents, or automatic resend of uncertain sends
are included. Existing portal notifications remain the primary record.

## Behavior and success criteria

- Notification and outbox intent commit or roll back together. The dispatcher
  runs in a separate session and never contacts Twilio from the request handler.
- One intent per notification is enforced by a unique database index.
- A database lease serializes dispatchers; interrupted attempts become Unknown
  after three minutes, never automatically requeued. Exactly-once external
  delivery cannot be guaranteed: the app deliberately prefers holding an
  uncertain attempt over risking a duplicate text.
- Recheck active role, phone binding, latest consent, suppression, and delivery
  gates immediately before dispatch. A STOP received after a provider request
  is already in flight cannot recall that message.
- Intents expire after one hour. At most ten recent message intents per phone
  are dispatched per hour. Only connection-not-established and HTTP 429 are
  retried, with backoff and at most three attempts. Timeout, 5xx, malformed
  acceptance, and interruption require reconciliation.
- Generic SMS contains only the authenticated notifications link, branding,
  and STOP/HELP instructions. The outbox stores no plaintext phone or message
  body, only phone binding, keyed fingerprint, masked label, and provider SID.
- Signed callbacks correlate the attempt token, account, recipient, and SID;
  compare-and-set transitions prevent late callbacks from regressing delivery.
- STOP cancels pending intents and blocks the phone. START clears provider
  suppression but never restores app consent: the user must opt in again.
  Duplicate inbound MessageSids are ignored. Provider error 21610 also blocks
  further attempts. Advanced Opt-Out replies are not duplicated by the app.
- `/team/admin/sms` is an admin-only, read-only, paginated delivery log. Unknown
  outcomes have no resend button. Use Twilio logs to reconcile them first.

## Schema and runtime

Four additive SQLModel tables: `smsoutbox`, `smssuppression`, `smsinboundevent`,
and `smsdispatchlease`. They are created by the existing `init_db/create_all`
path. No existing table columns or audit history are modified. SQLite uses
atomic updates/upserts; PostgreSQL uses the corresponding PostgreSQL upsert.
The dispatcher uses the existing FastAPI lifecycle and background task monitor.

Defaults remain off:

```text
SMS_PROVIDER=dry_run
SMS_OPERATIONAL_ALERTS_ENABLED=false
SMS_DISPATCHER_ENABLED=false
SMS_WEBHOOKS_ENABLED=false
SMS_CALLBACK_BASE_URL=
```

`SMS_CALLBACK_BASE_URL` must be the exact public HTTPS origin, without a path,
query, credentials, or fragment. Signatures use that configured origin rather
than forwarded headers. Callback bodies must be form-encoded with unique
fields, no query parameters, and at most 16 KiB.

## Provider configuration and staged rollout

Twilio toll-free verification was submitted separately and is not assumed
approved. Deployment and live activation require separate approval.

1. Deploy with the flags above. Verify additive tables and admin page, without
   changing subscriptions or sending texts. Retain the preceding revision.
2. Validate database behavior on an isolated PostgreSQL instance before live
   activation. Local regression testing uses SQLite; SQL portability is not
   evidence of a live PostgreSQL concurrency test.
3. Set the canonical callback origin and configure the existing Twilio sender's
   incoming-message webhook to POST `/webhooks/twilio/inbound`. Outbound sends
   supply their own `/webhooks/twilio/status/{attempt_token}` callback URL.
4. Verify TLS, edge access, signatures, and actual carrier STOP/START/HELP
   behavior. A command-line fetch of the public site previously received 403
   while Chrome loaded it: Twilio callback reachability must be verified before
   activation. Do not weaken site authentication globally to fix callback access.
5. Confirm provider approval and explicit opt-in, then approve one named test
   recipient and exact message. Enable dispatcher/webhooks/operational delivery
   only for that controlled test; verify received text and delivery callback.
6. Expand gradually to opted-in owners/managers. Monitor failed/unknown rows.

## Verification and rollback

Focused regressions cover transaction rollback, one-time dispatch, retries,
expiration, consent revocation, phone change, lease ownership, signed callbacks,
callback/send races, duplicate callbacks, STOP/START, provider errors, masking,
admin authorization, and paused delivery. Full repo suite is required before
commit. No test may contact production or send actual messages.

Rollback: first disable dispatcher and operational SMS. Preserve the four new
tables and consent/suppression evidence, then revert code through normal GitHub
deployment. Never enable the older sender after rollback, since it cannot honor
the new suppression records. Do not delete unknown attempts to force retries.
An already delivered SMS cannot be recalled.

Provider references: [signature validation](https://www.twilio.com/docs/usage/security),
[incoming message parameters](https://www.twilio.com/docs/messaging/guides/webhook-request),
and [Advanced Opt-Out](https://www.twilio.com/docs/messaging/tutorials/advanced-opt-out).
