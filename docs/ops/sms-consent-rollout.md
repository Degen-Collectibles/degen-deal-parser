# Owners/managers SMS consent and toll-free correction

## Scope

This release fixes consent collection and prepares the rejected toll-free
registration for correction. It does not activate delivery or implement the
separate durable SMS outbox/callback workstream. Keep
`SMS_OPERATIONAL_ALERTS_ENABLED=false` (the default) and `SMS_PROVIDER=dry_run`
while deploying and demonstrating the consent flow.

The business contact and notification email approved by Jeffrey is
`info@degencollectibles.com`. The account's existing toll-free sender ends in
2027. Twilio rejected it with 30482 (business email domain) and 30475 (consent
bundled with account access). Do not purchase another number to work around
these errors.

## What changes

- `/team/profile` adds a separate optional checkbox and separate POST form at
  `/team/profile/sms`. Owners use the existing admin role; managers use manager.
- The checkbox is initially unchecked, not required, and not part of account
  creation or the main profile update form. The latest explicit self-service
  consent is recorded in AuditLog with time, wording/version, source, and a
  hash binding to the encrypted phone record. No table/column migration.
- Missing consent, withdrawal, role/inactive status, phone change, malformed
  evidence, or disabled delivery prevents the notification sender from running.
- Portal notifications remain available. SMS bodies contain a generic notice
  and authenticated notification-page link rather than sensitive business data.
- SMS invitations are unavailable during this pilot. The existing copy-link
  invitation workflow remains available; password reset continues using email.
- SMS status shows consent rather than inferring enrollment from phone presence.
- `/static/compliance/team-sms-consent.html` documents the actual opt-in flow,
  terms, privacy, message scope, and unsubscribe options.

## Twilio draft correction packet

Business contact and notification email: `info@degencollectibles.com`.

Use case: Account notifications only. Remove Two-factor authentication.

Description:

> Degen Collectibles sends internal operational alerts only to owners and
> managers who separately opt in through their authenticated Degen Team
> profile. Alerts cover schedules, shifts, and team operations. SMS is optional
> and is not required for app access. No marketing, customer outreach, employee
> invitations, password resets, or two-factor authentication messages are sent
> in this pilot.

Sample:

> Degen Collectibles: A team operations update is available.
> https://ops.degencollectibles.com/team/notifications
> Reply STOP to unsubscribe or HELP for help.

Opt-in type: Web form.

Terms and opt-in explanation:
`https://ops.degencollectibles.com/static/compliance/team-sms-consent.html`.
Use this page for the privacy-policy URL as well: it contains the SMS privacy
section. The old `https://degencollectibles.com/policies/privacy-policy` URL
returned a visible 404 during this review. The main business website publicly
lists `info@degencollectibles.com` as its contact email.
This URL must show the new content live before submission. Attach a public
screenshot or walkthrough of the deployed form with synthetic account data;
do not disclose employee profiles or invite/reset tokens. Local previews alone
are not evidence that the production flow is deployed.

Additional information after deployment:

> Owners and managers enroll through a separate optional, unchecked SMS
> checkbox in their authenticated Degen Team profile. Providing a phone number,
> onboarding, or agreeing to other terms does not enroll anyone. App access
> remains available without SMS. Consent is recorded with recipient, time,
> wording version, and saved phone binding. Changing the phone requires a new
> opt-in. Recipients can unsubscribe in the profile or reply STOP.

No SMS keyword enrollment is implemented by the app. Do not claim an app
opt-in auto-reply. Verify the actual provider STOP/START/HELP behavior before
enabling delivery; carrier-managed opt-out remains authoritative.

## Outstanding account checks

- Jeffrey confirmed the exact legal name on the EIN letter is Degen
  Collectibles LLC. The open toll-free application draft now uses that name,
  matching the primary profile. The tax identifier was not changed.
- Twilio's primary profile remains Draft. Its completion flow invokes Persona
  identity verification with biometric terms. Jeffrey must handle any required
  identity verification and consent. The SMS-specific correction wizard is
  separately accessible and allows an unsubmitted draft.
- The form has no separately verified Save draft action. Values entered in the
  open tab are not evidence of a saved or accepted registration. Preserve this
  packet in case the browser draft expires.

## Release preflight and verification

1. Review exact branch diff and tests. Verify origin/main/deployed revisions.
2. Approve normal GitHub/Green deployment of only this scoped change, retaining
   the preceding revision for rollback. No direct production file edits.
3. Verify both provider and operational delivery remain disabled. Do not change
   secrets or send a test text as part of consent deployment.
4. Verify public terms anonymously and profile form with each permitted role.
   Declining consent must preserve account access. Grant/withdraw only using an
   explicitly approved pilot/test account; verify audit evidence without PII.
5. Prepare sanitized public evidence of the deployed flow. Recheck email,
   identity, existing sender, scope, sample, and URLs in Twilio. Only submit
   after Jeffrey approves the concrete application and any terms/fees.
6. Record verification outcome. Do not describe a submitted request as approved.

## Delivery activation is a separate gate

Before setting `SMS_OPERATIONAL_ALERTS_ENABLED=true`: complete durable
post-commit dispatch and duplicate/unknown-outcome handling; signed provider
delivery/opt-out callbacks; provider sender verification; recipient/category
controls and rate limits; then obtain approval for one named test recipient and
exact message. The existing synchronous sender must not be treated as a
transactional queue. Consent changes themselves never send a text.

## Rollback

Disable operational delivery before any rollback. Keep AuditLog evidence;
do not delete consent/withdrawal history. This release needs no schema rollback.
Restore the previous code revision only with SMS disabled, because that version
does not enforce separate consent. Already delivered texts cannot be recalled.
Reverting public terms to old wording would recreate the verification issue;
prefer leaving SMS paused and correcting forward.
