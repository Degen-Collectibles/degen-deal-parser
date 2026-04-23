# Degen Employee Portal — Read-Only Audit (2026-04-23)

**Auditor:** Mission Control (Claude)
**Scope:** Phase 1 waves 1–4.5 — PII-encrypted employee records, CSRF + rate-limited auth, audit log, permissions matrix, employee routes, admin routes, plus the post-commit `7e4a04d` mobile drawer / bottom nav / editable-schedule changes.
**Repo state:** `main @ 1091642` (rebase onto `origin/main` aborted — five pre-existing unstaged modifications in the tree; audit performed against the current working tree). No code was modified; this file is the only artifact.

**Severity legend:** `[BUG]` correctness, `[SEC]` security, `[UX]` usability, `[PERF]` performance, `[CODE]` maintainability, `[DATA]` data integrity. Each finding is tagged `(LOW|MED|HIGH|CRITICAL)`.

---

## 1. Executive summary

The portal is in solid shape for an internal pilot. The core PII model (Fernet + `email_lookup_hash`, audit-before-decrypt, fail-closed boot) is well-thought-out, CSRF coverage on state-changing routes is consistent, and the draft-employee / invite flow avoids a whole category of race conditions. The permissions matrix is clean and admin short-circuit keeps break-glass usable.

The biggest risks are **deployment-shape bugs**, not design bugs:

1. Rate limiting is keyed on `request.client.host` with no `X-Forwarded-For` support — behind any reverse proxy (which the production deploy likely has), every caller looks like one IP and all per-user limits collapse into a single shared bucket. **`[SEC] HIGH`**
2. Session cookie defaults are dev-grade (`DEFAULT_SESSION_SECRET = "degen-dev-session-secret"`, `session_https_only=False`, 30-day max age), and `validate_runtime_secrets` only fires in `public_host_mode`, which the team portal isn't gated on. **`[SEC] HIGH`**
3. Login has a username-enumeration timing oracle: `authenticate_user` returns immediately on unknown username without performing a dummy bcrypt, so an unauthenticated attacker can enumerate valid accounts by timing. **`[SEC] MED`**
4. `hourly_rate_cents` accepts an arbitrary Python int with no bounds check, so a typo on the admin page can persist `999999999` into the DB — later payroll math will wrap or blow up. **`[DATA] MED`**
5. `app/routers/team_admin_schedule.py` is 2,036 lines with business rules, SQL, and HTML hinting intermixed; further growth on the schedule feature will be painful. **`[CODE] MED`**

Nothing rises to CRITICAL in the ship-stopping sense — the fail-closed PII boot, CSRF, and permissions gating mean a missing key or misconfigured role doesn't silently leak. If Jeffrey only had a day, the rate-limit + session-cookie fixes alone would retire most of the real-world exposure.

**Findings by severity:** CRITICAL 0 / HIGH 7 / MED 18 / LOW 12 = **37 total**.

---

## 2. Per-feature findings

### 2.1 Auth (login, invite, forgot/reset, pw-change)

- **`[SEC] HIGH` — Username enumeration via timing.** `app/auth.py:authenticate_user` returns `None` immediately on unknown username without computing a throwaway bcrypt/PBKDF2. A `.2s` vs `.001s` response reliably tells an attacker whether a username exists. Fix: run `pbkdf2_hmac` against a fixed dummy hash on the unknown-user path.
- **`[SEC] HIGH` — Default session secret.** `app/config.py:DEFAULT_SESSION_SECRET = "degen-dev-session-secret"` is used unless `public_host_mode` is on. The team portal is reachable without that flag. `validate_runtime_secrets` should also run whenever `employee_portal_enabled=True`.
- **`[SEC] HIGH` — Session cookie is not HTTPS-only by default.** `session_https_only=False`, `session_max_age_seconds = 30 * 24 * 3600`. Any HTTP fallback leaks the session cookie for a month. Default to `Secure`; require explicit opt-out for local dev.
- **`[SEC] MED` — Rate limit bypass behind a proxy.** `app/rate_limit.py:25-45` keys on `request.client.host`. Reverse proxies make every request share the same key, both over-limiting real users *and* under-limiting attackers (they still hit the shared bucket but so does everyone). Read `X-Forwarded-For` (trust only the configured proxy hop), or switch to per-user keying on authenticated endpoints.
- **`[SEC] MED` — Per-process rate limit buckets.** The `collections.deque` state is in-process, so `uvicorn --workers 4` multiplies the effective limit by 4. OK for single-worker today; note this before scaling.
- **`[SEC] MED` — Default admin password.** `app/config.py:DEFAULT_ADMIN_PASSWORD = "degen1234"`. Check on boot that this is not still in effect for any admin user; log or refuse start.
- **`[BUG] MED` — Silent email-drop on invite consume.** `app/auth.py:consume_invite_token` drops the new-user-supplied email if it clashes with an existing account and writes `account.invite_email_dropped` audit. The user sees a 303 success with no UI notice that their email was not saved. At minimum flash a warning on the next page.
- **`[UX] MED` — No "account locked" UX after repeated failures.** Login rate-limit returns 429 with a generic message; users don't learn they're locked until the next attempt. Add a "try again in N minutes" hint using the remaining TTL.
- **`[UX] LOW` — Password rules are discovered by submission.** `validate_password_strength` rejects with a text error; `invite_accept.html` has a client-side scorer but the two are not unified. Render the server rules inline on every password field.
- **`[UX] LOW` — "Skip" on address/emergency contact during onboarding.** The skip button (`invite_accept.html` step 4/5) is fine but has no follow-up nag — profile page doesn't surface missing fields. Add a "complete your profile" banner when those fields are blank.
- **`[CODE] LOW` — `authenticate_user` audit-logs failure reason in cleartext details.** `audit.login.failure` includes reason `"no_such_user"` vs `"bad_password"`. That distinction is itself an oracle if the audit log is ever exposed to managers. Normalize to a single `"auth_failed"` reason.

### 2.2 PII / Employees

- **`[SEC] HIGH` — PII reveal renders inline with `user-select:all`.** `app/templates/team/admin/employee_detail.html` shows the decrypted SSN/DOB inline after reveal. Once rendered, it stays on screen (no TTL, no "hide" button), and shoulder-surfers see it on the monitor. Auto-hide after 30 s; require re-click to re-reveal.
- **`[SEC] MED` — PII *edit* needs no reveal permission.** `team_admin_employees.py:pii_update` (~L690) accepts a blind overwrite. An admin who can edit can silently replace a field without first seeing it — useful attack path for social-engineered "correction" requests. Require `admin.employees.pii_reveal` *and* `pii_edit` for non-empty writes, or at minimum audit both the old and new ciphertext hashes.
- **`[DATA] MED` — `hourly_rate_cents` has no bounds check.** `team_admin_employees.py:603-612` parses `int(form["hourly_rate_cents"])` with no clamp. A typo (`100000` meaning dollars instead of cents) persists `$1,000/hr`. Clamp `[0, 20_000_00]` or similar and reject otherwise.
- **`[BUG] MED` — Dead `reveal_field` query param.** `team_admin_employees.py:admin_employee_detail` accepts `reveal_field: Optional[str] = Query(...)` but the template reveals fields via POST. The Query param has no effect — either wire it up or remove it to avoid the impression that GET-reveal exists.
- **`[SEC] MED` — `generate_invite_token` self-audit is caller-provided.** `app/auth.py:generate_invite_token` does not emit its own audit row; every caller has to remember. Co-locating the write inside the function (with caller-provided `reason`) would prevent drift.
- **`[UX] MED` — Employee search is username-only.** `team_admin_employees.py` list page search only matches `User.username`. Admin is likely to search by display name or email; neither works. Also search `display_name` and `email_lookup_hash` (for exact-email lookup).
- **`[UX] LOW` — `purge` leaves display_name + username intact.** Purge zeros PII tables but keeps the `User` row's `display_name`. For regulatory right-to-delete that's arguably incomplete. Either anonymize `display_name` to `deleted-<user_id>` or document why it stays.
- **`[CODE] LOW` — Non-clobbering rule hidden in comment.** PII update's "empty field = leave alone" rule is documented in a 6-line comment only; easy for a future edit to break silently. Factor into `_coalesce_pii(old, new)` helper with a unit test.

### 2.3 Permissions / Admin Gate

- **`[SEC] MED` — `_permission_gate` does not enforce role floor.** `app/routers/team_admin.py:_permission_gate` checks `has_permission(resource_key)` but doesn't require admin/manager/reviewer role. If an `employee`-role user is ever granted a resource key via the matrix (e.g. an over-eager admin), they can access admin routes from the public URL. Add `if current.role == "employee": return redirect_to_login()` unless the route explicitly allows it.
- **`[CODE] MED` — Resource key catalog lives in two places.** `app/permissions.py:RESOURCE_KEYS` enumerates keys; routers string-literal them. Any rename breaks silently. Export constants (`PERM_ADMIN_SUPPLY_APPROVE = "admin.supply.approve"`) and have routers import them.
- **`[UX] LOW` — No indicator of "you cannot do X" in UI.** Manager users hit actions (e.g. reveal PII) and get a 403 redirect; the sidebar doesn't hide the link. Hide/disable rather than redirect.
- **`[CODE] LOW` — Admin short-circuit inside `has_permission`.** Fine today; but means `set_permission` can silently record a row that has no effect for admins. Document in the function docstring.

### 2.4 Schedule (admin + employee)

- **`[CODE] HIGH` — `team_admin_schedule.py` is 2,036 lines.** SQL, HTML hinting, holiday lookup, stream-schedule join, and roster management are in one file. Extract (a) shift parsing into `lib/schedule/shifts.py`, (b) roster into its own router, (c) closures into its own router.
- **`[BUG] MED` — `_parse_shift_hours` overnight wrap is untested.** `team_admin_schedule.py` parses `"22:00-03:00"` as a 5 h shift via wrap, but no test covers the case where the shift ends exactly at midnight (`"22:00-00:00"`). Risk: 0 h shift stored. Add unit tests for `22:00-00:00`, `00:00-08:00`, `23:59-00:01`.
- **`[DATA] MED` — No optimistic concurrency on grid save.** `admin_schedule_save` reads then writes without comparing an `updated_at` token. Two managers editing the same week last-write-wins. Add a hidden `grid_version` (max `updated_at`) and reject on mismatch with a flash.
- **`[UX] MED` — Copy-previous-week copies holidays too.** `admin_schedule_generate_from_previous` mirrors shifts regardless of `StoreClosure`. If Memorial Day falls only in week N, week N+1 still gets the shifts from the closed day. Skip closed dates when copying.
- **`[UX] LOW` — Non-shift tokens (`OFF`, `PTO`) are inconsistent.** Parser accepts a short whitelist; users type variants (`off`, `O/T`, `VAC`). Add a legend and case-insensitive matching.
- **`[PERF] LOW` — Grid load issues N+1.** `_grid_context` fetches stream schedule hints one week at a time inside the loop. Batch with a single `select` keyed on the week range.
- **`[UX] LOW` — `<dialog>`→`<div>` migration (commit 7e4a04d) does not trap focus.** The hand-rolled modal does not return focus to the invoking row on close, and `Esc` only closes if the JS handler is bound. Add focus trap + `Esc` handler + inert on background.
- **`[CODE] LOW` — `_us_legal_holidays` is hand-coded per year.** Replace with a `holidays` PyPI dep, or at least pull out into a dict so the next year's entries are obvious.

### 2.5 Hours (`/team/hours`)

- **`[BUG] MED` — Clockify "Not connected" state is templated but no endpoint exists.** `dashboard.html:40-48` shows "Ask an admin to link Clockify." but there's no admin page to do so. Either ship the link page or remove the widget — dead UI breeds learned helplessness.
- **`[UX] LOW` — No self-reported hours path.** If Clockify isn't wired, employees cannot log hours; there's no fallback form. Even a read-only "we'll sync later" notice is better than the blank state.

### 2.6 Supply requests

- **`[SEC] MED` — `admin_supply_*` all key off `admin.supply.approve` for deny + mark-ordered.** `team_admin_supply.py:149-202`. Acceptable shortcut today, but "mark ordered" is a weaker action than "deny"; splitting the key lets you grant warehousing staff the ordered-toggle without approval power.
- **`[PERF] LOW` — Status counts recomputed per page load by scanning all rows.** `team_admin_supply.py:61-63` iterates every `SupplyRequest` to build `counts`. Run a `GROUP BY status` aggregate instead.
- **`[UX] LOW` — No request-detail page.** Admin view is a list; notes on submitted requests truncate. Add a drawer or dedicated detail route.
- **`[DATA] LOW` — `notes` silently truncates at 2000 chars.** `_transition` does `notes[:2000]` with no user warning. Validate in the form.

### 2.7 Policies

- **`[PERF] MED` — Policies ack reads unbounded audit log.** Policy ack state is reconstructed by scanning `AuditLog` rows with `action="policy.acknowledged"` and JSON-parsing `details_json`. Grows forever. Add a `PolicyAcknowledgement` table with a unique `(user_id, policy_key)` or at least an index on `(action, actor_user_id)`.
- **`[BUG] LOW` — Acknowledged-at shows most recent ack, not first.** If a user re-acks after policy version bump, their "signed" date jumps forward. Intentional? Document, or store `first_ack_at` + `latest_ack_at`.

### 2.8 Dashboard

- **`[UX] MED` — Three widgets say "Coming soon."** `dashboard.html:40-57` shows hours, pay, tasks, upcoming-shifts all as placeholders. Employees see a hollow dashboard on login. Either hide unshipped widgets for non-admin roles or ship a simple "last clock-in" surrogate.
- **`[UX] LOW` — "Go to Ops" button visible to reviewer-role.** Reviewers land on `/review`, not `/dashboard`. Either send them to `/review` or hide the button.
- **`[CODE] LOW` — Greeting's `now_hour` defaults to 12.** `dashboard.html:4-5` — `{% set hour = now_hour if now_hour is defined else 12 %}`. Server should always pass it; default is a papered-over bug.

### 2.9 Admin Tools (employees / invites)

- **`[SEC] MED` — Invite list shows full token fingerprint.** `team_admin_invites.py:_invite_status` — audit what's rendered in `invites_list.html`; if raw token prefixes are shown, a manager glancing over a shoulder has a foothold. Render only `invite.id` + `expires_at`.
- **`[BUG] LOW` — `_invite_status` normalizes tz-naive `expires_at` without logging.** SQLite returns naive datetimes; code silently replaces `tzinfo`. Fine, but log once at startup that this migration is happening, or fix the model to always write aware.
- **`[UX] LOW` — Bulk actions missing.** No "resend invite" on list; admin has to drill into each row.
- **`[UX] LOW` — Terminate is a hard delete of session but not of invites.** Pending invites for a terminated user remain valid until TTL. Revoke on terminate.

### 2.10 Mobile (drawer + bottom nav)

- **`[UX] MED` — Bottom nav hides behind iOS safe-area.** Home-indicator overlaps. Add `padding-bottom: env(safe-area-inset-bottom)` on the nav container.
- **`[UX] LOW` — Drawer backdrop click closes, but `Esc` does not.** Parity with the hand-rolled schedule modal.
- **`[UX] LOW` — No active-state on bottom nav.** Current page not highlighted; users lose context.

---

## 3. Prioritized backlog (tickets)

Effort: **S** <½ day, **M** ½–2 days, **L** >2 days. Impact: **low / med / high**.

| # | Title | Sev | Effort | Impact |
|---|---|---|---|---|
| 1 | Read `X-Forwarded-For` in `rate_limit.py` (trust single proxy hop) | `[SEC] HIGH` | S | high |
| 2 | Run `validate_runtime_secrets` whenever portal is enabled (not just `public_host_mode`) | `[SEC] HIGH` | S | high |
| 3 | Default `session_https_only=True`; require explicit opt-out | `[SEC] HIGH` | S | high |
| 4 | Add dummy bcrypt on unknown-username path in `authenticate_user` | `[SEC] HIGH` | S | med |
| 5 | Clamp `hourly_rate_cents` to `[0, 2_000_000]` + reject-on-overflow | `[DATA] MED` | S | med |
| 6 | Auto-hide PII reveal after 30s; explicit "hide" button | `[SEC] HIGH` | S | med |
| 7 | Require reveal permission for PII edits; audit old/new cipher hash | `[SEC] MED` | S | med |
| 8 | Flash warning when invite email is dropped due to clash | `[BUG] MED` | S | med |
| 9 | `PolicyAcknowledgement` table + backfill from AuditLog | `[PERF] MED` | M | high |
| 10 | Optimistic concurrency on `admin_schedule_save` (`grid_version` token) | `[DATA] MED` | M | high |
| 11 | Extract schedule shift-parse into `lib/schedule/shifts.py` + unit tests | `[CODE] HIGH` | M | med |
| 12 | Split `team_admin_schedule.py` (roster, closures, grid) | `[CODE] HIGH` | L | med |
| 13 | Role floor in `_permission_gate` (reject `employee` by default) | `[SEC] MED` | S | high |
| 14 | Constants for resource keys in `permissions.py`; refactor callers | `[CODE] MED` | M | med |
| 15 | Search employees by `display_name` + email lookup hash | `[UX] MED` | S | med |
| 16 | Revoke pending invites on terminate | `[UX] LOW` | S | med |
| 17 | Hide or ship the Clockify widgets on dashboard | `[UX] MED` | S | med |
| 18 | `env(safe-area-inset-bottom)` on mobile bottom nav | `[UX] MED` | S | med |
| 19 | Skip `StoreClosure` dates when copy-previous-week runs | `[UX] MED` | S | med |
| 20 | Remove dead `reveal_field` Query param in `admin_employee_detail` | `[BUG] LOW` | S | low |

---

## 4. Test coverage gaps

Looked at `tests/test_team_portal_*.py`, `tests/test_pii.py`, `tests/test_auth.py`, `tests/test_permissions.py`.

- **Timing oracle on login** — no test asserts that `authenticate_user("unknown", ...)` takes comparable time to `authenticate_user("known", "wrong")`. Add one that bounds the delta.
- **Rate-limit bypass via `X-Forwarded-For`** — no test simulates a reverse proxy. Add one that sends 10 requests with different XFF values and asserts rate-limit still triggers (once fix is in).
- **Concurrent schedule edits** — no test for two sessions saving the same week. Add one that asserts the second writer gets a conflict flash.
- **Overnight shift parsing at midnight** — no test for `"22:00-00:00"`, `"00:00-08:00"`, `"23:59-00:01"`.
- **Admin lockout defense** — no test asserts that rate-limiting an admin's login doesn't permanently brick the only admin. Confirm there's an escape hatch (CLI reset).
- **Purge leaves `display_name`** — no test asserts what remains after purge; add one to pin current behavior or drive the fix.
- **Permissions matrix: employee granted admin key** — no test that an employee-role user with an admin resource key still gets rejected. Drives the `_permission_gate` role-floor fix.
- **Policies ack replay** — no test for what happens when a user acks v1, then v2 is published, then user re-acks. Should `latest_ack_at` advance or both be recorded?
- **Invite email-drop audit** — no test asserts that `account.invite_email_dropped` is emitted and that the UI flashes.
- **CSRF rotation on login** — `rotate_token` is called; no test asserts that the old token is rejected after login.

---

## 5. Appendix: quick wins (< 30 min each)

1. **`dashboard.html:4-5`** — pass `now_hour` from `_nav_context` or a template global so the 12-noon default never fires. *5 min.*
2. **`team_admin_employees.py:603-612`** — wrap `int(form["hourly_rate_cents"])` with `max(0, min(2_000_000, value))` and flash on clamp. *10 min.*
3. **`team_admin_employees.py:admin_employee_detail`** — delete the unused `reveal_field: Optional[str] = Query(...)` parameter. *5 min.*
4. **`config.py`** — flip `session_https_only` default to `True` behind a `DEGEN_DEV_MODE=1` escape hatch. *15 min.*
5. **`auth.py:authenticate_user`** — collapse `"no_such_user"` / `"bad_password"` reason strings to `"auth_failed"` in the audit details. *10 min.*

---

*End of report.*
