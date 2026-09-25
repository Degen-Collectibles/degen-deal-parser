# PRD: Employee portal redesign (/team, employee side)

Status: questions resolved, waiting for go-ahead · 2026-09-24
Mockup: [employee-mockup.html](employee-mockup.html) (approved 2026-09-24)

## Problem

Employees mostly open the portal on a phone to answer three questions: when do I work next, am I clocked in, and how many hours do I have. The current portal makes that hard:

- The dashboard shows clock status, hours and break about 3 times each (hero, focus list, widgets). "Next shift" is the 4th block on a phone. The "N items to check" count never clears, because every active announcement counts as a to-do.
- The phone bottom bar for hourly staff is Home · Policies · **Supply** · Schedule · Profile. Hours and Time off are hidden in the hamburger menu.
- The Schedule page shows three full-team grids, 760–1000px wide, that you have to scroll sideways.
- Time off and Supply are two separate pages with tables that scroll sideways on phones, and a pending request can't be cancelled.
- Announcements, Notifications and Documents are three reading pages, and Notifications mostly repeats the other two. Nothing has read/unread state.
- Pages carry a lot of one-off styling (804 inline `style=` attributes, 20 `<style>` blocks across `templates/team/**`), so every page looks slightly different.

## Current state

- Templates: `app/templates/team/*.html`, `app/templates/_team_mobile_bottom_nav.html`, `app/static/portal.css`.
- Routes: `app/routers/team.py` (dashboard, hours, schedule, supply, announcements, notifications, documents, policies; nav at about lines 770–841), `app/routers/team_timeoff.py` (GET/POST only).
- Data: `TimeOffRequest` and `SupplyRequest` have a string `status` column (`submitted` / `approved` / `denied`). Employee notifications are `AuditLog` rows (`EMPLOYEE_NOTIFICATION_ACTION`). There is no read-state storage anywhere.
- Clocking happens in Clockify. The portal only reads hours.

## Decisions already made (2026-09-24)

- Same 5 phone tabs for every role: **Home · Schedule · Hours · Requests · More**. Ops tools move under More, shown by role.
- **No** estimated pay on Hours.
- **Add** cancel and edit for pending time-off and supply requests.
- **Merge** Announcements, Notifications and Documents into one **Inbox** with read/unread. Updating the nav tests is OK.

## Success criteria

1. On a 390px phone, Home shows clock status (or the next shift) and pay-period hours without scrolling.
2. No employee page scrolls sideways at 375px (the schedule's team view included).
3. Hours and Requests are each one tap from any page on a phone.
4. An employee can cancel or edit a pending request. Managers see the change in their queue, and approved or denied requests can't be edited.
5. The Inbox unread count goes to 0 once items are read, and the Home "Needs you" list is empty when there's nothing to do.
6. Employee templates touched by this work have no inline `style=` and no page-level `<style>` blocks; styling lives in `portal.css` components.
7. The full test suite passes. Tests that pin old markup are updated on purpose, not deleted.

## Scope

**Phase 0: shared component kit** (`portal.css`)
- Type and spacing tokens, and components for card, row list, pill, button (primary/ghost/sm, 44px minimum tap target), section header, callout, empty state, sheet (bottom-sheet form) and week strip.
- Delete the dead `.tp-*` aliases in `team/base.html`.
- Fix the `linear.css` `body` `!important` override that stops the portal's font from applying (scoped to `body.pt-body`).

**Phase 1: navigation and Home**
- New bottom nav (5 tabs, same for every role). The desktop sidebar is regrouped to match: Home, Schedule, Hours, Requests, Inbox, then an "Ops" group by role, then Profile/Help.
- Home redesign: status hero (3 states), pay-period and this-week tiles, "Needs you" (real to-dos only), week strip, my requests, latest unread announcement.
- Drop the "Signed in" chip and the live clock from the page header.

**Phase 2: Schedule and Hours**
- Schedule: "My shifts" list by default, and "Whole team" as a day-by-day list. Same data (`entry_map`), no grid on phones. The desktop keeps the grid as an option under "Whole team".
- Hours: pay-period framing, a small worked-vs-scheduled bar chart (plain CSS, no chart library), a list of days with flags, and a Clockify explainer.

**Phase 3: Requests**
- New `/team/requests` page combining time off and supplies. It uses a sheet form with date checks done in the browser and a warning about scheduled shifts you'd miss.
- New routes (CSRF-protected, owner-only, only while `status == "submitted"`):
  - `POST /team/timeoff/{id}/cancel`, `POST /team/timeoff/{id}/edit`
  - `POST /team/supply/{id}/cancel`, `POST /team/supply/{id}/edit`
- Cancel sets `status = "cancelled"`, keeping the row for audit, and writes an AuditLog entry. Manager queues hide cancelled requests by default.
- `/team/timeoff` and `/team/supply` GET become redirects to `/team/requests?tab=…` so old links and bookmarks still work.

**Phase 4: Inbox**
- New `/team/inbox` that combines announcements, employee notifications and documents, with read/unread.
- A new **table** `team_inbox_read` (`user_id`, `item_kind`, `item_id`, `read_at`) with a unique constraint on (user_id, item_kind, item_id). It's a new table, so `create_all` creates it on both SQLite and Postgres and no ALTER migration is needed.
- `/team/announcements`, `/team/notifications` and `/team/documents` redirect to the matching Inbox filter.
- Announcements no longer count as to-dos on Home. The notification poll keeps its current contract.

## Non-scope

- The manager/admin side (a separate PRD follows). Admin bug fixes are on their own branch, `fix/team-portal-admin-bugs`.
- Buylist and onboarding (`invite_accept`) rewrites. They get the shared components later.
- Native clock-in (Clockify stays the only source).
- Light mode, and estimated pay.

## Constraints

- Keep all existing routes working (redirects are fine). The PWA scope stays `/team/`.
- CSRF: every new POST uses `Depends(require_csrf)` and the hidden `csrf_token` field.
- Keep the DOM hooks that `portal-drawer.js` depends on (`pt-sidebar`, `pt-hamburger`, `pt-drawer-*`). Mobile-nav tests pin `pt-mobile-bottom-nav`, `pt-mbn-fab`, `pt-mbn-item-center` and attribute order. There's no center FAB any more, so `test_team_portal_mobile_nav.py` gets updated to the new structure.
- Permissions: every tab and page keeps the same `page.*` resource checks. A tab is hidden (not a 403) if the user lacks the permission.
- No new JS framework. Plain JavaScript in `/static/`, not inline.
- No schema changes to existing tables.

## Plan

Each phase ships as its own PR off `main`, reviewed and green before the next phase starts. Phase 0 and Phase 1 can be one PR if the diff stays reviewable.

## Risks

| Risk | Mitigation |
|---|---|
| Nav change confuses staff who are used to the old layout | Same 5 tabs for everyone; update the employee tutorial (`_employee_portal_tutorial.html`) in Phase 1; send a short announcement at rollout |
| Markup tests break widely | Update tests in the same PR; list every changed assertion in the PR description |
| The `linear.css` override fix changes the look of the main app | Scope the fix to `body.pt-body`; check a few main-app pages before and after |
| Cancel/edit racing a manager's approval | Owner check plus `status == "submitted"` check in the same transaction; if it's already decided, show "already decided" |
| Conflicts with the admin bug-fix branch in `team.py` nav | Merge the bug-fix branch first |

## Verification

- `python -m compileall app`, focused tests for each phase, then the full suite (per `AGENTS.md`).
- New tests: cancel and edit (owner-only, submitted-only, CSRF, audit row); inbox read-state (unread count, mark read, idempotent); redirects from old URLs; bottom nav renders 5 tabs for each role.
- Manual check in the browser at 375px and 1280px for each changed page, plus a real phone check of the installed app (safe areas, bottom nav).

## Rollback

- Each phase is its own PR, so rolling back means reverting that PR.
- Phase 4's `team_inbox_read` table is additive; leaving it in place after a revert is harmless.
- Cancelled requests keep their rows. After a revert they show as status `cancelled` in the old UI, which is readable, so no data cleanup is needed.

## Resolved questions (2026-09-24)

1. **Editing a pending request notifies managers**, using the same alert path as a new request. Editing or cancelling an approved or denied request isn't allowed; the employee cancels and resubmits instead.
2. **New documents count as unread** in the Inbox, along with announcements and notifications.
3. **The desktop sidebar gets the same grouping** as the phone tabs (Claude's recommendation; Jeffrey had no preference). One layout means one set of training, and staff switch between the shop iPad and their phones.
