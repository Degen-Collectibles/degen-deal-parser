# PRD: Productize the TikTok Creator-Attribution Pilot

- **Date:** 2026-06-10
- **Author:** Jeffrey + Claude (audit follow-up)
- **Executor:** Codex
- **Status:** Approved direction pending these baseline corrections — observability/productization phase only
- **Related:** `TIKTOK_API.md` ("Search Creator Affiliate Orders", "Search Seller Affiliate Orders"), commits `f1a84f9`, `4eccdec`, `ab4e23d`, `b20ffb1`, `9675bc1`

## 1. Problem

DC LLC runs one TikTok shop with two in-house creators (`@degencollectibles`, `@degenboss0`). TikTok records no creator on orders from in-house lives, so the streamer dashboard can only split orders per creator with labeled heuristics (time window, live-product pools) and pauses overlapping feeds entirely.

The authoritative fix — TikTok affiliate attribution — has a complete data pipeline in the codebase, but it is **invisible and unverifiable from the app**. Nobody can tell from the UI whether creator tokens exist, whether they carry the required scope, whether the trace backfill ran, or why zero orders are attributed. Today the only way to check is SSH'ing into Green and running ad-hoc SQL probes. That is the gap between "pilot code exists" and "pilot is operable."

## 2. Current state (verified 2026-06-10, all code on `main` at `9675bc1`)

Note on baseline: `9675bc1` ("Harden TikTok streamer attribution and reset links") additionally shipped the `PUBLIC_BASE_URL` fail-closed reset-link fix and wired `creator_order_rows_precise` into the streamer page/poll context. Chunked-suite result at this commit: **1511 passed, 5 subtests passed**.

What already exists and **must not be rebuilt**:

| Piece | Location | Status |
|---|---|---|
| Creator auth storage | `TikTokCreatorAuth` model; `tiktok_creator_auth` table (both engine migration dicts in `app/db.py`) | Done |
| Creator OAuth flows | `GET /integrations/tiktok/oauth/creator-shop-start` (`app/routers/shopify.py:186`, shop-service flow, admin-only), `GET /integrations/tiktok/oauth/creator-start` (`shopify.py:218`), callback at `shopify.py:527` | Done |
| Creator token auto-refresh | `app/tiktok/tiktok_auth_refresh.py` (~line 142+, serialized with shop refresh) | Done |
| Creator trace pull + join | `backfill_tiktok_creator_affiliate_attributions` (`scripts/tiktok_backfill.py:2313`) — walks `TikTokCreatorAuth` rows, requires `creator.affiliate_collaboration.read`, pages `/affiliate_creator/202410/orders/search`, upserts `affiliate_creator_username` onto `TikTokOrder` | Done |
| Periodic wiring | `run_tiktok_pull_cycle` (`app/shared.py:3384`) passes `affiliate_attribution=tiktok_affiliate_order_scope_authorized(session)`; the gate (`app/shared.py:3062`) accepts EITHER seller-scope on `TikTokAuth` OR creator-scope on any `TikTokCreatorAuth` row | Done |
| Dashboard consumption | `_filter_orders_to_affiliate_creator` flips to authoritative `AFFILIATE_ORDERS` mode automatically once `_has_affiliate_creator_orders` sees attributed rows (`app/routers/tiktok_streamer.py`) | Done, regression-tested |
| Pull-state plumbing | `update_tiktok_integration_state(...)` already records `affiliate_order_scope_authorized`, `affiliate_scope_missing`, and per-pull `affiliate_attributed/missing/failed` counts | Done (not surfaced) |

What is missing (this PRD):

1. **No status surface.** Creator-auth health (token present? scope present? expiry? last refresh?) and trace-pull outcomes (last run, attributed/missing/failed counts, scope-missing flag) are only visible via prod shell probes (`outputs/tiktok_prod_creatorauth.sh`) and structured log lines.
2. **No per-creator persistence of trace-pull outcomes.** Pull summaries are aggregated across creators and held in integration state; you cannot answer "when did degenboss0's traces last sync and how many orders matched?"
3. **No operator runbook in-app or in-docs** for the one-time setup chain (scope request → reauth → affiliate plan → creator authorization).
4. **No alerting** when a creator token is expired/expiring or when trace pulls start failing — silent decay back to heuristics.

## 3. Success criteria

1. An admin can answer, from a single authenticated page, with zero shell access: which creators are connected, scope per token, access/refresh expiry, last refresh time, last trace-pull time/result per creator, and count of attributed orders in the last 7 days per creator.
2. When the pilot's operational prerequisites complete (see §7 step 0), attributed orders appear on the dashboard with **no further code changes** — the page in (1) shows nonzero attributed counts and the streamer dashboard shows "Filtering by TikTok creator order attribution."
3. A failed or scope-blocked trace pull is visible within one poll cycle on the status page and in `/status.json` (no silent decay).
4. Full test suite passes (baseline: 1511 passed, 5 subtests at `9675bc1`); no changes to webhook signature logic, token-exchange logic, or the attribution filter semantics shipped in `b20ffb1`/`9675bc1`.

**Graduation gate (explicitly out of THIS phase):** this PRD ships observability only — no payout changes, no dashboard attribution-semantics changes, no manual split logic. After the panel shows real attributed orders across a full live weekend, Jeffrey decides whether the affiliate trace data is trustworthy enough to graduate to behavior changes; that graduation is a separate PRD.

## 4. Scope

- **A. Creator attribution status panel** (**admin-only** in this iteration — it lives on `/tiktok/streamer/config`, which is already admin-gated at `app/routers/tiktok_streamer.py:3182`; the panel exposes integration health, OAuth links, scopes, and failure detail, which is ops material, not floor-operator material). Read-only rows per `TikTokCreatorAuth`: handle, has `creator.affiliate_collaboration.read` (parse `scopes_json` defensively — never render the raw JSON), access/refresh expiry with expired/expiring-soon badges, `updated_at`; plus shop-token row: has `seller.affiliate_collaboration.read`. Action buttons that link to the two existing OAuth start routes (no new auth logic). Show last trace-pull status per creator (including truncated last-error detail — admin panel is the ONLY place error strings render) and 7-day attributed-order counts: filter on `TikTokOrder.created_at >= now-7d` and `TikTokOrder.affiliate_creator_username == <handle>`, where the handle is normalized the same way the writers normalize it (`_clean_affiliate_creator_username` / `_normalize_creator`: lowercase, no `@`).
- **B. Per-creator trace-pull status persistence** — the current `backfill_tiktok_creator_affiliate_attributions` returns a single aggregate `TikTokPullSummary`, so the caller cannot reconstruct per-creator outcomes. Make per-creator telemetry explicit inside that function: either (preferred) have it additionally return a per-creator mapping (`dict[creator_handle, per-creator summary]`) alongside the aggregate, or accept an optional `telemetry_callback(creator_handle, summary)` invoked at the end of each creator's loop iteration. Keep the change backward-compatible for existing callers/tests and do not touch request signing. The pull path then records, per creator: timestamp, attributed/missing/failed counts, scope_missing flag, last error (truncated). **Storage: one `AppSetting` key (e.g. `tiktok_creator_trace_status`) holding a JSON object keyed by normalized creator handle. Do NOT add table columns** — avoids dual-engine migration risk for operational telemetry.
- **C. `/status.json` extension** — note `/status.json` is **viewer-gated** (`app/routers/dashboard.py:343`), not admin-gated, so its content must be strictly coarse: per-creator entries limited to `{handle, scope_ok, access_expired, refresh_expired, last_trace_pull_at, last_trace_attributed_count}` plus the global `affiliate_order_scope_authorized`. **No token-adjacent data, no raw payloads, no `scopes_json` dump, and no error strings** — error detail is admin-panel-only (§4A).
- **D. Expiry/failure visibility** — reuse the existing ops-log/structured-log conventions: emit a structured warning when a creator refresh token is within 7 days of expiry or a trace pull fails; surface the same condition as a red badge in panel (A). (External alerting/SMS is non-scope.)
- **E. Operator runbook** — `docs/ops/tiktok-creator-attribution-runbook.md`: the one-time setup chain with exact URLs/routes, the verification steps using panel (A), and the rollback posture. Update `TIKTOK_API.md` cross-reference and add panel/status keys to `AGENTS.md`.

## 5. Non-scope

- Any change to `_filter_orders_to_affiliate_creator`, `_vote`/window/product-split semantics, or attribution messages (just hardened in `b20ffb1`; regression-tested).
- Any change to webhook signature verification, OAuth token exchange, or token refresh logic (protected paths).
- The Seller Center affiliate plan itself (operational, done by Jeffrey in TikTok UI — see §7 step 0).
- Requesting/altering TikTok app scopes in the developer console (operational).
- New DB columns or tables (use `AppSetting` JSON per §4B).
- External alert channels (Twilio/Discord) for token expiry — log + UI badge only in this iteration.
- Per-creator payroll/performance reporting (separate feature).
- Encrypting tokens at rest (separate change, needs its own approval).

## 6. Constraints

- **Protected paths:** do not refactor `app/tiktok/tiktok_ingest.py` signing logic, `app/tiktok/tiktok_auth_refresh.py` refresh flow, or `scripts/tiktok_backfill.py` request signing. Panel/status code reads DB rows and `AppSetting`; the only write into the pull path is the per-creator summary recording in §4B, which must be additive (wrap in try/except so a telemetry failure can never fail the pull).
- **Never render or log token values.** Handles, scopes, expiry timestamps, counts, and truncated error strings only. Treat `scopes_json` parsing defensively (it is raw TikTok payload).
- Dual-engine: no schema changes expected; if any become unavoidable, follow the `app/db.py` checklist for BOTH `SQLITE_ADDITIVE_MIGRATIONS` and `POSTGRES_ADDITIVE_MIGRATIONS`.
- Roles: panel is **admin-only** (inherits the existing gate on `/tiktok/streamer/config`); the OAuth start routes are already admin-gated server-side. `/status.json` additions must respect its existing viewer gate by carrying only coarse data (§4C).
- All new routes go through existing routers (`app/routers/tiktok_streamer.py` config page context or `app/routers/admin.py`) — no new parallel dashboards (CLAUDE.md).
- Tests patch at `app.routers.<module>.X` / `app.shared.X` (AGENTS.md patch-target rule).

## 7. Plan

**Step 0 — Operational prerequisites (Jeffrey, not Codex; panel must be useful even while these are pending):**
1. Run `outputs/tiktok_prod_creatorauth.sh` on Green (read-only) to baseline current creator-auth state.
2. TikTok developer console: confirm/request `seller.affiliate_collaboration.read` and `creator.affiliate_collaboration.read` on the app; reauthorize the shop if scope set changed.
3. Seller Center: create a Targeted Collaboration plan for `@degenboss0` at minimum commission (~1%) covering streamed products. **Blocked on policy verification — see Open Question 1.**
4. Each creator completes authorization via `/integrations/tiktok/oauth/creator-shop-start?creator=<handle>` (for example, `/integrations/tiktok/oauth/creator-shop-start?creator=degenboss0`). **This must be a supervised browser session**: the route is Degen-admin-gated, but the TikTok session in that browser determines WHICH TikTok account gets authorized. Procedure: an admin logs into the Degen app, and in the same browser the creator logs into their own TikTok account (verify the handle in TikTok's UI before consenting); use a separate browser profile/incognito window per creator so a lingering TikTok session can't silently authorize the wrong account. The runbook (§4E) must spell this out step-by-step, including how to verify the resulting `TikTokCreatorAuth` row shows the expected handle on the status panel before moving to the next creator.

**Step 1 — Status persistence (§4B):** add per-creator summary recording from `backfill_tiktok_creator_affiliate_attributions`, then write the `AppSetting` JSON from the pull path. Pure addition; telemetry failures swallowed with a structured log line.

**Step 2 — Status panel (§4A) + `/status.json` block (§4C):** template section + context builder + JSON keys. Read-only queries; no caching needed (admin page).

**Step 3 — Expiry/failure badges + structured warnings (§4D).**

**Step 4 — Runbook + doc sync (§4E).**

**Step 5 — Verification (§9), then commit in small reviewable units** (status persistence / panel / docs), full suite before each commit, push through `origin/main` per the normal Green deploy path. Stage only intended files; the worktree may contain unrelated work.

## 8. Risks

1. **TikTok may not return traces for in-house lives even with scope + affiliate plan.** Mitigation: that is exactly what panel (A) makes observable; the dashboard keeps its honest heuristics as fallback, so user-visible behavior never regresses. The pilot's go/no-go becomes a data question readable off the panel.
2. **Telemetry write breaking the pull cycle.** Mitigation: §6 — try/except around the `AppSetting` write, never raise into the pull.
3. **Token leakage via new surfaces.** Mitigation: §6 rule (no token values), code review checklist item, and a test asserting the panel/status payload contains no `access_token`/`refresh_token` substrings.
4. **`scopes_json` shape drift** (raw TikTok payload). Mitigation: defensive parsing with the existing `_scope_json_contains` helper; unknown shape renders as "scope unknown", not a crash.
5. **Concurrent `AppSetting` JSON writes** (pull cycle vs. manual trigger). Mitigation: single writer is the pull path; read-modify-write inside the pull's session/commit; acceptable last-writer-wins for telemetry.

## 9. Verification

- `./.venv/Scripts/python.exe -m compileall app`
- Focused: new tests below + `tests/test_tiktok_reporting.py`, `tests/test_tiktok_streamer_surprise_sets.py`, `tests/test_tiktok_token_refresh.py`
- Full suite before every commit (`./.venv/Scripts/python.exe -m pytest --tb=short -q`); baseline is all-pass as of `9675bc1` (chunked-suite: 1511 passed, 5 subtests passed).
- **New tests (minimum):**
  1. Trace backfill records per-creator summary into `AppSetting` (success, scope-missing, and failure paths).
  2. Telemetry write failure does not fail the pull (patch `AppSetting` write to raise → pull summary still returned).
  3. Status panel context: creator row with scope → green; without scope → scope-missing badge; expired refresh token → expired badge.
  4. `/status.json` includes the `tiktok_creator_attribution` block and respects the viewer-gate content rules: assert the serialized payload excludes the actual token strings seeded in the fixture, excludes the raw `scopes_json` text, and excludes seeded error strings (errors must appear only in the admin panel context).
  5. Per-creator telemetry mechanism (§4B): two creators in the fixture, one succeeding and one scope-blocked → the recorded `AppSetting` JSON distinguishes them (e.g., degencollectibles attributed=8, degenboss0 scope_missing=true), and the aggregate `TikTokPullSummary` is unchanged for existing callers.
  6. Panel renders with zero `TikTokCreatorAuth` rows (empty state pointing at the runbook).
- **Manual (Green, after deploy):** load the panel; confirm parity with `outputs/tiktok_prod_creatorauth.sh` output; confirm `/status.json` block via authenticated fetch.

## 10. Rollback

- All changes are additive (one `AppSetting` key, read-only panel section, status keys, docs). Rollback = revert the commit(s); no migration to unwind, no data mutation to repair.
- The `AppSetting` row is inert if orphaned; optionally delete `tiktok_creator_trace_status` after revert.
- The pilot's operational pieces (scopes, affiliate plan) are independent of this code and can be unwound in TikTok's UIs without touching the app.

## 11. Open questions

1. **Policy (blocking step 0.3, not the code):** Does TikTok permit a Targeted Collaboration plan with a creator tied to the same business entity, and does `@degenboss0` meet affiliate eligibility (follower threshold)? Verify in Seller Center / TikTok Shop Academy before creating the plan. If blocked, the panel still has full value for the creator-side trace path (step 0.4), which has no commission component.
2. **Panel placement: RESOLVED** — `/tiktok/streamer/config`, admin-only (the page's existing gate). Revisit reviewer visibility only after the pilot graduates and the panel's content stabilizes.
3. Trace-pull lookback: the periodic cycle's existing window is assumed sufficient to cover a full live session retroactively. Confirm the current pull `since` window ≥ 24h; if shorter, extend the trace-backfill `since` (only for the trace call) to 48h.
4. Should the panel expose a manual "pull traces now" button (admin-only, calls `run_tiktok_pull_in_background`)? Nice-to-have; include only if trivially wired to the existing background trigger.
