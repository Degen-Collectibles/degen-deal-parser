# TikTok Creator Attribution Runbook

Use this runbook to set up and verify the creator-attribution pilot for the shared DC LLC TikTok Shop. This is observability-only: do not change dashboard attribution rules, payout logic, or manual split behavior from this process.

## Safety Rules

- Use only official TikTok Shop/Creator authorization surfaces.
- Do not expose or copy access tokens, refresh tokens, raw OAuth payloads, or `scopes_json`.
- Shared-shop orders without a reliable creator trace stay unknown/paused. Do not manually assign them from guesswork.
- Production checks are read-only unless Jeffrey explicitly approves a write action.

## App Surfaces

- Admin panel: `/tiktok/streamer/config`, section `Creator Attribution`
- Coarse status JSON: `/status.json`, key `tiktok_creator_attribution`
- Creator auth route: `/integrations/tiktok/oauth/creator-shop-start?creator=<handle>`
- Trace telemetry storage: `AppSetting` key `tiktok_creator_trace_status`

## One-Time Setup

1. Confirm TikTok app scopes in the developer console:
   - `seller.affiliate_collaboration.read`
   - `creator.affiliate_collaboration.read`
2. Reauthorize the shop if seller scopes changed.
3. Verify whether TikTok permits a Targeted Collaboration plan for the creator/business relationship before creating one.
4. If policy allows it, create the targeted plan for the creator at the minimum intended commission and include streamed products.

## Creator Authorization

Use a separate browser profile or incognito session for each creator.

1. Log into the Degen app as an admin.
2. In the same browser profile, log into TikTok as the creator account you are about to authorize.
3. Verify the TikTok UI shows the expected handle before consenting.
4. Open the matching route:
   - `/integrations/tiktok/oauth/creator-shop-start?creator=degencollectibles`
   - `/integrations/tiktok/oauth/creator-shop-start?creator=degenboss0`
5. Complete TikTok consent.
6. Return to `/tiktok/streamer/config` and verify the `Creator Attribution` panel shows the expected handle before authorizing the next creator.

## Verification

From `/tiktok/streamer/config`, confirm:

- Each creator row is present.
- `scope ok` appears for `creator.affiliate_collaboration.read`.
- Refresh token is not expired or expiring soon.
- Last trace pull updates after a TikTok order pull cycle.
- Last trace attributed/missing/failed counts are visible per creator.
- Attributed orders in the last 7 days match the expected creator rows.

From `/status.json`, confirm:

- `tiktok_creator_attribution.affiliate_order_scope_authorized` is true once any seller or creator affiliate scope is valid.
- Creator rows include only handles, booleans, timestamps, and counts.
- No token strings, raw payloads, raw scopes JSON, or error strings are present.

## Troubleshooting

- `scope missing`: recheck TikTok app scopes and reauthorize the affected account.
- `refresh expired`: rerun the creator authorization flow for that handle.
- `trace failed`: inspect the admin panel error, then check app logs for `tiktok.creator_affiliate_orders.failed`.
- Zero attributed rows after a full live weekend: treat the affiliate trace pilot as unproven. Keep dashboard fallback/paused behavior unchanged.

## Rollback

Code rollback is a normal revert. No schema migration is involved.

The `tiktok_creator_trace_status` row is inert if orphaned. It can be deleted after rollback if desired.

Operational rollback happens in TikTok Seller Center/developer console: remove or disable collaboration plans/scopes there.
