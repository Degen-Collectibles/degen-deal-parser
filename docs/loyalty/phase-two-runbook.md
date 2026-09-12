# Phase two — local implementation and release gates

## Connected native dev preview — September 11, 08:56 PDT

This update supersedes the earlier pending-login/secret/Shop-GID and unconfirmed-dev-Pro checkpoints below. Parent completed isolated CLI login and exact **Degen Loyalty Dev** linking; its authorized TOML rewrite to webhook API **2026-07** is preserved (shared Admin reader remains **2026-04**). Authenticated exact-app `app env show` was captured in memory and validated before saving the DEV secret as an owner-only **0600** file; no secret value was emitted or auth store dumped. No manual secret handoff is needed. [Official environment retrieval](https://shopify.dev/docs/apps/launch/deployment/deploy-to-hosting-service)

`store info --store degen-loyalty-dev.myshopify.com --json` verified **`gid://shopify/Shop/81339547671`**, exact domain and **dev** type. Only these selected identity fields were retained; no customer query/export or production call occurred. Parent verified DEV location **`gid://shopify/Location/104761688087`**, active **POS Pro**, POS installed, and iPad/iPhone POS installed. Exact OS/POS versions remain unfilled. [Store metadata command](https://shopify.dev/docs/api/shopify-cli/store/store-info)

The bounded `app dev` session started around **08:54 PDT** after actual-secret/config deny checks passed. CLI reports **App has been installed** and **Ready, watching for changes**; no install/permission prompt was presented or answered. Configured scopes remain **empty**. The extension preview built successfully; no deploy/release command ran. The CLI's APP_UNINSTALLED development probe failed against the intentionally minimal backend, which has no webhook receiver; phase-one processing remains disabled.

Parent device action: open [the DEV Console](https://admin.shopify.com/store/degen-loyalty-dev/apps/75c6c90117c95bd9f683cacaf3ed5c51?dev-console=show), then use its real mobile-preview QR/link on the dev-store iPad/iPhone. The embedded app root intentionally returns 404; use the Dev Console POS preview. Temporary [backend health](https://harvard-finance-concepts-thumb.trycloudflare.com/health) is running. Session **68372** automatically stops by approximately **09:24 PDT / 16:24 UTC**; these URLs are temporary and must be reverified after expiry. No QR/native pass was fabricated. Synthetic customer mapping is empty; no Shopify customer was created. Use only verified synthetic DEV customer IDs for subsequent fixtures.

Actual loopback **and HTTPS tunnel** checks passed: health **200**, missing/invalid bearer **401**, `/demo/token`, `/docs` and `/graphiql` **404**. Focused native tests **15 passed**, 3.58s (`/tmp/loyalty-native-linked-tests.log`), including the corrected linked webhook-version expectation. Earlier broader test evidence remains historical; no new full-suite/full-green claim. Source/production flags stay OFF; only this isolated test process enables POS reads. Demos 8766/8767, original WIP and production remain untouched.

Current handoff: `/tmp/degen-loyalty-native-status.md`. Parent native-device validation is now the next action, not login or secret copying. Stop the dedicated session/tunnel for rollback; review exact dev-only cleanup/active-version restoration separately. No cleanup/uninstall has run, and no production activation follows.

## Earlier setup checkpoints (historical)

Updated September 11, 2026; earlier implementation results remain dated September 10, with new native-setup results explicitly recorded below. Local implementation is approved and complete for review. **Production read access and the all-staff policy both default OFF.** Jeffrey approved all cashiers viewing balances/history through the all-staff approved-app/shop policy, corrections restricted in Ops, and read-only Shopify setup inspection. This settles access policy without granting production access or turning on flags. Jeffrey subsequently approved creation of **Degen Loyalty Dev** and the synthetic **degen-loyalty-dev** store, with no paid plan or production changes, and selected **iPad/iPhone**. Parent completed app/store creation and verified the development-store domain and plan as recorded below. Jeffrey now approved exact-dev-app linking/configuration, isolated synthetic backend preparation, local pinned tooling and a bounded temporary native preview. App installation/scopes require parent review at the actual prompt. No install, tunnel, backend exposure or native-device test has executed. Source/production flags remain OFF; only the isolated native-test process may enable the two read flags.

## What is implemented

`GET /api/loyalty/pos/customers/{customer_id}` verifies a Shopify bearer token with PyJWT **2.13.0**. It requires HS256, the configured app audience, exact `https://{shop}/admin` issuer and `https://{shop}` destination, authenticated-user `sub`, session `sid`, integer `iat`/`nbf`/`exp`, and a positive lifetime of at most 300 seconds. No Ops cookie fallback, individual Ops mapping, extra staff login, Shopify request or accounting write occurs. Both feature flags and exact app/shop configuration are required on every request; schema readiness also gates every read.

This is the configurable all-staff app/shop option: a valid approved app/shop authenticated session can read individual balances across that shop. **PIN identity is neither verified nor an authorization boundary.** The backend cannot attest that the requested customer is selected in POS, and does not claim that it can. The cashier read policy is approved; runtime activation remains a separate launch action. If a future policy requires restricted roles, return to the PRD’s per-user alternatives before implementation. Admin corrections/replay remain in Ops. There are no bulk, search, contact or write endpoints.

The single-statement ledger reader returns the exact balance and bounded history from one database snapshot. Whole points are JSON decimal integer strings, preserving values beyond JavaScript’s safe integer range without fractional point storage. Pages are signed with a separate derived cursor key and bound to app/shop/customer/watermark/count/sum. A late lower-ID commit requires refresh instead of silently changing the prior snapshot. No local account is created for an unknown customer; empty history does not validate Shopify customer existence. Pending/review indicators apply only where phase-one records already have an authoritative customer ID; no contact or evidence-JSON matching is introduced.

The two native entry points use `pos.customer-details.block.render` and `pos.customer-details.action.render`, native customer context, a fresh `getSessionToken()` per read, the relative app backend URL, and Shopify web components. Context generation checks, aborts and subscriptions discard old responses after customer/session/PIN/connectivity changes. Missing token, offline state, timeout and errors clear protected data; they never become a zero balance. The history page shows at most 10 entries at once. The extension does not calculate or post earnings.

## Configuration and bounds

| Setting | Shipped value / effect |
|---|---|
| `LOYALTY_POS_READ_ENABLED` | `false`; independent of receiving/processing/posting |
| `LOYALTY_POS_ALL_STAFF_ENABLED` | `false`; explicit all-staff policy switch |
| `LOYALTY_POS_CLIENT_ID` | empty; exact approved extension app audience |
| `LOYALTY_POS_CLIENT_SECRET` | empty; server-only app client secret, masked in settings/auth representations |
| `LOYALTY_SHOP_DOMAIN`, `LOYALTY_SHOP_ID` | remain unset; verified claim domain selects the ledger shop; mapping to the configured Shop GID must be verified during authorized integration |
| `LOYALTY_POS_SESSION_REQUESTS` | 60/minute/process; configurable 1–600 |
| `LOYALTY_POS_TOTAL_REQUESTS` | 600/minute/process, including invalid tokens/preflights; configurable 1–12,000 |

Limits: canonical positive customer ID at most `Number.MAX_SAFE_INTEGER`; 1–25 history entries; 4 KiB Authorization header, 8 KiB total headers, 2 KiB query, 200-character path; no GET body, duplicate auth/query parameters or unknown query fields. Cursors expire after five minutes. The UI has a ten-second read deadline, including token acquisition, and no persistent token/balance storage. Budgets use bounded in-memory session hashes (maximum 4,096 buckets), a locked fixed one-minute window and no database writes. They reset on process restart and are not a distributed limiter; aggregate ingress limits and expected load remain release gates.

Only `https://cdn.shopify.com` and `https://extensions.shopifycdn.com` receive CORS allow-origin headers; OPTIONS permits GET/Authorization only and returns no customer data. Cookies are not enabled by CORS and do not authorize reads. CORS is not the authentication boundary. HTTPS, synchronized server clocks and a correct app `application_url` are required for native use. Tokens are locally verified; an already-issued token can remain valid until expiry after Shopify-side revocation. Disabling either local flag requires the normal approved configuration rollout and denies reads once effective.

Application errors are sanitized and do not log tokens, customer IDs, SQL parameters or payloads. Before deployment, configure ingress/access logs to omit or redact this endpoint’s customer path and cursor query, suppress Authorization/body logging, and agree access-log retention. Existing upstream log behavior was not inspected or changed. No new retention/disposal scheduler is claimed; point balances remain non-expiring and phase-one retention limitations remain in force.

## Reproduce local preview and checks

From the feature worktree in WSL, with the prepared Python environment and Node 22.23.1/npm 10.9.8:

```bash
python3 scripts/run_loyalty_pos_node.py npm ci --ignore-scripts
python3 scripts/run_loyalty_pos_node.py npm run typecheck
python3 scripts/run_loyalty_pos_node.py npm test
python3 scripts/run_loyalty_pos_node.py npm run build
/tmp/degen-loyalty-test-venv/bin/python scripts/loyalty_pos_demo.py --port 8767
```

Open **http://127.0.0.1:8767/**. This separate synthetic browser preview uses the actual shared component and bearer read router. Choose purchase, refund, pending, review or long history; open **View history**, select **Older activity**, change the simulated PIN context, and toggle offline. It is explicitly labeled **not native Shopify POS**. Its HTML/CSS/custom-element accessibility shims are browser-only; neither native entry point imports them. The preview creates a fresh scratch database/home and ephemeral synthetic signing key, disables dotenv/all providers, blocks outgoing network, checks loopback Host/Origin and suppresses access logs. Its demo token endpoint is never registered in `app.main`. Port 8766 is rejected by this launcher to preserve the existing phase-one walkthrough. Stop only this preview with Ctrl-C; restarting creates new examples.

The Node wrapper uses fresh synthetic HOME/USERPROFILE/LOCALAPPDATA/XDG paths, empty npm user/global config and a temporary cache. Dependencies are local to the extension package, with exact direct versions and a checked-in lockfile. Build outputs/node_modules are ignored. No global installation is required. The prepared WSL Python venv has pinned PyJWT 2.13.0 plus the local API/demo dependencies; the existing Windows app venv already has PyJWT 2.13.0 and the full app dependencies.

To repeat browser verification while the separate preview is running:

```bash
python3 scripts/run_loyalty_pos_node.py node node_modules/@playwright/test/cli.js install chromium
python3 scripts/run_loyalty_pos_node.py node node_modules/@playwright/test/cli.js test
```

The browser binaries use `/tmp/loyalty-pos-browsers`; no real browser profile is opened. Browser requests are restricted to the synthetic loopback origin. Tests regenerate [desktop](verification/phase-two/desktop.png) and [mobile](verification/phase-two/mobile.png) evidence. Both screenshots were opened and inspected: clear balance/refund cards, no horizontal overflow at 390px, no raw GIDs, contact data or token display. The preview is not validation of native rendering, device permissions or live token claims.

Focused Python command with the existing Windows app interpreter, from this worktree’s Windows directory:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' scripts/run_loyalty_tests.py tests/test_loyalty_pos.py tests/test_loyalty_pos_integration.py tests/test_loyalty_config.py tests/test_loyalty_domain.py tests/test_loyalty_reader.py tests/test_loyalty_service.py tests/test_loyalty_lifecycle.py tests/test_loyalty_ops.py tests/test_loyalty_webhook.py -q --tb=short
```

The same isolated harness supports WSL API/PostgreSQL tests. Initialize a fresh disposable PostgreSQL cluster under `/tmp/loyalty-worker-pg-*`, listening **only on its Unix socket** (`listen_addresses=''`), port 55439, role/database `loyalty_test`; never reuse the independent parent TCP server or any real database. Run:

```bash
/tmp/degen-loyalty-test-venv/bin/python scripts/run_loyalty_tests.py --loyalty-pg-socket /tmp/loyalty-worker-pg-REPLACE_WITH_OWN_SCRATCH tests/test_loyalty_pos.py tests/test_loyalty_service.py tests/test_loyalty_lifecycle.py -q --tb=short
```

The supplied socket must be your initialized scratch cluster; the placeholder is deliberately not a live target. Tests create unique schemas, use only synthetic data and test both SQLite and PostgreSQL. Stop only your own disposable cluster afterward. The reproducible test code is in the repository, including concurrent receive/post/fencing regressions and read/post interleaving; the local `/tmp/loyalty_p2_pg_verify.py` controller was used for this run and shut down only its own Unix-socket server.

## Recorded results

| Verification | Result / evidence |
|---|---|
| Focused API/security, actual app middleware and phase-one regressions, existing Windows app venv | **174 passed, 1 skipped**, 16.84s; `/tmp/loyalty-p2-windows-final.log` |
| SQLite + isolated PostgreSQL 16 API/service/lifecycle | **144 passed, 1 skipped**, 35.46s; `/tmp/loyalty-p2-postgres-final.log` |
| Preact component behavior | **12 passed**, 44.62s including jsdom startup; `/tmp/loyalty-pos-components-verified.log` |
| Actual HTTP browser flow and preview-origin denial | **2 passed**, 5.1s; `/tmp/loyalty-pos-browser-verified.log` |
| TypeScript, local esbuild bundles, Python compile | Passed; `/tmp/loyalty-pos-types-complete.log`, `/tmp/loyalty-pos-build-verified.log`, `/tmp/loyalty-pos-compile.log` |

The one skip is the existing PostgreSQL-only numeric-type regression when parameterized for SQLite; it runs in the PostgreSQL variant. Security coverage includes signatures/claims/algorithms, default-off/missing settings, no cookie/PIN fallback, bounded requests/rates, exact large points, schema failure, shop/customer cursor isolation, single-statement concurrent reads, late lower-ID commits, zero accounting writes and CORS preflight without authentication bypass. Component coverage includes reordered responses, unsafe/mismatched customers, missing authorization, held/pending/paused states, pagination, a hung token request and native connectivity loss.

Initial WSL integration collection lacked `bcrypt` in its minimal venv; the actual app check passed in the existing Windows app environment. The first Windows API run exposed a synthetic fixture relying on field-name construction for aliased settings; switching fixtures/demo to the declared aliases fixed compatibility without changing shared Settings behavior. Initial component timeout and CORS acceptance tests failed before the corresponding fixes and now pass.

No new full repository run was performed: the prior completed full run and failed-case rerun are recorded in [release-review.md](release-review.md). The unrelated clean-base Clockify week-bounds fixture still prevents a full-green claim. Tests must pass before a future commit; no commit was requested or made.

## Source verification, parent UI evidence and remaining live gaps

Official current docs and the locked Shopify package confirm the 2026-07 customer-details targets, Preact components, Customer API, Session API authenticated-user/PIN distinction and connectivity signal. Relative fetch resolves to the app `application_url`; backend auth is documented from POS 10.6.0 and extension 2025-07 onward. This is not a compatibility guarantee for every 2026-07 feature on that POS version. [POS reference](https://shopify.dev/docs/api/pos-ui-extensions/latest), [Customer targets](https://shopify.dev/docs/api/pos-ui-extensions/latest/targets/customer-details), [Session API](https://shopify.dev/docs/api/pos-ui-extensions/latest/target-apis/standard-apis/session-api), [ID tokens](https://shopify.dev/docs/apps/build/authentication-authorization/id-tokens), [Server/CORS contract](https://shopify.dev/docs/apps/build/pos/communicate-with-server), [PyJWT verification API](https://pyjwt.readthedocs.io/en/stable/api.html).

The new parent project `shopify/loyalty-dev/shopify.app.toml` includes both extension targets and the isolated backend. **Shopify CLI 4.5.2 configuration/build validation now passes**; CLI generated local UID **`a611fd77-b939-c9be-7353-0235da6422209a2e1e4a`** during build; its dashboard registration/link remains blocked on interactive CLI login. Native POS execution has not occurred. Parent’s read-only UI inspection now establishes the organization, existing app/version, configured placeholder URL/scopes and one live location described below. The dedicated development app/store names and iPad/iPhone device family are now approved; app/store creation and the development-store domain are verified. Shop GID/test location, distribution/credential binding, actual grants/protected-data access, test install permissions, Pro entitlement, exact OS/POS device details and native token behavior remain unresolved. The PRD retains the documented custom-distribution and POS Pro verification gates; no plan purchase is assumed. The POS view calls no Admin API, and the shared phase-one Admin API stays 2026-04 with its independent reader-scope gates.

Before activation: verify enforcement of the approved cashier read/Ops correction policy and authorize runtime enablement; confirm approved app/shop/location bindings (the observed live location is not a dev-test selection) and supply the UTC launch cutoff, named exception owner, retention decisions and spending budget; approve aggregate ingress limits/log minimization; complete the now-approved bounded synthetic dev-store/native-device verification after login/private-input/install prerequisites; then review deployment/install/placement/activation as distinct operations. No values are fabricated. Existing approved whole-point/seven-day/gift-card policies are not reopened. TikTok stays permanently excluded; Shopify online remains excluded at launch and may be added later.

## September 11 bounded setup follow-up

Parent supplied September 11 authenticated read-only UI findings in `degen-loyalty-browser-findings.txt`. That initial read-only inspection reported no settings mutations or intentionally read/retained secrets. It preceded the later expressly approved creation actions below. These observations supplement the earlier worker-only source/documentation inspection; they are not API/export evidence or newly executed tests:

- **Degen Collectibles organization 169353178** lists **Degen Ops** (app ID **346302742529**, one install), active **degen-ops-4** (version **933925158913**, released April 21, 2026 at 20:30 UTC), and separate **Degen Site Singles Reader**. Degen Ops is a Dev Dashboard app, not an entry in the observed legacy custom-app list.
- That Degen Ops version has placeholder `app_url` **`https://example.com/callback`**, embedded true, legacy install flow false and webhook API **2026-04**. Configured scopes are `read_all_orders,read_customers,write_customers,write_inventory,read_inventory,read_orders,write_orders,read_products,write_products`; **`read_returns` is absent**. Effective installed-token grants/protected-data access and existing Ops credential-to-app binding remain unverified. The installed-app UI’s broad Orders / All order history category does not resolve those gaps.
- The initial organization Stores page displayed **no entries**, only **Create dev store**; the later approved store submission is recorded below. The live POS location is **2266 Senter Road**, **`gid://shopify/Location/83278725336`**. Its subscription row showed a dash and settings offered Add POS Pro, so **Pro is unconfirmed**, not definitively Lite or a purchase requirement. The test device family is now **iPad/iPhone**; exact device and OS/POS details remain unanswered. The admin handle `degencollectibles` is not verified canonical shop-domain/Shop-GID evidence.
- Smile.io was listed but not opened or changed; free/off remains user-reported. No deployment, production-schema or native-token verification occurred.

**Approved creation update:** Jeffrey authorized the dedicated **Degen Loyalty Dev** app and synthetic **degen-loyalty-dev** store, with **no paid plan or production changes**. Parent created the app in organization **169353178**, verified app ID **422232031233**, with **zero installs**. Shopify automatically generated initial active **degen-loyalty-dev-1**, version **1125275041793**; parent **never clicked Release**. The default app URL is **`example.com`**, no scopes were added, and no secrets were opened. Parent subsequently supplied OAuth client ID **`75c6c90117c95bd9f683cacaf3ed5c51`** from this DEV app’s install link. The numeric app ID is distinct; an automatically active initial version is not native-test or extension-install evidence.

Parent submitted **Create dev store** for **degen-loyalty-dev**, selecting **Basic plan FEATURE PREVIEW** in the development-store form, **not a paid subscription**. **Generate test data** and **developer preview** were unchecked. Creation is **complete**: parent verified the **dev badge** at **https://admin.shopify.com/store/degen-loyalty-dev**, **degen-loyalty-dev.myshopify.com** listed **Primary / Connected** on Domains, and **Basic Development store** on Plan. No paid subscription was selected, app install performed or secret read. Shop GID and test location remain unverified. Jeffrey selected **iPad/iPhone**; exact device, OS/POS versions and test-user details remain unfilled. **Existing production Degen Ops is not an approved development target:** do not change its URL/scopes/released settings for testing. The approved names, verified app/version IDs, completed development-store/domain evidence and iOS choice are recorded in the [native-test preflight](native-test-preflight.md). The HTTPS test-backend URL and Shop GID/test-location identifiers remain unfilled. Local dev setup/bounded preview is now approved; actual install/scope prompts still require parent review. Cashier balance/history access and restricted Ops corrections are already approved; no policy reapproval is requested.

The preceding parent-evidence integration was documentation-only and did not execute CLI/tests/browser actions. The later authorized local setup below supersedes its earlier uninspected-CLI and not-yet-authorized linking/preview statements. No production access or cashier-policy reapproval is implied.

## September 11 native setup implementation

New standalone launchers `scripts/loyalty_native.py` and `scripts/loyalty_native_web.py` serve **only** minimal `/health` plus the actual bearer-authenticated POS GET/OPTIONS route. They require the exact dev client/domain, a verified supplied Shop GID and the explicitly handed-off DEV secret file (owner-only regular file, 0600, no symlink). The CLI adapter accepts only the documented public client/URL/port context and ignores the CLI-injected secret. No inherited credentials/dotenv/providers, `/demo/token`, Ops/admin/docs, contact queries or Shopify calls enter this process. Wrong/missing app/shop/session authentication denies reads. Source/production flags remain OFF; only this test process enables the two POS read flags. Receiving/processing/posting stay OFF.

Each launch creates a fresh synthetic scratch DB and refuses an existing DB. Optional verified synthetic customer IDs map to earned **20**, cumulative refund **19**, pending **0**, or review **12** point fixtures. This fabricates no live entitlement: all sample ledger/order evidence is labeled synthetic and remains local. After seeding, SQLite reopens in **read-only mode**, enforcing zero endpoint accounting writes. Host/request/session bounds apply, access logs are disabled, backend binds only **127.0.0.1:8768**, and its normal preview lifetime is **30 minutes** (hard maximum 60). The dedicated CLI `dev` wrapper validates exact targets/empty scopes, reruns deny checks without a listener, requires an interactive parent terminal for prompts, then bounds the dev process group to **30 minutes**. Existing 8766/8767 demos are preserved and must never be tunneled.

`shopify/loyalty-dev/` contains the public parent app/web TOML, pinned **@shopify/cli 4.5.2** package and lockfile. Its default app URL is still the observed placeholder `https://example.com`; the reviewed `app dev` configuration can replace it with the temporary dev preview URL only when explicitly run. Extension API remains **2026-07**; shared Admin API remains **2026-04**. Scopes are deliberately **empty**: this ledger UI makes no Admin API calls. Phase-one `read_returns` and other production grants remain separate unresolved gates; no scope or secret was read/changed in Shopify by this worker.

`python3 scripts/run_loyalty_shopify.py link` used the exact DEV client, then stopped at **authorization required; current environment does not support interactive prompts**. No existing auth store was copied/read, no browser opened, no app installed, no UID registered/linked in Shopify and no tunnel started. Parent must complete isolated CLI login and supply the private DEV secret plus verified dev Shop GID. Native install/scopes remain an action-time review. No real native token/QR/URL or device result is claimed.

Current verification (synthetic, isolated environment; no full-suite rerun):

| Check | Exact result / evidence |
|---|---|
| Native backend/adapter + existing POS/domain/service/lifecycle regression | **141 passed, 1 skipped**, 18.97s; `/tmp/loyalty-native-focused-final.log`. Includes subprocess `--check-only`, no cookie/token-mint fallback, app/shop/session denial, expiry, private-secret safety and read-only SQLite. Skip is the PostgreSQL-only schema corruption check on this SQLite run; prior PostgreSQL results above remain historical. |
| Extension component tests | **12 passed**, 62.20s; `/tmp/loyalty-native-components.log` |
| TypeScript and esbuild | Passed; `/tmp/loyalty-native-types.log`, `/tmp/loyalty-native-extension-build.log` |
| Shopify CLI 4.5.2 parent/web/extension build | Passed; `/tmp/loyalty-native-cli-build-final.log` (both targets, no auth/native execution required) |
| Python compile | Passed for the new launchers and native tests |
| Linking | Correct exact-target attempt blocked on interactive login; `/tmp/loyalty-native-cli-link.log`. CLI 4.5.2 rejects simultaneous `--config` and `--client-id`; launcher now uses only the exact client ID. |

No new full-green claim: the previously verified clean-base Clockify fixture failure is unchanged. Local install stayed project-scoped; no global upgrades or paid services. No browser/native tests were repeated in this setup; prior browser/component preview evidence is not native POS evidence. SHA-256 comparison confirms all preexisting application/test/component/demo source files are unchanged. The only preexisting changes are the two authorized native-preflight/runbook docs and the extension TOML: CLI added the local UID, with its comment updated to distinguish generation from unverified dashboard registration. New setup scripts/config/tests are additive. Original WIP is preserved and uncommitted.

For exact parent login, private input, deny-check, bounded preview and iPad/iPhone steps, use the [native-test preflight](native-test-preflight.md#ready-commands-and-parent-handoff). The immediate handoff is **CLI login + private DEV secret + verified dev Shop GID**. Test location, synthetic customer IDs, exact iOS/POS versions, native compatibility/Pro entitlement and actual install/grants remain unverified. No paid plan or production app/store is a fallback.

## Rollback and preservation

Disable either POS read/policy flag through an approved configuration rollout. Remove native placement/app release only through a separately approved operation if needed. No rollback step deletes accounts, expires points or rewrites the ledger. Receiving/processing/posting flags and subscriptions stay independent; Ops corrections/history retain their existing permission matrix.

At the September 10 implementation checkpoint, the pre-phase-two SHA-256 manifest covered 51 existing files. At that checkpoint, only `app/config.py`, `app/main.py`, `requirements.txt` and the three authorized phase-two/release documents had changed from that snapshot. The earlier September 11 evidence follow-ups changed only loyalty documentation; the later native-setup additions and preservation check are described above. Phase-one accounting/reader/schema/router/tests, demo launcher and screenshots, and protected Shopify/TikTok sources remain byte-for-byte preserved. Existing original WIP remains uncommitted. The phase-one demo on 8766 was not stopped, restarted or modified.

For native-test rollback, stop only the dedicated dev/backend process group and temporary tunnel; the wrapper bounds the session, and the backend independently expires. Verify the preview URL stops responding. Review exact dev-target `app dev clean` restoration to initial active version **1125275041793** before executing; do not deploy/release, uninstall production apps, or touch demos. Restore the captured dev URL/config if changed. Secret handoff, isolated CLI session and scratch files need explicit disposal after testing; stopping a tunnel does not remove them. No cleanup command has run because no preview/install started.
