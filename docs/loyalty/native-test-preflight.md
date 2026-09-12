# Native POS test preflight — connected dev preview; parent device validation ready

## Connected native dev preview — September 11, 08:56 PDT

This update supersedes the earlier pending-login/secret/Shop-GID and unconfirmed-dev-Pro checkpoints below. Parent completed isolated CLI login and exact **Degen Loyalty Dev** linking; its authorized TOML rewrite to webhook API **2026-07** is preserved (shared Admin reader remains **2026-04**). Authenticated exact-app `app env show` was captured in memory and validated before saving the DEV secret as an owner-only **0600** file; no secret value was emitted or auth store dumped. No manual secret handoff is needed. [Official environment retrieval](https://shopify.dev/docs/apps/launch/deployment/deploy-to-hosting-service)

`store info --store degen-loyalty-dev.myshopify.com --json` verified **`gid://shopify/Shop/81339547671`**, exact domain and **dev** type. Only these selected identity fields were retained; no customer query/export or production call occurred. Parent verified DEV location **`gid://shopify/Location/104761688087`**, active **POS Pro**, POS installed, and iPad/iPhone POS installed. Exact OS/POS versions remain unfilled. [Store metadata command](https://shopify.dev/docs/api/shopify-cli/store/store-info)

The bounded `app dev` session started around **08:54 PDT** after actual-secret/config deny checks passed. CLI reports **App has been installed** and **Ready, watching for changes**; no install/permission prompt was presented or answered. Configured scopes remain **empty**. The extension preview built successfully; no deploy/release command ran. The CLI's APP_UNINSTALLED development probe failed against the intentionally minimal backend, which has no webhook receiver; phase-one processing remains disabled.

Parent device action: open [the DEV Console](https://admin.shopify.com/store/degen-loyalty-dev/apps/75c6c90117c95bd9f683cacaf3ed5c51?dev-console=show), then use its real mobile-preview QR/link on the dev-store iPad/iPhone. The embedded app root intentionally returns 404; use the Dev Console POS preview. Temporary [backend health](https://harvard-finance-concepts-thumb.trycloudflare.com/health) is running. Session **68372** automatically stops by approximately **09:24 PDT / 16:24 UTC**; these URLs are temporary and must be reverified after expiry. No QR/native pass was fabricated. Synthetic customer mapping is empty; no Shopify customer was created. Use only verified synthetic DEV customer IDs for subsequent fixtures.

Actual loopback **and HTTPS tunnel** checks passed: health **200**, missing/invalid bearer **401**, `/demo/token`, `/docs` and `/graphiql` **404**. Focused native tests **15 passed**, 3.58s (`/tmp/loyalty-native-linked-tests.log`), including the corrected linked webhook-version expectation. Earlier broader test evidence remains historical; no new full-suite/full-green claim. Source/production flags stay OFF; only this isolated test process enables POS reads. Demos 8766/8767, original WIP and production remain untouched.

Current handoff: `/tmp/degen-loyalty-native-status.md`. Parent native-device validation is now the next action, not login or secret copying. Stop the dedicated session/tunnel for rollback; review exact dev-only cleanup/active-version restoration separately. No cleanup/uninstall has run, and no production activation follows.

## Earlier setup checkpoints (historical)

September 11, 2026. Jeffrey approved **all cashiers viewing shop-scoped balances/history**, corrections restricted to existing authorized Ops controls, and **read-only Shopify setup inspection**. The implemented all-staff app/shop authenticated-session policy requires no individual Ops binding or extra login. PIN identity is not verified or an authorization boundary. These decisions are settled. Jeffrey additionally approved creation of **Degen Loyalty Dev** and the synthetic **degen-loyalty-dev** store, with **no paid plan or production changes**, and selected **iPad/iPhone** for testing. Jeffrey now also approved local linking/configuration, isolated synthetic backend preparation, local pinned tooling, and a bounded temporary native preview for these exact dev targets. Production/source flags remain OFF; only the isolated native-test process may enable the two POS read flags. App installation/scopes still require parent review at the actual browser prompt. No tunnel, installation or native-device test has run.

Parent supplied authenticated, read-only Shopify UI evidence on September 11 in `degen-loyalty-browser-findings.txt`; the observations below come from that evidence, not this worker’s browser or API access. That initial read-only inspection reported no settings changes and no intentional secret reads or retained secrets. Parent subsequently performed the expressly approved creation actions recorded below. That earlier documentation-only follow-up made no CLI or test calls. The later authorized setup work below ran local CLI/build/security checks and attempted exact-dev-app linking; it did not control a browser, read credentials, install the app or access shop/customer APIs.

## Read-only local findings

- Existing package: `extensions/loyalty-pos/`, lockfile and both customer-details entry points present; extension API pinned to 2026-07, shared Admin API unchanged at 2026-04.
- Parent project **`shopify/loyalty-dev/shopify.app.toml`** now pins the exact DEV client/domain, includes the existing two-target extension and bearer-only web process, and requests **no Admin API scopes**. Shopify CLI builds it successfully. CLI generated local UID **`a611fd77-b939-c9be-7353-0235da6422209a2e1e4a`**; dashboard registration/link remains pending login; a build is not installation/native validation.
- WSL Node **22.23.1**, Git **2.43.0**, and Shopify CLI **4.5.2** are locally verified. The existing CLI ran under WSL; an isolated project-local **4.5.2** installation and lockfile now make the setup reproducible. CLI HOME is private under `/tmp/degen-loyalty-native-cli`; no existing auth store was copied or inspected. Exact-app linking stopped because that isolated profile needs interactive login.
- Source confirms both POS flags default `false`. No environment, secret files, runtime settings or real databases were read. Existing loopback demos on 8766/8767 were not changed or controlled.

## Parent-observed setup and remaining uncertainty

- **Organization:** Degen Collectibles, dashboard organization **169353178**. Its Apps listing shows **Degen Ops**, app ID **346302742529**, one install, active version **degen-ops-4**, and a separate **Degen Site Singles Reader** app. Degen Ops is in Dev Dashboard; the legacy custom-app list instead shows SortSwift and SortSwift Pricing. The observed numeric app ID is not a verified OAuth client ID or proof of the existing Ops credential-to-app binding.
- **Released production app:** Degen Ops version **933925158913** is Active, released **April 21, 2026 at 20:30 UTC**. Its configured `app_url` is the placeholder **`https://example.com/callback`**, embedded is true, legacy install flow is false, and webhook API version is **2026-04**. This URL is not an approved test backend. **Degen Ops is not an approved development target; do not change its URL, scopes, released configuration or installation for this test.**
- **Configured scopes:** `read_all_orders,read_customers,write_customers,write_inventory,read_inventory,read_orders,write_orders,read_products,write_products`. **`read_returns` is absent from this version configuration.** Actual installed-token grants, protected-data access and credential binding remain unverified; do not equate configuration with effective grants. Installed-app UI reports Degen Ops by Degen Collectibles, installed April 12, with Orders / All order history and activity “Yesterday”; that broad category does not verify precise scopes. The POS read endpoint adds no Admin API reads; the phase-one Return reader’s scope gate remains unresolved.
- **Development stores:** the initial organization Stores page displayed **no store entries**, only **Create dev store**. Parent completed the approved creation of **degen-loyalty-dev**. Its admin at **https://admin.shopify.com/store/degen-loyalty-dev** displays a **dev badge**; the Domains page lists **degen-loyalty-dev.myshopify.com** as **Primary / Connected**, and the Plan page explicitly shows **Basic Development store**. Shop GID and test location remain unverified. **Basic plan FEATURE PREVIEW** was selected in the development-store form, not a paid subscription. **Generate test data** and **developer preview** were both unchecked.
- **Observed live location:** **2266 Senter Road**, **`gid://shopify/Location/83278725336`**. Its POS subscription row showed a dash rather than affirmative Pro/renewal, and settings offered **Add POS Pro**. **Pro is unconfirmed**; neither Lite nor a purchase requirement is established. This live location is not the selected synthetic test location.
- **Identity/device:** the admin handle `degencollectibles` does not establish the canonical shop domain or Shop GID; those remain unverified. Jeffrey selected **iPad/iPhone**; the exact device, OS/POS versions and test-user contexts remain unfilled. No production schema, deployment or native-token verification occurred. Smile.io was listed but not opened or changed; free/off remains user-reported.

**Authorized creation result:** parent created **Degen Loyalty Dev** in organization **169353178**, verified app ID **422232031233**, with **zero installs**. Shopify automatically generated initial active version **degen-loyalty-dev-1**, version ID **1125275041793**; parent **never clicked Release**. The default app URL is **`example.com`**, no scopes were added, and no secrets were opened. This initial active version is not evidence that the loyalty extension was installed or natively tested. The numeric app ID is distinct from the OAuth client ID; parent subsequently supplied **`75c6c90117c95bd9f683cacaf3ed5c51`** from this DEV app’s install link. The approved **degen-loyalty-dev** store creation is complete; parent verified **degen-loyalty-dev.myshopify.com** on the Domains page. No paid subscription was selected, app install performed or secret read. Production Degen Ops remains excluded. Local linking/configuration and bounded dev-only preview preparation are now approved. Login, the private DEV secret, verified dev Shop GID, and actual install/scope review remain prerequisites; none implies production access. The cashier read policy needs no reapproval.

## Official requirements rechecked

Shopify’s POS tutorial requires a Partner account, dev store, current Shopify CLI and the POS app on iOS/Android. Its native preview flow connects the app to the dev store, starts `shopify app dev`, then opens the Dev Console mobile-preview QR link on a device signed into that dev store. The tutorial’s smart-grid sample is not our target: verify the existing customer-details block and action. [POS setup](https://shopify.dev/docs/apps/build/pos/getting-started)

Current CLI requirements are Node **22.12+**, Git **2.28+**, and a supported package manager. CLI 4 can auto-upgrade outside project-local/CI installs; the approved setup now pins CLI 4.5.2 locally. Its own help/build were checked: **`--config` and `--client-id` are mutually exclusive in 4.5.2**; use the exact client ID alone in the commands below. [CLI requirements and upgrade behavior](https://shopify.dev/docs/api/shopify-cli)

`app dev --store` accepts an existing development or Plus sandbox store; this preflight requires a named synthetic development store. Default development creates a tunnel; the approved bounded preview is restricted to this dev app/store and the safe synthetic backend. No tunnel has started. `--use-localhost` does not establish that a physical POS device can reach a WSL backend. POS refuses non-HTTPS fetches; permit Shopify’s documented extension origins without treating CORS as authorization. [Dev command](https://shopify.dev/docs/api/shopify-cli/app/app-dev), [HTTPS/CORS](https://shopify.dev/docs/apps/build/pos/communicate-with-server)

## Setup targets — observed context is not target approval

| Required target/evidence | Value |
|---|---|
| App owner/organization and authorized operator | Degen Collectibles **169353178**; parent completed the approved app/store creation and verified the development-store domain. Named native-test operator **UNFILLED**. |
| Dev-only app name/client ID, existing vs new, distribution and effect on any live installation | **Degen Loyalty Dev** created; app ID **422232031233**, zero installs; initial active **degen-loyalty-dev-1** / **1125275041793** generated by Shopify without parent clicking Release. OAuth client ID **`75c6c90117c95bd9f683cacaf3ed5c51`** from parent’s DEV install-link evidence; distribution **UNVERIFIED**. Production Degen Ops excluded. |
| Parent project/config path and real extension UID | **`shopify/loyalty-dev/shopify.app.toml`** prepared and CLI-built; CLI-generated local UID **`a611fd77-b939-c9be-7353-0235da6422209a2e1e4a`**; dashboard registration/link **PENDING LOGIN**. |
| Synthetic dev-store domain/Shop GID; confirmation it contains no real customer data | **COMPLETE**. Admin displays dev badge; **degen-loyalty-dev.myshopify.com** is **Primary / Connected**; Plan is **Basic Development store**. Shop GID **UNVERIFIED**. No paid subscription selected; generated test data and developer preview unchecked. |
| Test location ID, POS plan/extension availability, app-use permission | **UNVERIFIED / UNFILLED** for the completed development store. Observed live Senter Road location is not a test target; Pro remains unconfirmed. |
| iOS/Android device, OS/POS version and permitted test users/PIN contexts | **iPad/iPhone selected**; exact device, OS/POS version and permitted test users/PIN contexts **UNFILLED**. |
| Synthetic customer IDs and fixture ledger cases | **UNFILLED** |
| Reviewed HTTPS application URL, tunnel/provider, local port and exposure duration | HTTPS origin **UNFILLED**, CLI default temporary tunnel planned, backend **127.0.0.1:8768**, wrapper/backend **30-minute** maximum; the new dev app’s default URL is `example.com`, not an approved test backend. Production’s `https://example.com/callback` also remains unchanged. |
| Installed/requested scopes and protected-data approvals | Dev app creation baseline: **no scopes added**, zero installs. Local preview config requests **empty scopes**; actual install/grants **UNVERIFIED**. No customer API scope is requested by this read-only ledger UI. Existing Degen Ops configured scopes observed above lack `read_returns`; actual grants/protected-data access unverified. |
| Server-only credential handling reference; access-log redaction and retention | Explicit private `/tmp/degen-loyalty-native-handoff/dev-app-secret`, mode **0600**, not yet supplied. Access logs OFF; fresh synthetic scratch DB opened read-only after seeding. No secret in TOML, argv or response. |
| CLI version, dependency plan, named test/rollback owner and spending budget | CLI **4.5.2**, local lockfile installed; named operator/rollback owner **UNFILLED**. No paid services authorized. |
| Separate authorization for exact actions below | App/store creation **APPROVED**, no paid plan or production changes. Local setup and bounded dev-only preview **APPROVED**; install/scope prompt review still required. No install, tunnel, exposure or native-device test **EXECUTED**. |

## Ready commands and parent handoff

Run from this isolated worktree in WSL. Parent handles browser/device interaction; the worker does not open/control it.

1. Complete isolated CLI login, then link **only** Degen Loyalty Dev:

   ```bash
   python3 scripts/run_loyalty_shopify.py login
   python3 scripts/run_loyalty_shopify.py link
   ```

   These use the pinned local CLI, a private temporary HOME, and the supplied dev client ID. Linking can rewrite TOML; the launcher preserves the prepared public config in `/tmp/degen-loyalty-native-cli/prepared-app.toml`. Compare/restore the reviewed extension/web directories, empty scopes and dev-only build URL settings after linking. Do not select/create another app, accept production targets or release a version. CLI authentication requires parent assistance; no login completed in this worker run.

2. Parent securely supplies **only this DEV app’s** maintained signing secret to `/tmp/degen-loyalty-native-handoff/dev-app-secret` (regular owner-only file, **0600**, no symlink). Never paste it into chat, command arguments, TOML or logs. Provide `/tmp/degen-loyalty-native-handoff/native-inputs.json` with the following shape; **null is an unresolved placeholder, not a usable Shop GID**:

   ```json
   {"shop_id": null, "sample_customers": {}}
   ```

   Replace `shop_id` with the verified dev Shop GID. Optional `sample_customers` maps up to four **verified synthetic dev-store numeric customer IDs** to `earned`, `refund`, `pending` or `review`. No fabricated Shopify IDs are supplied here. These seed only local synthetic ledger cases (20 points, cumulative refund 19, pending 0, review 12); they do not create customers/orders or post Shopify/accounting changes. Empty mapping permits an authenticated empty-history test once installed. Customer creation remains dev-only, after exact app authorization and any minimum necessary scope review; parent can use the dev Admin UI without adding broad Admin API scopes.

3. Local deny check, after the secret and Shop GID are supplied:

   ```bash
   /tmp/degen-loyalty-test-venv/bin/python scripts/loyalty_native.py \
     --secret-file /tmp/degen-loyalty-native-handoff/dev-app-secret \
     --shop-id '<VERIFIED_DEV_SHOP_GID>' --check-only
   ```

   This prepares a fresh synthetic database and tests health, missing/invalid bearer, cookie denial and absence of token/Ops/docs routes **without binding a listener**. It does not manufacture a real Shopify session or prove credential-to-app binding; native token verification remains required.

4. Once login/link and private inputs are ready, parent runs:

   ```bash
   python3 scripts/run_loyalty_shopify.py dev
   ```

   The wrapper validates the exact app/store and empty scopes, reruns deny checks before invoking CLI, requires an interactive parent terminal, and stops the dev process group after at most **30 minutes**. Its underlying command is `shopify app dev --path shopify/loyalty-dev --client-id 75c6c90117c95bd9f683cacaf3ed5c51 --store degen-loyalty-dev.myshopify.com`. It may register the local UID/create a dev preview and temporary HTTPS tunnel and update only the dev preview URL. **Stop at the actual installation/permission prompt for parent review; do not silently approve grants.** No deploy/release command is provided. The backend consumes only the CLI’s public client/URL/port context, ignores the injected CLI secret, clears inherited settings, and loads the explicit private secret file. Only health and the bearer read/OPTIONS route are served; no `/demo/token`, Ops/admin/docs, external providers or accounting writes.

5. On the selected **iPad/iPhone**, record exact iOS and Shopify POS versions and confirm login to **degen-loyalty-dev**, test location and allowed test-user context. Open the actual CLI Dev Console mobile-preview QR/link only once running. Verify both customer-details targets, earned/refund/pending/review, refresh/history bounds, customer/PIN/session switching, offline/timeouts and zero writes. PIN changes test privacy, never verified identity. Verify missing/wrong-app/shop tokens deny access. Corrections remain in Ops. Native component availability/Pro entitlement remains unconfirmed; do not buy a plan. **No native preview URL/QR or native pass result exists yet.**

## Local verification for this setup

- Exact current results and logs: [phase-two runbook](phase-two-runbook.md#september-11-native-setup-implementation).
- CLI **4.5.2** local build passes for the parent configuration, web TOML and both extension targets. Type-check, esbuild and **12 component tests** pass. The backend has dedicated synthetic safety tests plus phase-one/POS regression coverage.
- The link attempt with the correct target stopped at **interactive authorization required**. No shop/customer data API was called; no app install, release, native token, tunnel or device result is claimed.
- Official CLI/web process contracts verified: [config link](https://shopify.dev/docs/api/shopify-cli/app/app-config-link), [build](https://shopify.dev/docs/api/shopify-cli/app/app-build), [web process variables](https://shopify.dev/docs/apps/build/cli-for-apps/app-structure). CLI 4.5.2 help takes precedence over newer documentation flags unavailable in that installed version.

## Rollback and exit criteria

Stop the named development process/tunnel and disable both test read flags. Under the same explicit dev-test authorization, run `shopify app dev clean` with the same `--path`, `--client-id` and `--store` targets (do not combine `--config` with `--client-id` in CLI 4.5.2): Shopify documents that it stops the preview and restores the app’s active version on that dev store. Confirm the captured active version is the intended rollback target before running it. [Dev cleanup](https://shopify.dev/docs/api/shopify-cli/app/app-dev-clean)

Restore any changed dev-only URL/config/placement from the captured baseline and verify preview exposure is gone. Cleanup is a Shopify dev-state write; review its exact dev target/active-version restoration before executing. This worker has not run cleanup because no preview or install started. An app install, issued credential, test-data creation or retained external log is not undone merely by stopping the tunnel; name and approve each required cleanup separately. Do not delete ledger data, overwrite newer accounting with a backup, uninstall a shared live app, or touch production, Smile, inventory/TikTok ingestion or existing demos. Remove the explicitly created private secret/CLI session and scratch artifacts after the approved test, without touching other auth stores or demos; stopping the backend alone does not dispose these files. Budget, owner, retention and phase-one launch IDs/cutoff remain unspecified; no activation follows automatically from a passing native test.
