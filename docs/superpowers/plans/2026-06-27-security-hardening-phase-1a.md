# Security Hardening Phase 1A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the three validated browser-script execution paths without changing safe attachment, deal-detail, or TikTok configuration behavior.

**Architecture:** Enforce a shared response policy at both attachment sinks, normalize deal return targets once before authorization/rendering, and HTML-escape the TikTok title only at its HTML sink. Preserve raw provider data for JSON consumers and preserve known-good local navigation paths.

**Tech Stack:** Python 3.14, FastAPI/Starlette, Jinja2, unittest/pytest, Pillow

---

### Task 1: Block active attachment content at both response sinks

**Files:**
- Modify: `app/main.py:655-738`
- Test: `tests/test_attachment_active_content_security.py`

- [ ] **Step 1: Write the failing direct-route and thumbnail tests**

Create route tests that store an SVG asset and assert the direct route returns `application/octet-stream`, `Content-Disposition: attachment`, `X-Content-Type-Options: nosniff`, and a restrictive CSP. Assert the thumbnail route never returns the original SVG bytes or an inline SVG media type after Pillow rejects it. Add a PNG case asserting the existing inline image behavior still works.

- [ ] **Step 2: Run the new tests and verify the vulnerable assertions fail**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_attachment_active_content_security.py -q
```

Expected: SVG direct-response headers and/or thumbnail fallback assertions fail against the vulnerable implementation; PNG control passes.

- [ ] **Step 3: Implement one shared untrusted-attachment response policy**

Add a strict passive raster allowlist such as `image/png`, `image/jpeg`, `image/gif`, `image/webp`, and `image/bmp`. Inline only allowlisted types. For every other type, return `application/octet-stream` with attachment disposition. Add `X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox; default-src 'none'` to untrusted attachment responses. When thumbnail creation fails for a non-allowlisted source, return `415 Unsupported Media Type` or another non-content response; never return the original active bytes inline.

- [ ] **Step 4: Verify the focused tests and nearby attachment suite**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_attachment_active_content_security.py tests/test_attachment_routes.py tests/test_legacy_security_hardening.py tests/test_discord_ingest.py -q
```

Expected: all selected tests pass.

- [ ] **Step 5: Checkpoint the narrow diff**

Run `git diff --check` and `git diff -- app/main.py tests/test_attachment_active_content_security.py`. Do not commit, push, deploy, or rotate credentials without Jeffrey's separate approval.

### Task 2: Normalize deal-detail return targets before authorization and rendering

**Files:**
- Modify: `app/routers/deals.py:27-219`
- Test: `tests/test_deal_return_path_security.py`

- [ ] **Step 1: Write failing hostile-return-target tests**

Parameterize `javascript:`, `data:`, `//evil.example`, backslash-prefixed forms, query/fragment-bearing bases, and percent-decoded CR/LF/NUL. Assert every hostile input is replaced by `/deals` in `back_url`, `return_path`, authorization selection, and hidden form context. Add positive cases for `/deals`, `/table`, `/ledger`, and an ordinary local application path.

- [ ] **Step 2: Run the new tests and verify the vulnerable assertions fail**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_deal_return_path_security.py -q
```

Expected: at least the `javascript:` case fails because it reaches the rendered return link unchanged.

- [ ] **Step 3: Implement `_normalize_return_path` and reuse its result everywhere**

At the beginning of `deal_detail_page`, percent-decode for validation, reject parse errors, control characters, backslashes, schemes, netlocs, fragments, embedded queries, protocol-relative paths, and values that do not begin with exactly one `/`. Return `/deals` on rejection. Replace the local `return_path` value before role selection, `build_return_url`, and template context construction.

- [ ] **Step 4: Verify focused and existing routing tests**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_deal_return_path_security.py tests/test_admin_table_detail_routing.py -q
```

Expected: all selected tests pass, including valid table and ledger navigation.

- [ ] **Step 5: Checkpoint the narrow diff**

Run `git diff --check` and `git diff -- app/routers/deals.py tests/test_deal_return_path_security.py`. Do not commit or publish.

### Task 3: Encode TikTok LIVE titles at the HTML sink

**Files:**
- Modify: `app/routers/tiktok_streamer.py:3201-3280`
- Test: `tests/test_tiktok_streamer_config_security.py`

- [ ] **Step 1: Write a failing direct-route rendering test**

Mock the live-status provider with a title such as `<img src=x onerror=alert(1)>`. Call `tiktok_streamer_config`, assert the raw element and event-handler markup are absent from the HTML response, and assert the escaped text is present. Add a plain-title control.

- [ ] **Step 2: Run the new test and verify it fails for the raw title**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_tiktok_streamer_config_security.py -q
```

Expected: malicious title test fails because the current f-string inserts provider text as HTML.

- [ ] **Step 3: Escape only the HTML-rendered title**

Import the standard-library `html` module and apply `html.escape(auto_title, quote=True)` immediately before interpolation into the config page. Do not alter the provider/cache value or JSON response behavior.

- [ ] **Step 4: Verify focused and nearby TikTok tests**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_tiktok_streamer_config_security.py tests/test_public_tiktok_live_status.py -q
```

Expected: all selected tests pass and the public JSON title remains unchanged.

- [ ] **Step 5: Verify the complete phase**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m compileall app
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest --tb=short -q
```

Expected: compile succeeds and the full suite passes. Then inspect `git diff --check` and the complete phase diff. Do not commit, push, deploy, or perform operational changes.
