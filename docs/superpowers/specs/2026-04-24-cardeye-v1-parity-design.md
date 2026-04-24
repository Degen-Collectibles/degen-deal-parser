# Degen Eye v2 — v1-parity scan flow & perceived-latency fix

**Date:** 2026-04-24
**Owner:** jeffr
**Status:** Spec — awaiting review before implementation plan

## Motivation

Degen Eye v2 was built as a pHash-first, local-scan upgrade to v1's cloud pipeline. In practice it regressed two properties that made v1 feel reliable:

1. **Scans disappear instead of landing in the batch drawer.** When the orchestrator returns `AMBIGUOUS` (same-art reprint risk, LOW pHash with disagreeing Ximilar, or any uncertainty), the frontend shows a red "Needs review" toast and drops the card. v1 added every scanned card and let the user fix low-confidence ones at `/inventory/scan/batch-review`.

2. **Time-to-identify feels slow despite being faster on paper.** v2 uses a two-request SSE flow (`POST /scan-init` → `GET /scan-stream`) for the single-tap path, adding a round-trip plus a server-side pending-file write. The 4-dot progress indicator also makes a ~100ms operation *feel* multi-stage.

The goal of this spec is **v1-style reliability with v2's local-first speed**. No changes to the pHash orchestrator's confidence semantics — the backend keeps reporting its honest verdict. The frontend stops treating AMBIGUOUS as rejection.

## Goals

- Every scan that produces a `best_match` enters the batch drawer. No silent drops.
- Confidence (HIGH / MEDIUM / LOW / AMBIGUOUS) is surfaced in the drawer as a warning pill so the user sees which items to double-check at review time.
- Default tap-to-scan path is a single POST, not SSE init + stream. Auto-capture and opt-in "streaming mode" may keep SSE.
- Perceived time-to-identify ≤ v1, ideally noticeably faster when the pHash index is healthy.
- Zero regressions to v1 behavior, the capture/training loop, or the `/degen_eye/v2/stats` / `/degen_eye/v2/history` surfaces.

## Non-goals

- **Rewriting the orchestrator.** `run_v2_pipeline` keeps its current tiered logic (pHash → raw-image retry → Ximilar fallback on LOW → AMBIGUOUS if reprint-risk).
- **Keypoint matching, CNN embeddings, set-symbol OCR.** These are valid future accuracy levers but are out of scope; this spec is about reliability + perceived speed, not raw match quality.
- **Non-Pokemon TCGs in v2.** v2 is Pokemon-only by design; multi-TCG remains v1's territory.
- **Nightly index rebuild automation.** Separate Phase C item, unaffected here.

## Design

### 1. Frontend — `app/templates/inventory_scan_pokemon_v2.html`

**1a. Every `best_match` goes to the drawer.** Replace the MATCHED-gated branch in the `done` handler (currently ~L551) with:

```js
if (finalResult && finalResult.best_match) {
    addToBatch(finalResult);            // fires for MATCHED *and* AMBIGUOUS
    setTimeout(_hideResult, 900);
} else {
    // NO_MATCH or ERROR — only case where we don't add to batch
    showToast(finalResult?.error || 'No card detected', 'err', 2500);
    setTimeout(_hideResult, 1500);
}
```

**1b. Confidence pill in the drawer.** `addToBatch` already stores the match — extend `renderBatch` to render a small colored pill next to the card name:

| Status / confidence | Pill text | Pill color |
|---|---|---|
| MATCHED + HIGH | (no pill) | — |
| MATCHED + MEDIUM | "medium" | neutral (`--muted`) |
| MATCHED + LOW | "low" | warning yellow |
| AMBIGUOUS | "review" | warning yellow, bold |
| (any) + source=ximilar | append "· cloud" | secondary |

Pill tooltip explains why (e.g., "Same-art reprint risk — confirm the set on the review page"). The pill is also a hint to the reviewer at `/inventory/scan/batch-review` — existing review UI already shows the full candidate list.

Persist the pill state on the batch item: add `_confidence` (`"HIGH"|"MEDIUM"|"LOW"|"AMBIGUOUS"`) and `_source` (`"phash"|"ximilar"|"phash+ximilar"`) when pushing to `localStorage.scan_batch`. Existing v1 items without these fields render with no pill (graceful).

**1c. Default flow uses `POST /degen_eye/v2/scan` (non-streaming).** The route already exists and returns the full ScanResult in one shot. Replace the `/scan-init` + EventSource dance in the tap-to-scan handler with:

```js
const resp = await fetch('/degen_eye/v2/scan', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({image: b64, category_id: '3'})
});
const result = await resp.json();
if (result.best_match) addToBatch(result);
else showToast(result.error || 'No card detected', 'err');
```

Drop the 4-dot progress card on this path — show a single "scanning…" state that resolves to the card thumbnail + name. The 4-dot card stays behind a "live progress" toggle (localStorage flag) for users who like it; auto-capture can keep using SSE internally for its own overlay logic.

**Rationale:** cuts one HTTP round-trip and one pending-file write per scan. The user's perceived timeline becomes `shutter → single spinner → card in drawer` instead of `shutter → 4 dots flashing → card somewhere`.

**1d. Auto-capture flow unchanged for this spec.** It keeps polling `/detect-only` and firing a full scan after 3 stable frames. It can continue to use `/scan-init` + SSE for its live-progress overlay, or switch to `/scan` — either works. Defer that decision until after telemetry lands.

### 2. Backend — no behavioral changes

`run_v2_pipeline`, `run_v2_pipeline_stream`, the orchestrator's MATCHED/AMBIGUOUS logic, `_phash_exactness`, Ximilar fallback, price enrichment — all unchanged. The spec is explicitly a frontend + routing change.

The one exception: **add lightweight telemetry** so we can decide Phase 2 backend work with data instead of guessing.

`app/degen_eye_v2.py` — extend `_save_v2_history` entries with `stages_ms` already-present fields and add:

- `network_elapsed_ms` — time from request-received to first byte of response (set by a middleware, not the orchestrator)
- `phash_lookup_ms` — already in `v2_debug["stages_ms"]["phash"]`, just surface it in the history summary
- `identified_ms` — total time from request-received to the point where `identified` is first knowable (detect + pHash done)
- `total_ms` — equivalent to existing `processing_time_ms` (keep for parity)

These are added as top-level keys on the v2 history entry, then the debug page renders a p50/p95 table. No new routes, no DB schema change.

### 3. pHash lookup — Phase 2 (deferred, not in this spec's scope)

Vectorizing `phash_scanner._hamming` with numpy is a clean ~20×–30× speedup in theory (~20ms → ~1ms on 20k entries). **We don't ship it in this spec** because:

1. The user-perceived bottleneck is the UI gate + SSE round-trip, not the Python loop. Fix what the user sees first.
2. Adding numpy broadcast code before measuring gives us nothing to verify against.
3. Once the telemetry above is live, we'll know whether pHash lookup even shows up in the p95 latency — if it's dominated by price enrichment (likely), numpy vectorization is a write-optimization with no user-facing win.

Phase 2 follow-up spec if telemetry shows pHash lookup is the bottleneck.

## Files touched

| File | Change |
|---|---|
| `app/templates/inventory_scan_pokemon_v2.html` | Rework `done` handler (§1a), add pill rendering (§1b), swap to POST `/scan` default path (§1c), keep SSE path behind a flag |
| `app/degen_eye_v2.py` | Add `identified_ms` field to history entries (§2) |
| `app/inventory.py` | Add a tiny timing middleware or directly stamp `network_elapsed_ms` in the `/scan` and `/scan-stream` handlers (§2) |
| `tests/test_degen_eye_v2_scan.py` *(new or extend existing)* | Unit test: `/scan` response on an AMBIGUOUS result includes `best_match` and confidence is "AMBIGUOUS" (frontend now relies on this); pill-rendering snapshot test if the suite supports it |

Unchanged: `app/phash_scanner.py`, `app/card_detect.py`, `app/price_cache.py`, `scripts/build_phash_index.py`, orchestrator routing logic.

## Testing

- **Unit.** Mock the pHash index with a fixture card, run `run_v2_pipeline` with a synthetic near-miss input that forces AMBIGUOUS, assert `best_match` is populated and `status == "AMBIGUOUS"`. (The frontend's new contract is "best_match present → add to drawer"; the test just pins that contract.)
- **Manual.** Scan 5 real cards on machine B's v2 page with the index built — confirm all 5 land in the drawer, that LOW/AMBIGUOUS ones show the pill, and that the tap-to-review flow at `/inventory/scan/batch-review` shows them for fix-up. Compare perceived latency to v1 side-by-side on the same device.
- **Regression.** Existing `tests/test_schedule_mobile.py`-style integration tests that touch `/degen_eye/v2/*` continue to pass.

## Risks & tradeoffs

- **Users might accidentally confirm a LOW-confidence scan without checking.** The pill is a visible hint but doesn't block. Mitigation: `/inventory/scan/batch-review` already shows alternative candidates for each item; low-confidence items will sort to the top of the review list (small addition, one sort key).
- **Dropping the 4-dot progress may feel like a feature removal.** Kept behind a localStorage toggle so power users can opt back in without code changes.
- **POST-only default path loses the live "price is loading" signal.** Users who scan with a cold cache will see a single spinner for ~500ms instead of "card name, then price drops in." Acceptable — v1 had the same single-shot feel.
- **Telemetry is additive but still adds a small amount of history-entry bloat.** Four numeric fields, negligible disk impact.

## Open questions

- **Should the tap path also retry once on network error before showing a toast?** v1 does not; defer.
- **Should the warning pill include the specific reason (`"same-art reprint"`, `"low pHash"`, `"cloud fallback disagreed"`)?** Yes in tooltip, no in pill text — keeps the pill short.
- **Telemetry destination.** Extending the existing `_V2_SCAN_HISTORY` JSONL is the simplest path and what this spec proposes. Upgrading to a real observability backend is out of scope.

## Acceptance

This spec is done when:

1. Scanning a card with LOW pHash + no Ximilar agreement produces a drawer entry with a "review" pill (not a disappearing toast).
2. A healthy-index scan (HIGH pHash) goes from shutter tap to in-drawer in ≤ v1's measured time on the same device, and subjectively feels faster.
3. `/degen_eye/v2/debug` (or equivalent telemetry surface) shows per-stage p50/p95 numbers that let us make the next backend decision from data.
