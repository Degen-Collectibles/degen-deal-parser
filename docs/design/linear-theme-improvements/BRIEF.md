# Linear Theme Improvements — implementation brief

Source: `docs/design/linear-theme-improvements/project/wireframes/Linear Theme Improvements.html`

This brief is written as direct imperative instructions for the implementer.

## Scope locked (Jeffrey, 2026-04-21)

Three directions. Implement **A**, **B**, and **C** as described. **Do NOT** build the More-sheet. **Keep the current bottom-nav order.** For the Eye icon, use **Concept 3 — Live Eye** (eye with a green live dot in the corner, matching the existing `db-live` pattern).

---

## Step 0 — Before anything

```bash
cd /home/ubuntu/degen-deal-parser
git fetch origin main && git pull --rebase origin main
```

Read `docs/design/linear-theme-improvements/project/wireframes/Linear Theme Improvements.html` top-to-bottom — the three tabs (A Dashboard, B Deals, C Mobile Nav) show before/after for every change. Colors, tokens, and class names are already defined in the existing `app/static/linear.css`.

Existing tokens to use (already in `app/static/linear.css`): `--lx-bg`, `--lx-s1`, `--lx-s2`, `--lx-br`, `--lx-br2`, `--lx-text`, `--lx-muted`, `--lx-dim`, `--lx-red`, `--lx-live`, `--cx-orange`, `--cx-gold`, `--cx-green`. If any are missing, add them — match values from the wireframe.

---

## A — Dashboard hierarchy (`app/templates/dashboard.html` + `app/static/linear.css`)

Goal: revenue card becomes the visual hero; add a derived Net P&L card; de-emphasize breakdown rows so they don't compete with headline numbers.

### A1 — Lead stat accent (desktop + mobile)

On the **Revenue today** card:
- Add a 2px orange top border (`border-top: 2px solid var(--cx-orange)`). Use a new CSS class like `.db-card--accent-revenue` or a generic `.db-card--accent-top` + `.db-card--accent-orange`. Prefer a general modifier so Net P&L and Deals cards can reuse it with different colors.
- Very subtle orange-tinted background: `background: rgba(255,106,28,.04);`.
- Card label (`Revenue today`) in `var(--cx-orange)`.
- Main number in `var(--cx-orange)` and **~28% larger** than sibling cards (design shows 36px lead vs 28px others on desktop; on mobile lead is 28px vs 18px). Use a modifier class `.db-num--lg` or similar.

### A2 — Add derived Net P&L card (new, green-top)

After Purchases card, insert a new card:
- Label: `Net P&L today` in `var(--cx-green)`.
- Value: `revenue − purchases` computed in the template (reuse whatever scalars are already passed to the dashboard context; if they're not both available, compute in the route handler and pass in as `net_pl_today`). Format with `+` sign when positive, `-` when negative.
- 2px green top border, `background: rgba(147,214,167,.03);`.
- Breakdown row: single `margin` row showing `(net / revenue) * 100 | round`%, colored green.
- Number color: green if positive, `var(--lx-red)` if negative, `var(--lx-muted)` if zero.

### A3 — Variant 3 (desktop 5-column): OUT of scope for this pass

Stick to the 4-card layout already in place + Net P&L inserted. The 5-card Deals-today variant is nice-to-have; don't do it this pass.

### A4 — Mobile 2×2 stat grid

On mobile (≤600px breakpoint — use whatever `@media` the theme already uses for mobile):
- Revenue card stays full-width as the hero, with breakdown lines shown inline (Discord $X · TikTok $Y) as horizontal text, not as stacked `.db-bk` rows.
- Below it, a 2×2 CSS grid with 4 compact `.db-metric` cards: **Purchases**, **Net P&L** (green-top), **Review** (red number if > 0), **Deals**.
- Each compact metric: small uppercase label + single medium number (~18px). No breakdown rows.
- Below the 2×2 grid, a "Recent deals" teaser list — 2–3 rows, thumb + title + right-aligned colored amount. Link to `/deals`.

### A5 — "Needs attention" → red when count > 0

Current card uses default color. When count > 0, the number should be `#fca5a5` (muted red, per design). Add a conditional class `.db-num--warn` on the number.

---

## B — Deals table (`app/templates/deals.html`)

Goal: reduce above-the-fold filter form; make the table itself the primary content; color-code money so it scans at a glance.

### B1 — Replace the filter card with a compact top strip

Current state: a big `Filter The Ledger` card with 5 inputs + apply button. Replace with:

- **Kind tab switcher** (left): segmented control with options `All · Sales · Buys · Expenses · Other`. Active tab = orange background + orange border + orange text (uses existing `.lx-chip--active` or similar pattern). Clicking a tab submits the form with `kind=<value>` or, if HTMX is in use on this page, swaps the table via HTMX. Use standard form GET param approach if no HTMX — each tab is an `<a>` with the right query string.
- **Active-filter chips** (right, inline): each active non-`kind` filter renders as a dismissible chip (`Today ✕`, `buying-channel ✕`). ✕ links back to the current URL with that param stripped.
- **`+ Filter` button** at the far right: opens a collapsible `<details>` or a small dropdown containing the remaining filter inputs (date range, channel, search). Collapsed by default. Preserve existing filter form logic — just collapse the UI.

### B2 — Inline summary strip (replace 5-metric card row)

Below the filter strip, a single horizontal row of colored pill-style summaries:
- **Sales** — orange-tinted pill, orange label + number.
- **Buys** — neutral surface, default colors.
- **Net** — green-tinted pill, green number.
- **Rows** — neutral.

Pill shape: `padding: 5px 12px; border-radius: 7px;` with tinted `background` + matching `border-color`. Label = 10px uppercase bold. Number = 16px bold tabular-nums.

### B3 — Color-coded amount column in the table

Amounts get classes by `kind`:
- `sale` → `--cx-orange`
- `buy` → `--cx-gold`
- `expense` → `--lx-red`
- `other` / unknown → `--lx-dim` (with `—` dash if null)

Use `font-variant-numeric: tabular-nums` so numbers align. Right-align the column. Add CSS classes (`.deals-amount--sale`, `--buy`, `--expense`, `--unknown`) — pick the class in the template from the kind field.

### B4 — BK (bookkeeping) status as icon column

Replace any wide BK text with a narrow icon column:
- `✓` in `var(--lx-live)` when bookkept
- `~` in `#fbbf24` when partial/pending
- `—` in `var(--lx-dim)` when absent

~52px column, centered.

### B5 — Mobile: identical strip + compact rows

Mobile deals page: same kind-tab switcher (4 tabs fit: `All Sales Buys Other` — hide `Expenses` into `Other` if needed, or scroll horizontally), same summary strip (3 pills: Sales · Buys · Net). Deal rows: thumb + title/meta + right-aligned colored amount.

---

## C — Mobile nav: Eye as raised FAB with Live Eye icon

Files: `app/templates/_mobile_bottom_nav.html`, `app/static/linear.css`.

### C1 — Keep current order

Leave the order as-is: `Dashboard · Streamer · Eye · Deals · Review`. No changes to the link list or routes.

### C2 — Eye becomes a raised circular FAB

Current: `.mbn-item-center` (Eye) uses the same box as other items. Change to:
- 46px circular button (`border-radius: 50%`)
- Background: `linear-gradient(135deg, var(--cx-orange), #c44a00)`
- Box-shadow: `0 4px 16px rgba(255,106,28,.5)`
- 2px border in `#0b0c0e` to cut cleanly from the nav surface
- Positioned with `margin-top: -18px` so it sits raised above the nav bar
- Icon SVG stroke = `#fff`, stroke-width `2`
- Label below stays `Eye`, color `var(--cx-orange)`, weight 700

### C3 — Live Eye icon (Concept 3)

Replace the current Eye `<svg>` in `_mobile_bottom_nav.html` with:

```html
<div class="mbn-eye-fab">
  <svg class="mbn-icon" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
    <circle cx="12" cy="12" r="3" fill="rgba(255,255,255,.3)"/>
  </svg>
  <span class="mbn-eye-live-dot" aria-hidden="true"></span>
</div>
```

The `.mbn-eye-live-dot` is a 12px circle in `var(--lx-live)` with a 2px `#0b0c0e` border, absolutely positioned top-right of the FAB. This matches the existing `db-live` dot pattern already in the codebase.

### C4 — Active state stays the same

When `/degen_eye` is the active route, preserve existing active styling on the label only — the FAB itself always looks "live". Use `aria-current="page"` on the anchor when active.

---

## Cache-bust

After CSS edits, bump the `linear.css?v=...` query string everywhere. Use:

```bash
cd /home/ubuntu/degen-deal-parser
NEW_VER=$(date +%s)
grep -rln 'linear.css?v=' app/templates/ | xargs sed -i "s/linear\\.css?v=[^\"']*/linear.css?v=${NEW_VER}/g"
```

(This is the exact pattern Jeffrey documented in `~/.openclaw/workspace/TOOLS.md` under "Cache-bust sweeps". Single `sed` across all templates, not just the shared head partial.)

If any page still links `courtyard-theme.css`, leave it alone — that's a different theme. This pass only touches Linear theme templates.

---

## Don't do

- ❌ Screenshots / browser rendering — the source HTML spells out everything (per the bundle README).
- ❌ The "More" sheet / bottom nav reorder (out of scope per Jeffrey).
- ❌ The Desktop Variant 3 (5-card dashboard row) — out of scope this pass.
- ❌ Editing `courtyard-theme.css` or anything outside the Linear theme path.
- ❌ Committing on Machine B. Edit locally on `/home/ubuntu/degen-deal-parser` and push to origin — Machine B auto-deploys on pull.

## Commit + push

One commit per direction, or a single cohesive commit if the diff is tight:

```bash
cd /home/ubuntu/degen-deal-parser
git add -A
git commit -m "ui(linear): dashboard hierarchy + deals scannable table + eye FAB

Implements design handoff wireframes/Linear Theme Improvements.html.

A — dashboard: revenue card orange accent-top + larger lead number,
added derived Net P&L card (green-top), mobile 2x2 stat grid with
revenue hero, warn color on review count.

B — deals: replaced filter card with kind-tab switcher + dismissible
filter chips + inline summary pills. Amounts color-coded by kind
(sale=orange, buy=gold, expense=red). BK status as icon column.

C — mobile nav: Eye promoted to raised FAB (orange gradient, 46px
circle) with green live-dot (Concept 3). Nav order unchanged.

Cache-busted linear.css."
git push origin main
```

Then confirm by reading back `git log -1 --stat`.
