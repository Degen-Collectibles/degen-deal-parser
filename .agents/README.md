# Ruflo Agent Sidecar

This directory tracks the minimal project-local Ruflo integration for Codex and
OpenClaw-style agents working on `live-deal-parser`.

Use the safe wrapper first:

```powershell
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py status
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py init-memory --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py seed-memory --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py preflight "fix parser stitching" --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py handoff-openclaw "fix parser stitching" --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py remember "parser/example" "what worked" --tags parser,degen --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py review-diff --apply
```

The wrapper is dry-run by default. Runtime databases and logs created by Ruflo
are ignored in `.gitignore`.

`preflight` prints current git status, searches the `degen` Ruflo memory
namespace, and asks Ruflo hooks for routing hints. `handoff-openclaw` writes an
ignored markdown packet under `.ruflo/handoffs/` unless `--out` is provided.
`remember` stores a new lesson in Ruflo memory with `--upsert`.

The wrapper pins `ruflo@3.7.0-alpha.45` because the moving `latest` alpha tag
has broken during testing. `review-diff` currently uses Ruflo's `--classify`
and `--reviewers` paths only. In this Ruflo line, `analyze diff --risk` prints
a risk table but then exits with a TypeError, so the wrapper avoids that
alpha-only failure path.

Do not run `ruflo init --codex --force` in this repo. The generated init flow
would create a generic `AGENTS.md`; this repo already has a project-specific
one that is the source of truth.
