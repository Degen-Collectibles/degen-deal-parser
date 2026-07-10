# Green autodeploy cutover plan

## Current Green deployment contract (2026-07-10)

Green/Brev `openclaw-9902ae` is the active production host. GitHub Actions deploys pushes to `main` through two fail-closed stages:

1. `.github/workflows/deploy.yml` verifies branch `main`, rejects tracked checkout changes, fetches `origin/main`, fast-forwards `/opt/degen/app` to the triggering `$GITHUB_SHA`, and verifies exact equality.
2. The workflow invokes `scripts/redeploy-linux.sh` with `DEGEN_EXPECTED_GIT_SHA="$GITHUB_SHA"`. The script rechecks branch, tracked cleanliness, SHA shape, and exact `HEAD` before dependencies, environment updates, or service restarts. It does not fetch or pull in this mode.

Manual execution without `DEGEN_EXPECTED_GIT_SHA` retains the script's existing `git fetch origin main` and `git pull --rebase origin main` behavior.

### Current Green rollback contract

Preferred rollback is repository-driven and exact-SHA: create a corrective commit on `main`, or a targeted content revert that preserves the hardened `.github/workflows/deploy.yml` and `scripts/redeploy-linux.sh`; then let the SHA-pinned workflow synchronize `/opt/degen/app` to that new `$GITHUB_SHA` and redeploy it with `DEGEN_EXPECTED_GIT_SHA="$GITHUB_SHA"`.

A full revert of the deploy-hardening branch is a separate rollback mode because it restores the legacy workflow and script. GitHub evaluates that restored legacy deploy entrypoint at the full-revert commit, so this path is not SHA-pinned. It requires separate explicit approval, a preflight for Green/Brev `openclaw-9902ae` that verifies branch `main`, tracked cleanliness, the intended full-revert commit, and the expected legacy entrypoint behavior, followed by post-deploy verification of the deployed `HEAD`, service health, and the public health endpoint.

Do not use `git reset --hard`, force-push, force checkout, or deletion of untracked operational files as deployment or rollback mechanisms. Investigate tracked drift and resolve it through the canonical repository.

Any urgent manual deployment requires separate explicit approval. Before it runs, verify branch `main`, tracked cleanliness of both the working tree and index, and that `HEAD` equals the intended 40-character lowercase Git SHA; invoke the script with that SHA in `DEGEN_EXPECTED_GIT_SHA` so it rechecks exact equality before any deployment side effects.

**Historical-record boundary:** All remaining Machine B and cutover material below is a historical preparation record preserved for context only. It is not current production instruction; the current Green contract above governs production today.

Current production deploys from GitHub Actions to Machine B using a Windows self-hosted runner named `DESKTOP-PPF7VK9`. The live workflow is intentionally **not** changed in this prep commit because any push to `main` currently autodeploys Machine B.

## Current Machine B flow

- Workflow: `.github/workflows/deploy.yml`
- Trigger: push to `main`
- Runner selector: `self-hosted`
- Machine B runner: `DESKTOP-PPF7VK9`, labels `self-hosted`, `Windows`, `X64`
- Working directory: `C:\Users\Degen\degen-deal-parser`
- Deploy step: `powershell -ExecutionPolicy Bypass -File .\scripts\redeploy.ps1`
- Health check: `http://127.0.0.1:8000/health`

## Why the workflow must change at cutover

The current runner selector is too broad:

```yaml
runs-on: self-hosted
```

Once Green also has a self-hosted runner, GitHub may route deploy jobs to either Machine B or Green. At cutover, use a unique label such as `degen-prod` and target it explicitly:

```yaml
runs-on: [self-hosted, degen-prod]
```

Only the active production host should carry the `degen-prod` label.

## Green host files prepared by this branch

- `scripts/redeploy-linux.sh` — Linux equivalent of `scripts/redeploy.ps1`
- `deploy/systemd/degen-web.service` — production web unit on `127.0.0.1:8000`
- `deploy/systemd/degen-worker.service` — production worker unit
- `deploy/systemd/degen-actions-runner.sudoers` — narrow sudoers template for runner restarts

## One-time Green setup before switching production traffic

Run these on Green only after approving the cutover prep window.

```bash
cd /opt/degen/app
git fetch origin main
git pull --rebase origin main
chmod +x scripts/redeploy-linux.sh
sudo install -o root -g root -m 0644 deploy/systemd/degen-web.service /etc/systemd/system/degen-web.service
sudo install -o root -g root -m 0644 deploy/systemd/degen-worker.service /etc/systemd/system/degen-worker.service
sudo install -o root -g root -m 0440 deploy/systemd/degen-actions-runner.sudoers /etc/sudoers.d/degen-actions-runner
sudo systemctl daemon-reload
```

Do not start `degen-worker.service` until the official cutover/freeze window. Starting it early can duplicate worker/Discord/TikTok activity with Machine B.

A web-only dry run can be done safely with worker disabled:

```bash
sudo systemctl start degen-web.service
curl -fsS http://127.0.0.1:8000/health
sudo systemctl stop degen-web.service
```

## Register a Green GitHub Actions runner

Jeffrey must create/copy the ephemeral runner registration command from GitHub; do not paste the token into Telegram.

GitHub path:

1. Open `https://github.com/jmanballa/degen-deal-parser/settings/actions/runners/new`
2. Choose Linux x64.
3. Copy the generated commands/token directly into the Green host shell.
4. Add labels including `degen-prod` during `config.sh`.
5. Install/run it as a systemd service if possible.

Recommended install directory:

```bash
/opt/actions-runner/degen-deal-parser
```

Recommended runner labels:

```text
self-hosted,Linux,X64,degen-prod
```

## Workflow patch to apply at cutover

Replace the Windows deploy job with the Linux deploy job:

```yaml
name: Deploy to Degen Prod

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: [self-hosted, degen-prod]
    defaults:
      run:
        working-directory: /opt/degen/app

    steps:
      - name: Synchronize production checkout
        shell: bash
        run: |
          set -euo pipefail

          current_branch="$(git rev-parse --abbrev-ref HEAD)"
          if [[ "$current_branch" != "main" ]]; then
            echo "ERROR: expected branch main, got $current_branch" >&2
            exit 3
          fi

          if ! git diff --quiet || ! git diff --cached --quiet; then
            echo "ERROR: tracked production checkout changes must be resolved before deploy" >&2
            git status --short --untracked-files=no >&2
            exit 6
          fi

          git fetch origin main
          if ! git merge --ff-only "$GITHUB_SHA"; then
            echo "ERROR: production checkout cannot fast-forward to $GITHUB_SHA" >&2
            exit 7
          fi

          actual_sha="$(git rev-parse HEAD)"
          if [[ "$actual_sha" != "$GITHUB_SHA" ]]; then
            echo "ERROR: expected checkout $GITHUB_SHA, got $actual_sha" >&2
            exit 8
          fi

      - name: Redeploy app
        run: DEGEN_EXPECTED_GIT_SHA="$GITHUB_SHA" ./scripts/redeploy-linux.sh

      - name: Verify health
        run: curl -fsS http://127.0.0.1:8000/health >/dev/null
```

The synchronization step must remain before `Redeploy app`; otherwise a commit that changes `redeploy-linux.sh` can execute the prior in-memory script on its first deployment.

Do not leave `runs-on: self-hosted` after both runners exist.

## Cutover order

1. Freeze Machine B worker/app:
   ```powershell
   schtasks /end /tn DegenParser
   ```
2. Confirm Machine B is stopped/frozen.
3. Start Green prod web + worker:
   ```bash
   sudo systemctl start degen-web.service
   sudo systemctl start degen-worker.service
   curl -fsS http://127.0.0.1:8000/health
   ```
4. Switch Cloudflare/prod route to Green.
5. Confirm public smoke on `https://ops.degencollectibles.com/health` and key authenticated/media routes.
6. Move GitHub Actions deploy routing to Green (`runs-on: [self-hosted, degen-prod]`).
7. Remove or relabel Machine B runner so it cannot receive prod deploy jobs.

## Rollback

If Green fails before database writes have diverged:

```bash
sudo systemctl stop degen-worker.service
sudo systemctl stop degen-web.service
```

Then on Machine B:

```powershell
schtasks /run /tn DegenParser
```

Switch Cloudflare/prod route back to Machine B and verify `/health`.
