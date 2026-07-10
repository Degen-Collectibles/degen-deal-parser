# Production Deploy Entrypoint Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every GitHub-driven production deployment open the deployment script from the triggering commit and keep that script pinned to the same SHA through service restart.

**Architecture:** GitHub Actions first synchronizes `/opt/degen/app` to `$GITHUB_SHA` with fail-closed branch, cleanliness, fast-forward, and equality guards. It then invokes `redeploy-linux.sh` with `DEGEN_EXPECTED_GIT_SHA`; the script verifies the expected SHA and skips repository synchronization in workflow mode while retaining the current fetch/pull path for manual runs.

**Tech Stack:** GitHub Actions YAML, Bash, Git, Python 3.14/pytest static contract tests, systemd-backed Green production deployment.

## Global Constraints

- Production checkout changes and service restarts remain auditable through GitHub Actions.
- Never use `git reset --hard`, force checkout, or commands that discard tracked production work.
- Untracked production operational files remain allowed and untouched.
- Workflow mode deploys exactly `$GITHUB_SHA`; manual mode retains `git fetch origin main` and `git pull --rebase origin main`.
- Fail before dependency installation, environment mutation, or service restart on branch, tracked-cleanliness, SHA-shape, fast-forward, or exact-SHA errors.
- Do not modify model selection, parser behavior, database schema, backup policy, systemd units, or unrelated deployment behavior.
- Run focused and full tests before publication; require a separate production preflight and explicit approval before merging.

## File Map

- Modify `.github/workflows/deploy.yml`: synchronize the production checkout before opening the deploy script and pass `$GITHUB_SHA` into the script.
- Modify `scripts/redeploy-linux.sh`: implement expected-SHA workflow mode and preserve manual fetch/pull mode.
- Create `tests/test_deploy_workflow.py`: enforce workflow synchronization, ordering, and invocation contracts.
- Modify `tests/test_redeploy_linux_script.py`: enforce expected-SHA and manual-mode script contracts.
- Modify `docs/green-autodeploy-cutover.md`: document the current Green two-stage deployment and rollback behavior.
- Existing `docs/superpowers/specs/2026-07-09-deploy-entrypoint-bootstrap-design.md`: authoritative approved design; do not duplicate or contradict it.

---

### Task 1: Add expected-SHA mode to the deployment script

**Files:**
- Modify: `tests/test_redeploy_linux_script.py`
- Modify: `scripts/redeploy-linux.sh:6-205`

**Interfaces:**
- Consumes: optional environment variable `DEGEN_EXPECTED_GIT_SHA`.
- Produces: `EXPECTED_GIT_SHA` shell value and two synchronization modes: pinned workflow mode and existing manual mode.

- [ ] **Step 1: Write failing contract tests**

Append these tests to `tests/test_redeploy_linux_script.py`:

```python
def test_redeploy_expected_sha_mode_is_fail_closed_before_deploy_mutations():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    assignment = 'EXPECTED_GIT_SHA="${DEGEN_EXPECTED_GIT_SHA:-}"'
    mode_start = 'if [[ -n "$EXPECTED_GIT_SHA" ]]; then'
    dirty_guard = 'if ! git diff --quiet || ! git diff --cached --quiet; then'
    install_start = 'if [[ "$INSTALL_DEPS" != "0" ]]; then'

    assert assignment in script
    assert mode_start in script
    assert 'if [[ ! "$EXPECTED_GIT_SHA" =~ ^[0-9a-f]{40}$ ]]; then' in script
    assert 'actual_sha="$(git rev-parse HEAD)"' in script
    assert 'if [[ "$actual_sha" != "$EXPECTED_GIT_SHA" ]]; then' in script
    assert dirty_guard in script
    assert script.index(dirty_guard) < script.index(mode_start)
    assert script.index(mode_start) < script.index(install_start)


def test_redeploy_expected_sha_mode_skips_sync_but_manual_mode_keeps_it():
    script = Path("scripts/redeploy-linux.sh").read_text(encoding="utf-8")

    block_start = script.index('if [[ -n "$EXPECTED_GIT_SHA" ]]; then')
    block_end = script.index('\nfi\n\nif [[ "$INSTALL_DEPS"', block_start)
    sync_block = script[block_start:block_end]
    expected_section, manual_section = sync_block.split("\nelse\n", 1)

    assert "git fetch origin main" not in expected_section
    assert "git pull --rebase origin main" not in expected_section
    assert 'log "Using workflow-synchronized checkout at $actual_sha"' in expected_section
    assert "git fetch origin main" in manual_section
    assert "git pull --rebase origin main" in manual_section
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_redeploy_linux_script.py -q
```

Expected: the two new tests fail because `EXPECTED_GIT_SHA` and the conditional synchronization block do not exist. Existing tests continue to pass.

- [ ] **Step 3: Implement the minimal expected-SHA mode**

Add this setting beside the existing deploy settings near the top of `scripts/redeploy-linux.sh`:

```bash
EXPECTED_GIT_SHA="${DEGEN_EXPECTED_GIT_SHA:-}"
```

Replace the current unconditional fetch/branch/pull section after `cd "$APP_DIR"` with:

```bash
log "Starting Linux redeploy in $APP_DIR"

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

if [[ -n "$EXPECTED_GIT_SHA" ]]; then
  if [[ ! "$EXPECTED_GIT_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: DEGEN_EXPECTED_GIT_SHA must be a 40-character lowercase Git SHA" >&2
    exit 7
  fi

  actual_sha="$(git rev-parse HEAD)"
  if [[ "$actual_sha" != "$EXPECTED_GIT_SHA" ]]; then
    echo "ERROR: expected checkout $EXPECTED_GIT_SHA, got $actual_sha" >&2
    exit 8
  fi
  log "Using workflow-synchronized checkout at $actual_sha"
else
  log "Fetching origin/main"
  git fetch origin main
  log "Rebasing onto origin/main"
  git pull --rebase origin main
fi
```

Do not change dependency installation, environment updates, service restarts, bot behavior, deploy stamp generation, or health polling.

- [ ] **Step 4: Run focused tests and Bash syntax validation**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_redeploy_linux_script.py -q
wsl.exe -e bash -n '/mnt/c/Users/jeffr/OneDrive/Apps/Documents/Degen App/.worktrees/deploy-entrypoint-bootstrap/scripts/redeploy-linux.sh'
```

Expected: all redeploy-script tests pass and `bash -n` exits 0 with no output.

- [ ] **Step 5: Inspect and commit only the script contract**

Run:

```powershell
git diff --check
git diff -- scripts/redeploy-linux.sh tests/test_redeploy_linux_script.py
git add -- scripts/redeploy-linux.sh tests/test_redeploy_linux_script.py
git diff --cached --check
git commit -m "fix: pin workflow deploys to expected sha"
```

Expected: one commit containing only the script and its focused tests.

### Task 2: Synchronize the production checkout before opening the script

**Files:**
- Create: `tests/test_deploy_workflow.py`
- Modify: `.github/workflows/deploy.yml`

**Interfaces:**
- Consumes: GitHub Actions environment variable `$GITHUB_SHA`, self-hosted runner checkout `/opt/degen/app`.
- Produces: a verified checkout at `$GITHUB_SHA` and invocation `DEGEN_EXPECTED_GIT_SHA="$GITHUB_SHA" ./scripts/redeploy-linux.sh`.

- [ ] **Step 1: Create the failing workflow contract test**

Create `tests/test_deploy_workflow.py` with:

```python
from pathlib import Path


WORKFLOW_PATH = Path(".github/workflows/deploy.yml")


def _workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def test_deploy_synchronizes_trigger_sha_before_opening_entrypoint():
    workflow = _workflow_text()

    sync_name = "- name: Synchronize production checkout"
    redeploy_name = "- name: Redeploy app"
    assert sync_name in workflow
    assert redeploy_name in workflow
    assert workflow.index(sync_name) < workflow.index(redeploy_name)

    sync_section = workflow[workflow.index(sync_name) : workflow.index(redeploy_name)]
    for required in (
        "set -euo pipefail",
        'current_branch="$(git rev-parse --abbrev-ref HEAD)"',
        'if [[ "$current_branch" != "main" ]]; then',
        "if ! git diff --quiet || ! git diff --cached --quiet; then",
        "git fetch origin main",
        'git merge --ff-only "$GITHUB_SHA"',
        'actual_sha="$(git rev-parse HEAD)"',
        'if [[ "$actual_sha" != "$GITHUB_SHA" ]]; then',
    ):
        assert required in sync_section

    assert "git reset --hard" not in sync_section
    assert "git checkout --force" not in sync_section
    assert "git pull" not in sync_section
    assert "git rebase" not in sync_section


def test_redeploy_entrypoint_receives_trigger_sha():
    workflow = _workflow_text()
    redeploy_section = workflow[workflow.index("- name: Redeploy app") :]

    assert 'DEGEN_EXPECTED_GIT_SHA="$GITHUB_SHA" ./scripts/redeploy-linux.sh' in redeploy_section
```

- [ ] **Step 2: Run the workflow test and verify RED**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_deploy_workflow.py -q
```

Expected: both tests fail because the synchronization step and SHA-bound invocation are absent.

- [ ] **Step 3: Add the fail-closed workflow bootstrap**

Replace the current workflow `steps` block with:

```yaml
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

- [ ] **Step 4: Run focused workflow and script tests**

Run:

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_deploy_workflow.py tests/test_redeploy_linux_script.py -q
```

Expected: all tests in both files pass.

- [ ] **Step 5: Inspect and commit the workflow unit**

Run:

```powershell
git diff --check
git diff -- .github/workflows/deploy.yml tests/test_deploy_workflow.py
git add -- .github/workflows/deploy.yml tests/test_deploy_workflow.py
git diff --cached --check
git commit -m "fix: sync production before deploy entrypoint"
```

Expected: one commit containing only the workflow and its contract test.

### Task 3: Update the Green deployment runbook

**Files:**
- Modify: `docs/green-autodeploy-cutover.md`

**Interfaces:**
- Consumes: the completed workflow and script contracts from Tasks 1 and 2.
- Produces: operator documentation for GitHub mode, manual mode, failure behavior, and rollback.

- [ ] **Step 1: Add a current-state section near the top of the runbook**

Insert this section after the title:

```markdown
## Current Green deployment contract (2026-07-10)

Green/Brev `openclaw-9902ae` is the active production host. GitHub Actions deploys pushes to `main` through two fail-closed stages:

1. `.github/workflows/deploy.yml` verifies branch `main`, rejects tracked checkout changes, fetches `origin/main`, fast-forwards `/opt/degen/app` to the triggering `$GITHUB_SHA`, and verifies exact equality.
2. The workflow invokes `scripts/redeploy-linux.sh` with `DEGEN_EXPECTED_GIT_SHA="$GITHUB_SHA"`. The script rechecks branch, tracked cleanliness, SHA shape, and exact `HEAD` before dependencies, environment updates, or service restarts. It does not fetch or pull in this mode.

Manual execution without `DEGEN_EXPECTED_GIT_SHA` retains the script's existing `git fetch origin main` and `git pull --rebase origin main` behavior.

Do not use `git reset --hard`, force checkout, or delete untracked operational files to unblock a deployment. Investigate tracked drift and resolve it through the canonical repository.
```

- [ ] **Step 2: Replace the obsolete workflow sample**

In the `Workflow patch to apply at cutover` section, replace the old `steps` sample with the synchronized workflow block implemented in Task 2. Add one sentence after the sample:

```markdown
The synchronization step must remain before `Redeploy app`; otherwise a commit that changes `redeploy-linux.sh` can execute the prior in-memory script on its first deployment.
```

- [ ] **Step 3: Verify documentation terms and diff**

Run:

```powershell
rg -n 'Current Green deployment contract|DEGEN_EXPECTED_GIT_SHA|Synchronize production checkout|prior in-memory script' docs/green-autodeploy-cutover.md
git diff --check
git diff -- docs/green-autodeploy-cutover.md
```

Expected: all four terms appear and the diff is limited to the Green workflow contract.

- [ ] **Step 4: Commit the runbook update**

Run:

```powershell
git add -- docs/green-autodeploy-cutover.md
git diff --cached --check
git commit -m "docs: document sha-pinned Green deploy"
```

### Task 4: Verify the complete branch and prepare publication

**Files:**
- Verify all files changed by Tasks 1-3 plus the design and this plan.

**Interfaces:**
- Consumes: completed local commits.
- Produces: evidence-backed publication preflight; no remote mutation until Jeffrey explicitly approves the exact push and PR targets.

- [ ] **Step 1: Run focused validation**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest tests/test_deploy_workflow.py tests/test_redeploy_linux_script.py -q
wsl.exe -e bash -n '/mnt/c/Users/jeffr/OneDrive/Apps/Documents/Degen App/.worktrees/deploy-entrypoint-bootstrap/scripts/redeploy-linux.sh'
```

Expected: all focused tests pass and Bash syntax exits 0.

- [ ] **Step 2: Run the canonical full suite**

```powershell
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' -m pytest --tb=short -q
```

Expected baseline: at least 3,596 passed, 85 skipped, 46 subtests passed, and zero failures. If repository changes legitimately add tests, the passed count increases.

- [ ] **Step 3: Audit branch scope**

```powershell
git fetch origin --prune
git status --short --branch
git diff --check origin/main...HEAD
git diff --stat origin/main...HEAD
git diff --name-status origin/main...HEAD
git log --oneline origin/main..HEAD
```

Expected changed paths:

```text
.github/workflows/deploy.yml
docs/green-autodeploy-cutover.md
docs/superpowers/plans/2026-07-10-brev-cli-reauth.md
docs/superpowers/plans/2026-07-10-deploy-entrypoint-bootstrap.md
docs/superpowers/specs/2026-07-09-deploy-entrypoint-bootstrap-design.md
scripts/redeploy-linux.sh
tests/test_deploy_workflow.py
tests/test_redeploy_linux_script.py
```

- [ ] **Step 4: Present the publication preflight and wait**

Present:

- Target branch: `origin/codex/deploy-entrypoint-bootstrap`.
- Pull-request base: current `origin/main`.
- Reversible actions: push branch and open draft PR; no production effect.
- Irreversible/external boundary: merging the PR triggers production synchronization, environment rewrite, and service restarts.
- Rollback: prefer a corrective commit or targeted content revert that preserves the hardened workflow/script; a full branch revert is separately approval-gated because it restores the legacy deploy entrypoint. No database rollback is required.

Wait for explicit approval before pushing.

- [ ] **Step 5: After approval, push and open a draft PR**

Create `.codex-deploy-bootstrap-pr-body.md` with `apply_patch` using this exact content:

```markdown
## Summary

- synchronize the Green production checkout to the triggering GitHub SHA before opening the deployment script
- pin workflow-driven script execution to that same SHA while preserving manual fetch/pull behavior
- fail before dependencies or service mutation on branch, tracked-drift, fast-forward, SHA-shape, or SHA-mismatch errors
- document and test the two-stage deployment contract

## Root cause

The previous workflow opened `redeploy-linux.sh` before that script pulled the triggering commit. A commit that changed the script therefore executed the prior in-memory script on its first deployment.

## Production impact

Merging this PR triggers the normal Green deployment. The new bootstrap fast-forwards `/opt/degen/app` to the merge SHA before invoking the synchronized script; the script then restarts the existing services and performs the existing health check.

## Validation

- focused deploy-workflow and redeploy-script tests
- Bash syntax validation
- canonical full repository test suite

## Rollback

Revert the merge through GitHub. No database or environment-schema rollback is required.
```

Then run:

```powershell
git push --set-upstream origin codex/deploy-entrypoint-bootstrap
gh pr create --repo Degen-Collectibles/degen-deal-parser --base main --head codex/deploy-entrypoint-bootstrap --draft --title "Harden production deploy entrypoint synchronization" --body-file '.codex-deploy-bootstrap-pr-body.md'
```

Delete `.codex-deploy-bootstrap-pr-body.md` with `apply_patch` immediately after PR creation and confirm it is absent from `git status`.

### Task 5: Review and production merge gate

**Files:**
- No new local files unless review feedback requires a focused fix.

**Interfaces:**
- Consumes: pushed PR with green required checks.
- Produces: a merge-ready PR, explicit production preflight, and verified single-attempt deployment.

- [ ] **Step 1: Mark the PR ready and request automated review**

```powershell
$prNumber = gh pr list --repo Degen-Collectibles/degen-deal-parser --head codex/deploy-entrypoint-bootstrap --state open --json number --jq '.[0].number'
if (-not $prNumber) { throw 'Open deploy-bootstrap PR not found' }
gh pr ready $prNumber --repo Degen-Collectibles/degen-deal-parser
gh pr comment $prNumber --repo Degen-Collectibles/degen-deal-parser --body '@coderabbitai review'
```

Inspect every finding against the live diff. Fix valid findings test-first, rerun focused tests, commit, and push; document technical reasons for rejected findings.

- [ ] **Step 2: Wait for required checks**

```powershell
$prNumber = gh pr list --repo Degen-Collectibles/degen-deal-parser --head codex/deploy-entrypoint-bootstrap --state open --json number --jq '.[0].number'
if (-not $prNumber) { throw 'Open deploy-bootstrap PR not found' }
gh pr checks $prNumber --repo Degen-Collectibles/degen-deal-parser --watch --interval 10
```

Expected: `test` and CodeRabbit succeed. Do not merge on pending or failing checks.

- [ ] **Step 3: Run read-only production preflight**

Verify through cached SSH/Brev access:

- `/opt/degen/app` is on `main` at current `origin/main` and has no tracked changes;
- web and worker services are active;
- localhost health returns 200;
- the latest scheduled database backup succeeded (no fresh backup is required because this change has no database or environment schema mutation);
- ECCN model and disabled-loop process settings remain active.

- [ ] **Step 4: Present the production mutation gate and wait**

State that merging will:

- fast-forward `/opt/degen/app` to the merge SHA in the new workflow step;
- invoke the newly synchronized script with `DEGEN_EXPECTED_GIT_SHA`;
- reinstall requirements as currently configured;
- rewrite the existing approved environment keys;
- restart web, worker, and configured bot services; and
- run health verification.

Preferred rollback is a corrective commit or targeted content revert that preserves the hardened workflow/script, followed by the same synchronized exact-SHA deployment. A full revert of the deploy-hardening branch instead restores the legacy entrypoint, is not SHA-pinned, and requires separate explicit approval, a Green/Brev preflight, and post-deploy verification. Wait for explicit merge approval.

- [ ] **Step 5: Merge and monitor both workflows**

```powershell
$prNumber = gh pr list --repo Degen-Collectibles/degen-deal-parser --head codex/deploy-entrypoint-bootstrap --state open --json number --jq '.[0].number'
if (-not $prNumber) { throw 'Open deploy-bootstrap PR not found' }
gh pr merge $prNumber --repo Degen-Collectibles/degen-deal-parser --merge
$mergeSha = gh pr view $prNumber --repo Degen-Collectibles/degen-deal-parser --json mergeCommit --jq '.mergeCommit.oid'
$runs = gh run list --repo Degen-Collectibles/degen-deal-parser --branch main --limit 10 --json databaseId,workflowName,status,conclusion,headSha,url | ConvertFrom-Json
$deployRun = $runs | Where-Object { $_.headSha -eq $mergeSha -and $_.workflowName -eq 'Deploy to Degen Prod' } | Select-Object -First 1
$testRun = $runs | Where-Object { $_.headSha -eq $mergeSha -and $_.workflowName -eq 'Tests' } | Select-Object -First 1
if (-not $deployRun -or -not $testRun) { throw 'Matching deploy/test runs not found' }
gh run watch $deployRun.databaseId --repo Degen-Collectibles/degen-deal-parser --interval 10 --exit-status
gh run watch $testRun.databaseId --repo Degen-Collectibles/degen-deal-parser --interval 10 --exit-status
```

The deploy must succeed on its first attempt; do not use rerun as normal completion.

- [ ] **Step 6: Verify production**

Confirm:

- production `HEAD` and `logs/deploy.stamp` equal the merge SHA;
- GitHub logs show `Synchronize production checkout` before `Redeploy app`;
- deploy logs show `Using workflow-synchronized checkout at` followed by the actual merge SHA, with no fetch/pull after the entrypoint opens;
- web and worker are active with zero unexpected restarts;
- health is 200;
- post-restart severe log count is zero; and
- live process values remain `NVIDIA_MODEL=us/azure/openai/eccn-gpt-5.5`, `NVIDIA_FAST_MODEL=us/azure/openai/eccn-gpt-5.4-nano`, `PARSER_REPROCESS_ENABLED=false`, and `PERIODIC_STITCH_AUDIT_ENABLED=false`.

If the synchronization or expected-SHA contract fails, stop before additional production mutation and follow the rollback section of the approved design.
