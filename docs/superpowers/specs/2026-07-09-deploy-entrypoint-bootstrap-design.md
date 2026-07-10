# Production Deploy Entrypoint Bootstrap Design

Date: 2026-07-09
Status: Approved for planning

## Problem

The production GitHub Actions workflow runs from `/opt/degen/app` and currently invokes `./scripts/redeploy-linux.sh` immediately. That script updates the production checkout only after Bash has already opened the script. When a commit changes `redeploy-linux.sh`, the first deployment pulls the new commit but continues executing the old in-memory script.

This happened while deploying token-efficiency PR #24. Production reached the correct merge SHA, but the first deployment applied the prior script's non-ECCN model default and did not write the two new disabled-loop settings. Rerunning the same workflow after the checkout had advanced opened the new script and applied the intended settings.

The deployment entrypoint must therefore update and verify the production checkout before opening the repository-owned deployment script.

The local Brev CLI also completes cached SSH commands but then reports that its API login is expired. That access should be refreshed without first discarding the still-working cached SSH path.

## Current State

- `.github/workflows/deploy.yml` has one deployment step: `./scripts/redeploy-linux.sh`.
- `scripts/redeploy-linux.sh` requires branch `main`, rejects tracked changes, fetches `origin/main`, and runs `git pull --rebase origin main` before dependencies, environment updates, service restarts, and health verification.
- The script's internal update is useful defense-in-depth, but it cannot change the already-running script body.
- Production is currently healthy on merge SHA `d2f3c1d85d691a0762cf9a1167ebfd6a2311417d` with ECCN settings active.
- `brev exec` still reaches `openclaw-9902ae` through cached SSH, but the CLI prints a logged-out warning after the command.

## Success Criteria

1. A push that changes `redeploy-linux.sh` executes the new script on the first deployment attempt.
2. The workflow fails before dependency installation, environment mutation, or service restart when the production checkout:
   - is not on branch `main`;
   - has tracked staged or unstaged changes;
   - cannot fast-forward to the triggering GitHub SHA; or
   - does not exactly equal the triggering GitHub SHA after synchronization.
3. An older queued workflow cannot deploy over a newer production checkout. A SHA mismatch must fail closed.
4. Existing untracked operational files do not block deployment, matching the current deployment contract.
5. Focused regression tests enforce synchronization ordering and the exact-SHA guard.
6. Brev authentication is refreshed without exposing credentials or logging out first, then verified with `brev ls` and one read-only command on `openclaw-9902ae`.

## Scope

### In scope

- Add a production-checkout synchronization step to `.github/workflows/deploy.yml` before `Redeploy app`.
- Add focused tests for the workflow bootstrap and ordering.
- Update the Green auto-deploy runbook with the two-stage deployment contract.
- Publish the change through a separate pull request.
- After the code rollout, refresh the local WSL Brev CLI login and verify read-only access.

### Out of scope

- Refactoring `scripts/redeploy-linux.sh` beyond what is required by review findings.
- Replacing the self-hosted runner or `/opt/degen/app` working-directory deployment model.
- Cleaning or reconciling Jeffrey's intentionally divergent local `main`.
- Deleting existing feature worktrees or operational files.
- Changing models, parser behavior, database schema, backup policy, or service units.
- Logging out of Brev before a replacement login is proven.

## Constraints

- Production checkout changes and service restarts must remain auditable through GitHub Actions.
- Production must never use `git reset --hard`, force checkout, or another command that silently discards tracked work.
- The workflow must be safe when pushes arrive close together.
- The existing deployment script remains the owner of dependency installation, environment-file updates, service restarts, bot restart behavior, and health polling.
- Tests must pass before commit and again in the pull request.
- Merging the workflow change itself triggers a production deployment and therefore requires a fresh production preflight and explicit merge approval.

## Approaches Considered

### 1. Workflow bootstrap before script invocation - selected

Add a `Synchronize production checkout` step before `Redeploy app`. The workflow runner performs the branch, cleanliness, fetch, fast-forward, and exact-SHA checks. The next workflow step then opens the script from the synchronized checkout.

Advantages:

- Fixes the failure at the correct ownership boundary: the workflow controls which deploy entrypoint it opens.
- Executes a changed deployment script correctly on the first attempt.
- Can bind the deployment to `$GITHUB_SHA` and fail stale runs safely.
- Leaves the script's existing pull as defense-in-depth.
- Produces clear GitHub Actions evidence before any service mutation.

Trade-off: the branch and cleanliness checks are duplicated between the workflow and the script. The duplication is intentional because they protect two different boundaries.

### 2. Make the deployment script re-execute itself after pulling

The script could compare its pre-pull and post-pull version, set a recursion guard, and `exec` the updated script.

This would solve the immediate symptom, but it makes a script responsible for replacing itself while running. It adds recursion and environment-marker complexity and leaves the workflow unaware of which script version initially opened. It is retained only as a fallback if workflow bootstrap proves infeasible.

### 3. Force-reset production or deploy from a temporary checkout

`git reset --hard origin/main` would be simple but violates the production no-destructive-cleanup contract. Moving deployment to a separate runner checkout would be a larger architectural change involving environment paths, permissions, and systemd expectations. Both are rejected for this narrow fix.

## Detailed Design

### Workflow synchronization step

`.github/workflows/deploy.yml` will add a step before `Redeploy app` with shell commands equivalent to:

```bash
set -euo pipefail

test "$(git rev-parse --abbrev-ref HEAD)" = "main"
git diff --quiet
git diff --cached --quiet
git fetch origin main
git merge --ff-only "$GITHUB_SHA"
test "$(git rev-parse HEAD)" = "$GITHUB_SHA"
```

Behavior:

1. The branch guard prevents deploying from a detached or unexpected branch.
2. The two diff checks reject tracked unstaged and staged changes without touching untracked operational files.
3. Fetching `origin/main` makes the triggering commit available locally.
4. `git merge --ff-only "$GITHUB_SHA"` advances production without rewriting history.
5. The final equality check rejects an older queued workflow when production is already ahead of that workflow's SHA.
6. Only after this step succeeds does the next workflow step open `./scripts/redeploy-linux.sh`.

The deployment script keeps its existing fetch, branch, cleanliness, and pull checks. In the normal path its pull becomes a no-op.

### Regression coverage

A focused test module will read `.github/workflows/deploy.yml` as text and assert:

- the synchronization step exists before the redeploy step;
- it enables `set -euo pipefail`;
- it checks branch `main`;
- it checks both staged and unstaged tracked changes;
- it fetches `origin main`;
- it uses `git merge --ff-only "$GITHUB_SHA"` rather than reset, force checkout, or rebase;
- it asserts final `HEAD` equality with `$GITHUB_SHA`; and
- the redeploy step still invokes `./scripts/redeploy-linux.sh`.

The existing redeploy-script tests remain unchanged unless implementation review exposes an actual gap.

### Runbook update

`docs/green-autodeploy-cutover.md` will explain the two-stage contract:

1. GitHub Actions synchronizes `/opt/degen/app` to the triggering SHA and fails closed on drift.
2. The synchronized `redeploy-linux.sh` performs the application deployment and health verification.

It will explicitly warn that deployment-script changes depend on the workflow bootstrap and that the script's internal pull is defense-in-depth, not the bootstrap mechanism.

### Brev re-authentication

The authentication repair is operational and does not change repository files:

1. Confirm cached SSH access still works with a read-only command.
2. Run `brev login` from WSL without calling `brev logout` or deleting CLI state.
3. If Brev emits a browser URL, device code, or other interactive approval, pause for Jeffrey to complete that step rather than copying credentials into chat.
4. Verify `brev ls` returns without a login prompt.
5. Verify `brev exec openclaw-9902ae` can run a read-only command without the post-command logged-out warning.

If login fails, stop and preserve the existing cached SSH path. Do not deregister the device or rotate unrelated credentials.

## Error Handling

- Every workflow guard exits nonzero before `Redeploy app` begins.
- A dirty or divergent checkout is reported as a deployment failure; the workflow does not clean it automatically.
- An exact-SHA mismatch is treated as a stale or out-of-order run and fails instead of deploying a different commit.
- Existing deployment-script health failures retain their current logs and GitHub Actions failure behavior.
- Brev authentication failures are reported without logging out or modifying production.

## Verification

### Before publishing

- Run the new focused workflow test.
- Run existing redeploy-script tests.
- Run `git diff --check`.
- Run the canonical full repository suite.

### Pull request

- Confirm the PR diff contains only the workflow, focused tests, runbook, design, and implementation plan.
- Require `Tests / test` to pass.
- Review automated feedback and resolve valid findings.

### Production rollout

- Preflight current production SHA, branch, tracked cleanliness, service state, and health.
- Merge only after explicit approval.
- Confirm the workflow synchronization step reports the merge SHA before `Redeploy app` starts.
- Confirm the deploy workflow succeeds without a manual rerun.
- Verify production `HEAD` and deploy stamp equal the merge SHA.
- Verify web and worker services are active, health is 200, and post-restart logs contain no severe errors.
- Reconfirm ECCN process settings remain unchanged.

### Brev

- `brev ls` succeeds without prompting for login.
- A read-only `brev exec openclaw-9902ae` command succeeds without the logged-out warning.

## Risks and Mitigations

- **Concurrent pushes:** An older run may start after a newer run. The exact-SHA assertion fails the older run instead of rolling production backward.
- **Tracked production drift:** The workflow fails before mutation and surfaces the drift for investigation.
- **Untracked operational files:** They remain allowed and untouched.
- **Workflow syntax error:** Focused tests plus GitHub's workflow parsing and PR checks catch this before merge.
- **Unexpected service restart from the follow-up merge:** The normal deploy still runs because the workflow changed on `main`. Preflight and post-deploy verification remain mandatory.
- **Brev interactive login:** The operation pauses for user approval instead of attempting to automate or expose credentials.

## Rollback

If the workflow bootstrap blocks valid deployments or behaves unexpectedly:

1. Revert the follow-up merge through GitHub.
2. Use the currently working cached SSH path for read-only diagnosis.
3. If an urgent deployment is required before the revert workflow can run, execute the already-synchronized production script only after verifying branch, tracked cleanliness, and intended SHA, with separate explicit approval.

No database or environment rollback is required because this change does not alter application data or model settings.

## Open Questions

None. The selected workflow-bootstrap design, fail-closed behavior, separate Brev credential flow, and production verification contract are fully specified.
