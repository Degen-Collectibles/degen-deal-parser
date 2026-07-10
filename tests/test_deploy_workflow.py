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
