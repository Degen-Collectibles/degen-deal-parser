from pathlib import Path


def test_readonly_db_role_template_uses_placeholders_and_select_only_grants():
    template = Path("docs/ops/degen-ops-readonly-db-role.sql").read_text(encoding="utf-8")
    lowered = template.lower()

    assert "degen_ops_readonly_role" in lowered
    assert "degen_database_name" in lowered
    assert "replace_with_generated_password" in lowered
    assert "postgresql://" not in lowered
    assert "postgresql+psycopg://" not in lowered
    assert "grant select on all tables" in lowered
    assert "grant select on all sequences" in lowered
    assert "grant insert" not in lowered
    assert "grant update" not in lowered
    assert "grant delete" not in lowered
    assert "grant all" not in lowered
    assert "nosuperuser" in lowered
    assert "nocreatedb" in lowered
    assert "nocreaterole" in lowered
    assert "create table degen_ops_readonly_should_fail" in lowered


def test_team_rollout_prd_requires_approval_before_hosted_gateway():
    prd = Path("docs/ops/degen-ops-team-rollout-prd.md").read_text(encoding="utf-8")

    assert "Do not build a hosted MCP gateway until Jeffrey approves Option C" in prd
    assert "No production writes" in prd
    assert "No database migrations" in prd


def test_ops_docs_require_employee_default_for_missing_scope():
    prd = Path("docs/ops/degen-ops-team-rollout-prd.md").read_text(encoding="utf-8")
    pilot = Path("docs/ops/degen-ops-hermes-mcp-pilot.md").read_text(encoding="utf-8")
    prompt = Path("docs/ops/degen-ops-agent-instructions.md").read_text(encoding="utf-8")

    assert "Missing or blank scope defaults to `employee`" in prd
    assert "defaults to `employee` instead of `owner`" in pilot
    assert "treat the session as employee scope" in prompt


def test_ops_docs_describe_redacted_partner_scope():
    prd = Path("docs/ops/degen-ops-team-rollout-prd.md").read_text(encoding="utf-8")
    pilot = Path("docs/ops/degen-ops-hermes-mcp-pilot.md").read_text(encoding="utf-8")
    prompt = Path("docs/ops/degen-ops-agent-instructions.md").read_text(encoding="utf-8")

    assert "Partner scope can answer buy questions" in prd
    assert "partner: 6 tools" in pilot
    assert "Partner scope must not expose" in pilot
    assert "Partner scope must not reveal raw cash balances" in prompt


def test_ops_docs_include_topology_planner_before_team_rollout():
    prd = Path("docs/ops/degen-ops-team-rollout-prd.md").read_text(encoding="utf-8")
    pilot = Path("docs/ops/degen-ops-hermes-mcp-pilot.md").read_text(encoding="utf-8")

    assert "scripts/degen_ops_topology_plan.py" in prd
    assert "scripts\\degen_ops_local_gate.py --json" in prd
    assert "scripts\\degen_ops_local_gate.py --json" in pilot
    assert "scripts\\degen_ops_completion_audit.py --json" in prd
    assert "scripts\\degen_ops_launch_checklist.py --audience partner --client hermes" in prd
    assert "goal_complete: false" in prd
    assert "scripts\\degen_ops_completion_audit.py --json" in pilot
    assert "scripts\\degen_ops_topology_plan.py --audience partner --client hermes --json" in prd
    assert "scripts\\degen_ops_green_pilot_packet.py --audience partner --client hermes" in prd
    assert "scripts\\degen_ops_launch_checklist.py --audience partner --client hermes --json" in pilot
    assert "scripts\\degen_ops_topology_plan.py --audience partner --client hermes --json" in pilot
    assert "scripts\\degen_ops_green_pilot_packet.py --audience partner --client hermes" in pilot
    assert "launch checklist aggregates readiness" in pilot
    assert "Green-hosted first pilot access" in pilot
    assert "Green-hosted pilot packet is the approval target" in pilot


def test_ops_docs_describe_scope_aware_chat_preflight():
    prd = Path("docs/ops/degen-ops-team-rollout-prd.md").read_text(encoding="utf-8")
    pilot = Path("docs/ops/degen-ops-hermes-mcp-pilot.md").read_text(encoding="utf-8")

    assert "Chat `--preflight --read-check` exercises every tool" in prd
    assert "Partner preflight must show 6 checks" in prd
    assert "scripts/degen_ops_answer_eval.py" in prd
    assert "docs\\ops\\degen-ops-answer-examples.json" in prd
    assert "docs\\ops\\degen-ops-answer-examples.json" in pilot
    assert "outputs\\degen-ops-answer-examples.json" not in prd
    assert "degen_ops_change_manifest.py --summary --json" in prd
    assert "degen_ops_change_manifest.py --summary --json" in pilot
    assert "Do not use `git add -A`" in prd
    assert "answer-quality eval" in pilot
    assert "`--read-check` calls each tool available in the selected scope" in pilot
