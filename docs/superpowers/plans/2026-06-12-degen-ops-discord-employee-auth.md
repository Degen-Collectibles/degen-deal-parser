# Degen Ops Discord Employee Auth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let linked, authorized Degen employees and partners talk to the Degen Ops Discord bot while receiving only the scope of answers their Degen Ops site role permits.

**Architecture:** Add Discord identity fields to `EmployeeProfile`, resolve Discord message authors against the app DB, derive an effective bot scope from active `User.role` plus channel scope, and keep legacy env allowlists as a temporary fallback. The bot remains read-only and uses the existing `app.ops_chat`/`app.ops_mcp` harness.

**Tech Stack:** Python, FastAPI, SQLModel, Jinja2, discord.py, existing Degen Ops MCP/chat harness, pytest.

---

### Task 1: Add Discord Identity Fields

**Files:**
- Modify: `app/models.py`
- Modify: `app/db.py`
- Test: `tests/test_degen_ops_discord_auth.py`

- [ ] **Step 1: Write the failing model/schema test**

```python
def test_employee_profile_has_discord_identity_fields():
    from app.models import EmployeeProfile

    profile = EmployeeProfile(
        user_id=1,
        discord_user_id="206237952412483584",
        discord_username="jeff",
    )

    assert profile.discord_user_id == "206237952412483584"
    assert profile.discord_username == "jeff"
```

- [ ] **Step 2: Run the failing test**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_degen_ops_discord_auth.py::test_employee_profile_has_discord_identity_fields -q`

Expected: FAIL because `EmployeeProfile` does not have the Discord fields yet.

- [ ] **Step 3: Add fields to `EmployeeProfile`**

Add these fields near `clockify_user_id`:

```python
    discord_user_id: Optional[str] = Field(default=None, index=True, unique=True)
    discord_username: Optional[str] = Field(default=None, index=True)
    discord_linked_at: Optional[datetime] = Field(default=None, index=True)
    discord_linked_by_user_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
```

- [ ] **Step 4: Add idempotent SQLite/Postgres migration entries**

Follow existing `app/db.py` column-add patterns. Add columns for:

```text
employeeprofile.discord_user_id
employeeprofile.discord_username
employeeprofile.discord_linked_at
employeeprofile.discord_linked_by_user_id
```

Add a unique index on `discord_user_id` where supported, or a normal indexed unique field through SQLModel if current migration helpers support it safely.

- [ ] **Step 5: Run the test**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_degen_ops_discord_auth.py::test_employee_profile_has_discord_identity_fields -q`

Expected: PASS.

### Task 2: Add Admin Employee UI Fields

**Files:**
- Modify: `app/routers/team_admin_employees.py`
- Modify: `app/templates/team/admin/employee_detail.html`
- Modify: `app/templates/team/admin/employees_list.html`
- Test: `tests/test_wave47_admin_tools.py` or a new focused employee admin test

- [ ] **Step 1: Write profile-update test**

```python
def test_admin_can_save_employee_discord_identity(client, session, admin_user, employee_user):
    response = client.post(
        f"/team/admin/employees/{employee_user.id}/profile-update",
        data={
            "csrf_token": "test",
            "role": "employee",
            "display_name": "Employee One",
            "staff_kind": "storefront",
            "discord_user_id": "111222333444555666",
            "discord_username": "employee.one",
        },
        follow_redirects=False,
    )

    assert response.status_code in (302, 303)
    profile = session.get(EmployeeProfile, employee_user.id)
    assert profile.discord_user_id == "111222333444555666"
    assert profile.discord_username == "employee.one"
```

Adjust fixture names to match the existing employee admin test harness.

- [ ] **Step 2: Run the failing test**

Run the focused test and confirm it fails because the route does not accept or save Discord fields.

- [ ] **Step 3: Extend `admin_employee_profile_update` form parameters**

Add:

```python
    discord_user_id: str = Form(default=""),
    discord_username: str = Form(default=""),
```

- [ ] **Step 4: Save normalized Discord fields**

Inside the `can_edit_profile` block:

```python
        discord_id = re.sub(r"\D+", "", discord_user_id or "")
        if discord_id != (profile.discord_user_id or ""):
            profile.discord_user_id = discord_id or None
            profile.discord_linked_at = now if discord_id else None
            profile.discord_linked_by_user_id = current.id if discord_id else None
            changed.append("discord_user_id")

        discord_name = (discord_username or "").strip().lstrip("@")
        if discord_name != (profile.discord_username or ""):
            profile.discord_username = discord_name or None
            changed.append("discord_username")
```

- [ ] **Step 5: Add employee detail inputs**

Add rows after `Clockify user ID`:

```html
<tr>
    <td style="padding:6px 0; color:var(--lx-muted);">Discord user ID</td>
    <td><input type="text" name="discord_user_id" value="{{ profile.discord_user_id or '' }}" {% if not can_edit_profile %}disabled{% endif %} style="width:320px; padding:5px 8px; border-radius:6px; border:1px solid var(--lx-br); background:var(--lx-s2); color:var(--lx-text);"/></td>
</tr>
<tr>
    <td style="padding:6px 0; color:var(--lx-muted);">Discord name</td>
    <td><input type="text" name="discord_username" value="{{ profile.discord_username or '' }}" {% if not can_edit_profile %}disabled{% endif %} style="width:320px; padding:5px 8px; border-radius:6px; border:1px solid var(--lx-br); background:var(--lx-s2); color:var(--lx-text);"/></td>
</tr>
```

- [ ] **Step 6: Add list display and search**

Show Discord name/ID in `employees_list.html`, and extend the employee search query in `team_admin_employees.py` to include `EmployeeProfile.discord_username` and `EmployeeProfile.discord_user_id`.

- [ ] **Step 7: Run focused employee admin tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_wave47_admin_tools.py tests/test_admin_employee_list_search.py -q`

Expected: PASS.

### Task 3: Add DB-Backed Discord Auth Resolver

**Files:**
- Create: `app/degen_ops_discord_auth.py`
- Test: `tests/test_degen_ops_discord_auth.py`

- [ ] **Step 1: Write resolver tests**

```python
def test_resolver_denies_unlinked_discord_user(session):
    from app.degen_ops_discord_auth import resolve_discord_author_scope

    result = resolve_discord_author_scope(
        session=session,
        discord_user_id="999",
        channel_id="chan",
        channel_scopes={"chan": "employee"},
    )

    assert result.allowed is False
    assert result.reason == "discord_user_not_linked"
```

```python
def test_resolver_maps_active_employee_to_employee_scope(session):
    from app.models import EmployeeProfile, User
    from app.degen_ops_discord_auth import resolve_discord_author_scope

    user = User(id=1, username="emp", password_hash="x", password_salt="s", role="employee", is_active=True)
    profile = EmployeeProfile(user_id=1, discord_user_id="111")
    session.add(user)
    session.add(profile)
    session.commit()

    result = resolve_discord_author_scope(
        session=session,
        discord_user_id="111",
        channel_id="chan",
        channel_scopes={"chan": "employee"},
    )

    assert result.allowed is True
    assert result.scope == "employee"
    assert result.app_user_id == 1
```

- [ ] **Step 2: Implement resolver dataclass**

```python
@dataclass(frozen=True)
class DiscordAuthorScope:
    allowed: bool
    scope: str | None
    reason: str
    app_user_id: int | None = None
    app_role: str = ""
    display_name: str = ""
```

- [ ] **Step 3: Implement role mapping**

```python
ROLE_TO_DEGEN_OPS_SCOPE = {
    "employee": "employee",
    "viewer": "employee",
    "manager": "manager",
    "reviewer": "employee",
    "admin": "owner",
}
```

- [ ] **Step 4: Implement channel/user intersection**

Use rank order:

```python
SCOPE_RANKS = {"employee": 0, "manager": 1, "partner": 2, "tiktok": 2, "owner": 3}
```

Effective scope is the lower-ranked scope between user and channel.

- [ ] **Step 5: Run resolver tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_degen_ops_discord_auth.py -q`

Expected: PASS.

### Task 4: Add Manager Scope To Degen Ops Harness

**Files:**
- Modify: `app/ops_mcp.py`
- Modify: `app/ops_chat.py`
- Test: `tests/test_ops_mcp.py`
- Test: `tests/test_degen_ops_chat.py`

- [ ] **Step 1: Write scope test**

```python
def test_manager_scope_excludes_owner_cash_and_loan_tools():
    from app.ops_mcp import DEGEN_OPS_SCOPE_TOOL_NAMES

    tools = set(DEGEN_OPS_SCOPE_TOOL_NAMES["manager"])
    assert "get_employee_clock_status" in tools
    assert "get_employee_ops_status" in tools
    assert "get_cash_snapshot" not in tools
    assert "get_loan_and_payback_snapshot" not in tools
```

- [ ] **Step 2: Add manager scope**

Add `manager` with:

```python
"manager": [
    "get_ops_agent_manifest",
    "get_ops_memory",
    "get_inventory_snapshot",
    "get_channel_velocity",
    "get_sales_summary",
    "get_discord_sales_summary",
    "get_tiktok_product_sales",
    "get_tiktok_top_products",
    "get_shopify_product_sales",
    "get_shopify_top_products",
    "get_price_lookup",
    "get_market_trend_lookup",
    "get_web_search",
    "get_employee_clock_status",
    "get_employee_ops_status",
],
```

- [ ] **Step 3: Ensure chat schema accepts manager**

Update any `_normalize_scope` choices/tests so `manager` is accepted.

- [ ] **Step 4: Run scope/chat tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_ops_mcp.py tests/test_degen_ops_chat.py -q`

Expected: PASS.

### Task 5: Wire Bot To DB Auth Behind Feature Flag

**Files:**
- Modify: `scripts/degen_ops_discord_bot.py`
- Test: `tests/test_degen_ops_discord_bot.py`

- [ ] **Step 1: Add config fields**

Add env vars:

```text
DEGEN_OPS_DISCORD_DB_AUTH_ENABLED=false
DEGEN_OPS_DISCORD_LEGACY_ALLOWLIST_FALLBACK=true
DEGEN_OPS_DISCORD_ALLOW_DMS=true
```

- [ ] **Step 2: Add bot config fields**

```python
    db_auth_enabled: bool = False
    legacy_allowlist_fallback: bool = True
```

- [ ] **Step 3: Write tests**

Test that with DB auth enabled:

- linked active employee is allowed
- inactive linked employee is denied
- unlinked user is denied
- employee in owner-only channel is reduced or denied based on configured channel policy
- legacy allowlist still works when fallback is enabled

- [ ] **Step 4: Call resolver in `determine_message_scope` path**

When DB auth is enabled, open a short session, resolve the author, and return `(scope, reason, identity_metadata)` or equivalent. Preserve existing function behavior for tests by wrapping richer metadata in a new helper instead of breaking every existing caller at once.

- [ ] **Step 5: Add denial copy**

Use:

```text
I can't answer from this Discord account yet. Ask an admin to link your Discord user ID on your employee profile.
```

For out-of-scope:

```text
I can help with Degen Ops questions in your access scope, but that request is restricted for your role.
```

- [ ] **Step 6: Run bot tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_degen_ops_discord_bot.py tests/test_degen_ops_discord_auth.py -q`

Expected: PASS.

### Task 6: Audit And Rollout Checks

**Files:**
- Modify: `scripts/degen_ops_discord_bot.py`
- Modify: `scripts/degen_ops_readiness.py`
- Modify: `scripts/degen_ops_launch_checklist.py`
- Test: `tests/test_degen_ops_readiness.py`

- [ ] **Step 1: Add audit fields**

Prompt and answer audit records should include:

```json
{
  "app_user_id": 123,
  "app_role": "employee",
  "scope": "employee",
  "scope_reason": "db_auth"
}
```

- [ ] **Step 2: Add dry-run config fields**

`--dry-run-config` should print:

```json
{
  "db_auth_enabled": true,
  "legacy_allowlist_fallback": true
}
```

- [ ] **Step 3: Add readiness checks**

Readiness should verify:

- DB auth code exists
- Discord identity fields exist
- manager scope exists
- legacy fallback can be disabled

- [ ] **Step 4: Run readiness tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_degen_ops_readiness.py -q`

Expected: PASS.

### Task 7: Verification And Commit

**Files:**
- All intended files only.

- [ ] **Step 1: Compile**

Run: `.\.venv\Scripts\python.exe -m compileall app scripts`

Expected: exit 0.

- [ ] **Step 2: Focused tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_degen_ops_discord_auth.py tests/test_degen_ops_discord_bot.py tests/test_ops_mcp.py tests/test_degen_ops_chat.py tests/test_wave47_admin_tools.py tests/test_admin_employee_list_search.py -q
```

Expected: PASS.

- [ ] **Step 3: Local gate**

Run: `.\.venv\Scripts\python.exe scripts\degen_ops_local_gate.py --json`

Expected: exit 0.

- [ ] **Step 4: Stage explicit files**

Run: `git status --short --branch`, then stage only intended files:

```powershell
git add -- app/models.py app/db.py app/degen_ops_discord_auth.py app/ops_mcp.py app/ops_chat.py scripts/degen_ops_discord_bot.py scripts/degen_ops_readiness.py scripts/degen_ops_launch_checklist.py app/routers/team_admin_employees.py app/templates/team/admin/employee_detail.html app/templates/team/admin/employees_list.html tests/test_degen_ops_discord_auth.py tests/test_degen_ops_discord_bot.py tests/test_ops_mcp.py tests/test_degen_ops_chat.py tests/test_degen_ops_readiness.py tests/test_wave47_admin_tools.py tests/test_admin_employee_list_search.py docs/ops/degen-ops-discord-employee-auth-prd.md docs/superpowers/plans/2026-06-12-degen-ops-discord-employee-auth.md
```

- [ ] **Step 5: Commit**

Run:

```powershell
git diff --cached --check
git commit -m "Authorize Degen Ops Discord users from employee profiles"
```

### Task 8: Green Rollout

**Files:**
- No production file edits except approved env flag change.

- [ ] **Step 1: Push**

Run: `git push origin main`

- [ ] **Step 2: Verify deploy**

On Green, verify `/opt/degen/app` is on the pushed commit.

- [ ] **Step 3: Link pilot users**

Use the employee admin page to link:

- Jeffrey owner/admin Discord ID
- one employee Discord ID

- [ ] **Step 4: Enable DB auth**

Set in `/opt/degen/degen-ops-discord-bot.env`:

```text
DEGEN_OPS_DISCORD_DB_AUTH_ENABLED=true
DEGEN_OPS_DISCORD_LEGACY_ALLOWLIST_FALLBACK=true
```

- [ ] **Step 5: Restart only bot**

Run:

```bash
systemctl --user restart degen-ops-discord-bot.service
systemctl --user show degen-ops-discord-bot.service -p ActiveState -p SubState -p MainPID -p NRestarts
```

- [ ] **Step 6: Discord smoke**

Owner asks:

```text
How much money have we made today?
```

Employee asks:

```text
How many 151 packs have we sold in the last seven days on TikTok?
```

Employee asks forbidden:

```text
What is our cash balance and loan balance?
```

Expected:

- owner gets owner-scope answer
- employee gets TikTok sales answer
- employee gets refusal for cash/loan
- audit log shows linked `app_user_id`, `app_role`, `scope`, and `scope_reason`

---

## Self-Review

Spec coverage: The plan covers Discord identity, employee admin UI, DB auth, role/scope mapping, guardrails, audit logging, tests, and Green rollout.

Placeholder scan: No `TBD`/`TODO` placeholders remain. Open questions are in the PRD, not execution placeholders.

Type consistency: The new model field names are consistent across model, route, template, resolver, tests, and rollout.
