# Degen Ops Discord User Authorization PRD

## Problem

Degen Ops Bot currently works as a private, allowlisted partner-scope Discord bot. That is too manual for daily use. Employees, managers, partners, and owners should be able to message the same bot naturally from approved Discord channels, but the bot must only answer within the same access level they have in the Degen Ops site.

The critical risk is overexposure: Discord convenience must not leak owner cash, loans, payroll, labor financials, PII, protected employee status, or admin-only operational data to an employee or partner.

## Current State

- `scripts/degen_ops_discord_bot.py` supports:
  - channel/user allowlists
  - `DEGEN_OPS_DISCORD_ROLE_MAP` / `DEGEN_OPS_DISCORD_SCOPE_MAP_FILE`
  - per-user and per-channel scope resolution
  - short-term Discord context for follow-up questions
  - JSONL audit logs
- `app/ops_mcp.py` and `app/ops_chat.py` support scoped tools:
  - `owner`
  - `partner`
  - `employee`
  - `tiktok`
- The employee portal already has:
  - `User.role`
  - `EmployeeProfile`
  - `RolePermission`
  - `/team/admin/employees`
  - audit logging for sensitive employee operations
- `EmployeeProfile` does not currently store Discord identity fields.
- The bot does not currently resolve Discord users from the production employee table.

## Success Criteria

- Admins can add a Discord user ID and display handle to an employee profile.
- A Discord user can message the bot in approved channels without manual env allowlist edits.
- The bot denies unlinked, inactive, or unauthorized Discord users by default.
- The bot derives scope from the linked portal user and role/permissions, not from untrusted Discord role text.
- Employees cannot access owner/partner-only finance, cash, loan, payroll, PII, or admin-only employee data.
- Managers get only the extra operations/team access explicitly allowed by portal permissions.
- Partners remain partner-scoped and do not receive raw owner cash or owner loan detail.
- Every Discord prompt/answer audit entry includes the linked app user ID, role, effective scope, and scope reason.
- Existing legacy env allowlists keep working as a fallback during migration.
- No money movement, no inventory mutations, no listing changes, no customer messages, and no Discord management writes are added.

## Scope

1. Add Discord identity fields to employee profiles.
2. Expose those fields on `/team/admin/employees` and employee detail.
3. Add a DB-backed Discord authorization resolver for the bot.
4. Map app roles/permissions to Degen Ops bot scopes.
5. Add guardrail/denial messaging for unauthorized or out-of-scope requests.
6. Add audit evidence to bot logs.
7. Add tests for model migration, UI save behavior, scope resolution, and forbidden tool access.

## Non-Scope

- No automatic Discord server role sync in this phase.
- No automatic channel/category creation for everyone.
- No DMs until we explicitly approve DM access.
- No persistent auto-learning memory that writes facts without owner approval.
- No employee self-service Discord linking yet.
- No payroll, PII, money movement, listing, inventory, or customer-message actions.

## Constraints

- Read-only bot tools only.
- Deny by default.
- Production rollout must preserve current `degen-ops-discord-bot.service`.
- Use `DEGEN_OPS_READONLY_DATABASE_URL` for the bot.
- Keep Green rollout reversible by restarting only the bot service.
- Preserve existing partner bot behavior while migrating.

## Proposed Access Model

Discord identity is linked to `EmployeeProfile`:

- `discord_user_id`: canonical numeric Discord user ID, unique when present.
- `discord_username`: human-readable handle/name for admin display and search.
- `discord_linked_at`: timestamp for auditability.
- `discord_linked_by_user_id`: admin who linked it.

Effective bot authorization:

1. If `DEGEN_OPS_DISCORD_DB_AUTH_ENABLED=true`, resolve `message.author.id` against `EmployeeProfile.discord_user_id`.
2. If no active linked user exists, deny.
3. If linked user exists, derive bot scope from app role and permissions.
4. Intersect the derived user scope with the channel scope, if configured.
5. If the channel is not approved for that scope, deny.
6. Fallback to legacy env allowlist only when DB auth is disabled or no DB mapping exists and `DEGEN_OPS_DISCORD_LEGACY_ALLOWLIST_FALLBACK=true`.

Default scope mapping:

| App role | Default bot scope | Notes |
|---|---|---|
| `employee` | `employee` | Inventory, sales, TikTok product sales, price lookup, web search. No finance/cash/loan/employee status. |
| `viewer` | `employee` | Same as employee unless explicit permission adds more later. |
| `manager` | `manager` | Team operations status allowed, but no raw cash, loan, payroll dollars, or PII. |
| `reviewer` | `employee` | Reviewer does not get partner or owner bot access by default. |
| `admin` | `owner` | Full read-only owner scope. |

Permission refinements:

- `admin.labor_financials.view` can unlock manager labor-cost summaries only after a separate tool exists.
- `admin.employees.view` can unlock manager-safe employee status. For this launch, managers may ask whether employees are clocked in/out and get non-PII operational status.
- `admin.payroll.view`, `admin.employees.reveal_pii`, and compensation fields are never exposed through Discord in this phase.

## Guardrails

Employee scope must refuse:

- raw cash balances
- loan balances/payback ledger detail
- payroll or compensation
- PII
- owner-only finance
- employee status for other employees unless a future self/manager tool explicitly supports it
- production mutations

Manager scope must refuse:

- raw cash balances
- loan balances/payback ledger detail
- payroll export
- compensation values
- PII
- production mutations

Partner scope must refuse:

- raw owner cash balances
- owner loan/payback detail
- employee PII/payroll/labor detail
- production mutations

Owner scope remains read-only but can see owner-only finance, cash, loan, and employee ops status.

## Plan

1. Add DB fields and migration support.
2. Add employee admin UI fields.
3. Add Discord identity resolver module.
4. Add `manager` Degen Ops scope or explicit manager-to-existing-scope mapping.
5. Wire the Discord bot to DB auth behind a feature flag.
6. Improve denial copy and audit logs.
7. Add tests.
8. Roll out on Green with DB auth enabled after verifying linked test users.

## Risks

- Incorrect Discord ID entry could grant access to the wrong person.
- Discord username can change, so ID must be canonical.
- Existing `DEGEN_OPS_DISCORD_ALLOWED_USER_IDS` fallback could keep access broader than intended if not disabled after migration.
- Manager scope is currently not a first-class MCP scope, so adding it touches scope/tool tests.
- Existing local worktree may contain unrelated bookkeeping/ledger changes; staging must remain explicit.

## Verification

Local:

- `.\.venv\Scripts\python.exe -m compileall app scripts`
- Focused tests:
  - `tests/test_degen_ops_discord_auth.py`
  - `tests/test_degen_ops_discord_bot.py`
  - `tests/test_ops_mcp.py`
  - `tests/test_degen_ops_chat.py`
  - employee admin tests for profile update/search
- `scripts/degen_ops_local_gate.py --json`

Green:

- Verify deployed commit.
- Verify `DEGEN_OPS_READONLY_DATABASE_URL` present.
- Link one owner and one employee profile to Discord IDs.
- Enable `DEGEN_OPS_DISCORD_DB_AUTH_ENABLED=true`.
- Restart only `systemctl --user restart degen-ops-discord-bot.service`.
- Dry-run config must show DB auth enabled.
- Owner prompt can answer owner-safe question.
- Employee prompt can answer TikTok/product question.
- Employee prompt for raw cash/loan is refused.
- Audit log shows app user ID, role, and effective scope.

## Rollback

- Set `DEGEN_OPS_DISCORD_DB_AUTH_ENABLED=false`.
- Restart only `degen-ops-discord-bot.service`.
- Keep legacy env allowlist behavior intact.
- Database columns can remain unused; no data rollback required.

## Open Questions

1. Should `reviewer` map to `partner` or `owner` for bot access?
2. Should `manager` be allowed to ask about whether another employee is clocked in?
3. Should employees be allowed to DM the bot, or channel-only for launch? **Decision: allow DMs for linked active users only.**
4. Do partners live in the `User` table too, or should partner Discord IDs remain env/file mapped for now? **Decision: partners stay separate for this phase.**
5. Do we want employee self-linking later with a portal-generated verification code?
