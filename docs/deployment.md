# Deployment Notes

## Employee Portal Secrets

Set `EMPLOYEE_TOKEN_HMAC_KEY` to a high-entropy secret that is distinct from
`SESSION_SECRET`. Invite and password-reset lookup HMACs use this key so
rotating session cookies does not invalidate outstanding employee links.

Rotation procedure:

1. Set the new `SESSION_SECRET` and leave `EMPLOYEE_TOKEN_HMAC_KEY` unchanged.
2. Restart the app and verify employee login, invite, and reset flows.
3. Rotate `EMPLOYEE_TOKEN_HMAC_KEY` only after all outstanding invite/reset
   links have expired or been re-issued.

On boot, legacy users with empty `password_salt` are migrated to an explicit
stored salt before password verification drops the old `SESSION_SECRET`
fallback.
