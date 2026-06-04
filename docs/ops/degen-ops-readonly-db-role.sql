-- Degen Ops MCP read-only database role template.
--
-- TEMPLATE ONLY. Do not run this against production without approval.
-- Replace the uppercase placeholders before use, and store the password in a
-- secret manager or local env var such as DEGEN_OPS_READONLY_DATABASE_URL.

BEGIN;

CREATE ROLE DEGEN_OPS_READONLY_ROLE
    LOGIN
    PASSWORD 'REPLACE_WITH_GENERATED_PASSWORD'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

GRANT CONNECT ON DATABASE DEGEN_DATABASE_NAME TO DEGEN_OPS_READONLY_ROLE;
GRANT USAGE ON SCHEMA public TO DEGEN_OPS_READONLY_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO DEGEN_OPS_READONLY_ROLE;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO DEGEN_OPS_READONLY_ROLE;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO DEGEN_OPS_READONLY_ROLE;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO DEGEN_OPS_READONLY_ROLE;

COMMIT;

-- Verification after connecting as DEGEN_OPS_READONLY_ROLE:
--
-- BEGIN READ ONLY;
-- SELECT current_database(), current_user;
-- SELECT COUNT(*) FROM inventory_items;
-- ROLLBACK;
--
-- Negative verification should fail:
--
-- CREATE TABLE degen_ops_readonly_should_fail(id integer);
