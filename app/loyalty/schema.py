"""Idempotent additive schema and an independent fail-closed readiness check."""
import re
from sqlalchemy import inspect, text, BigInteger, Integer, String
from sqlmodel import SQLModel
from .models import TABLES


def install_schema(engine):
    SQLModel.metadata.create_all(engine, tables=TABLES)
    with engine.begin() as connection:
        if engine.dialect.name == 'sqlite':
            for action in ('UPDATE','DELETE'):
                connection.execute(text(f"CREATE TRIGGER IF NOT EXISTS loyalty_ledger_no_{action.lower()} BEFORE {action} ON loyalty_ledger BEGIN SELECT RAISE(ABORT, 'loyalty_ledger_append_only'); END"))
        elif engine.dialect.name == 'postgresql':
            connection.execute(text("""CREATE OR REPLACE FUNCTION loyalty_ledger_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'loyalty_ledger_append_only'; END $$"""))
            # Serialize repeated installers without replacing an existing trigger.
            connection.execute(text('SELECT pg_advisory_xact_lock(7301202604)'))
            connection.execute(text("""DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='loyalty_ledger'::regclass AND tgname='loyalty_ledger_immutable') THEN CREATE TRIGGER loyalty_ledger_immutable BEFORE UPDATE OR DELETE ON loyalty_ledger FOR EACH ROW EXECUTE FUNCTION loyalty_ledger_immutable(); END IF; END $$"""))
        else:
            raise RuntimeError('loyalty_unsupported_database')
    if not schema_ready(engine):
        raise RuntimeError('loyalty_schema_not_ready')


def normalized_check(sql):
    # PostgreSQL deparses casts and IN lists differently from SQLite. Normalize
    # those known representations, then compare the entire expression.
    sql=str(sql).lower()
    sql=re.sub(r'cast\(\s*(\w+)\s+as\s+bigint\s*\)',r'\1',sql)
    sql=re.sub(r'::(?:character varying|bigint|text)(?:\[\])?', '', sql)
    sql=re.sub(r'[\s()"]','',sql)
    sql=sql.replace('=anyarray[','in').replace(']','')
    return sql


def schema_ready(engine):
    try:
        inspector = inspect(engine)
        for table in TABLES:
            columns = {v['name']:v for v in inspector.get_columns(table.name)}
            if any(c.name not in columns or (not c.nullable and columns[c.name]['nullable'] and not c.primary_key) for c in table.columns):
                return False
            for column in table.columns:
                actual=columns[column.name]['type'];expected=column.type
                if actual._type_affinity is not expected._type_affinity:
                    return False
                if isinstance(expected,BigInteger) and not isinstance(actual,BigInteger):
                    return False
                if isinstance(expected,String) and expected.length and actual.length and actual.length<expected.length:
                    return False
            if inspector.get_pk_constraint(table.name)['constrained_columns'] != [c.name for c in table.primary_key.columns]:
                return False
            indexes={v['name']:v for v in inspector.get_indexes(table.name)}
            for index in table.indexes:
                actual=indexes.get(index.name)
                if (not actual or actual['column_names'] != [c.name for c in index.columns]
                    or bool(actual['unique']) != bool(index.unique)
                    or any('where' in key and value is not None for key,value in actual.get('dialect_options',{}).items())):
                    return False
            unique = {v['name']:set(v['column_names']) for v in inspector.get_unique_constraints(table.name)}
            checks = {v['name']:normalized_check(v['sqltext']) for v in inspector.get_check_constraints(table.name)}
            for constraint in table.constraints:
                if constraint.__class__.__name__ == 'UniqueConstraint' and unique.get(constraint.name) != {c.name for c in constraint.columns}:
                    return False
                if constraint.__class__.__name__ == 'CheckConstraint' and checks.get(constraint.name) != normalized_check(constraint.sqltext):
                    return False
            fks = {(tuple(v['constrained_columns']),v['referred_table'],tuple(v['referred_columns'])) for v in inspector.get_foreign_keys(table.name)}
            for fk in table.foreign_key_constraints:
                expected = (tuple(c.name for c in fk.columns),fk.referred_table.name,tuple(e.column.name for e in fk.elements))
                if expected not in fks:
                    return False
        with engine.connect() as connection:
            if engine.dialect.name == 'sqlite':
                triggers=dict(connection.execute(text("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='loyalty_ledger'")).all())
                for action in ('UPDATE','DELETE'):
                    expected=f"CREATE TRIGGER loyalty_ledger_no_{action.lower()} BEFORE {action} ON loyalty_ledger BEGIN SELECT RAISE(ABORT, 'loyalty_ledger_append_only'); END"
                    actual=triggers.get('loyalty_ledger_no_'+action.lower(),'').replace('IF NOT EXISTS ','')
                    if normalized_check(actual)!=normalized_check(expected):return False
                return True
            if engine.dialect.name == 'postgresql':
                for table in TABLES:
                    if connection.execute(text("SELECT 1 FROM pg_index WHERE indrelid=to_regclass(:name) AND (NOT indisvalid OR NOT indisready)"),{'name':table.name}).first():return False
                    if connection.execute(text("SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass(:name) AND NOT convalidated"),{'name':table.name}).first():return False
                body=connection.execute(text("SELECT p.prosrc FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid='loyalty_ledger'::regclass AND t.tgname='loyalty_ledger_immutable' AND t.tgenabled='O' AND t.tgtype=27 AND t.tgqual IS NULL")).scalar()
                return body is not None and normalized_check(body)==normalized_check("BEGIN RAISE EXCEPTION 'loyalty_ledger_append_only'; END")
        return False
    except Exception:
        return False
