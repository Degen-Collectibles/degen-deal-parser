import importlib.util
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text


@pytest.mark.parametrize('note,flag,expected', [
    (None, True, [1]), ('edited note', True, [1]),
    ('[unresolved-discord-overlap] retained evidence', True, []),
    (None, False, []),
])
def test_read_only_rollback_gate_exit_and_no_writes(monkeypatch, capsys, note, flag, expected):
    spec = importlib.util.spec_from_file_location('overlap_gate', Path(__file__).parents[1] / 'scripts/check_overlap_rollback_safety.py')
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    engine = create_engine('sqlite:///:memory:')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE bank_transactions (id INTEGER PRIMARY KEY, discord_overlap_unresolved BOOLEAN, match_override_note TEXT)'))
        c.execute(text('INSERT INTO bank_transactions VALUES (1, :flag, :note)'), {'flag': flag, 'note': note})
    real_dispose = engine.dispose
    monkeypatch.setattr(engine, 'dispose', lambda: None)
    monkeypatch.setattr(gate, 'dotenv_values', lambda path: {'DATABASE_URL': 'unused'})
    def connect(url, **kwargs):
        assert 'default_transaction_read_only=on' in kwargs['connect_args']['options']
        return engine
    monkeypatch.setattr(gate, 'create_engine', connect)
    assert gate.main() == (2 if expected else 0)
    assert json.loads(capsys.readouterr().out)['blocking_bank_ids'] == expected
    with engine.connect() as c:
        assert tuple(c.execute(text('SELECT id, discord_overlap_unresolved, match_override_note FROM bank_transactions')).one()) == (1, flag, note)
    real_dispose()
