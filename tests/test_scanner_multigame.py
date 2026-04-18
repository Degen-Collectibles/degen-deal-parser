"""Smoke-level regression test for multi-game scanner routing.

Doesn't hit live APIs — monkeypatches the underlying search functions and
verifies _lookup_candidates_by_category sends each category_id to the
right backend with the right keyword arguments.

For live-API smoke coverage, see scripts/smoke_test_degen_eye.py.
"""
import asyncio
import os
import sys
from dataclasses import asdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import pokemon_scanner as ps


def _sync(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


class _Recorder:
    """Monkeypatch target — captures positional+keyword call args."""
    def __init__(self, fake_result=None):
        self.calls = []
        self.fake_result = fake_result if fake_result is not None else []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.fake_result


def _patch_all(monkey):
    """Replace every backend searcher with a recorder."""
    stubs = {}
    for attr in (
        "_scryfall_search",
        "_ygoprodeck_search",
        "_optcg_search",
        "_lorcast_search",
        "_riftbound_search",
        "_tcgtracking_product_search",
        "lookup_candidates",
    ):
        stubs[attr] = _Recorder()
        monkey(attr, stubs[attr])
    return stubs


def test_routing_magic(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Bolt", set_name="4th", collector_number="208")
    _sync(ps._lookup_candidates_by_category(fields, "1"))
    assert len(stubs["_scryfall_search"].calls) == 1
    _args, kwargs = stubs["_scryfall_search"].calls[0]
    assert kwargs["name"] == "Bolt"
    assert kwargs["set_name"] == "4th"
    assert kwargs["number"] == "208"


def test_routing_one_piece_passes_number(monkeypatch):
    """OPTCG search must get the collector_number so downstream can post-filter."""
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Luffy", set_name="Romance Dawn", collector_number="OP01-003")
    _sync(ps._lookup_candidates_by_category(fields, "68"))
    _args, kwargs = stubs["_optcg_search"].calls[0]
    assert kwargs["number"] == "OP01-003"


def test_routing_riftbound(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Annie", set_name="Origins", collector_number="001/024")
    _sync(ps._lookup_candidates_by_category(fields, "89"))
    assert len(stubs["_riftbound_search"].calls) == 1
    # Generic TCGTracking fallback should NOT be hit for Riftbound.
    assert len(stubs["_tcgtracking_product_search"].calls) == 0


def test_routing_pokemon_uses_waterfall(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Pikachu", collector_number="25")
    _sync(ps._lookup_candidates_by_category(fields, "3"))
    assert len(stubs["lookup_candidates"].calls) == 1
    # And not any of the per-game branches.
    for attr in ("_scryfall_search", "_optcg_search", "_ygoprodeck_search",
                 "_lorcast_search", "_riftbound_search", "_tcgtracking_product_search"):
        assert len(stubs[attr].calls) == 0, f"{attr} should not be called for Pokemon"


def test_routing_unknown_falls_through_to_tcgtracking(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Whatever", set_name="Some Set")
    _sync(ps._lookup_candidates_by_category(fields, "57"))  # Digimon
    assert len(stubs["_tcgtracking_product_search"].calls) == 1
    _args, kwargs = stubs["_tcgtracking_product_search"].calls[0]
    assert kwargs["category_id"] == "57"


def test_category_game_map_includes_riftbound():
    assert ps._CATEGORY_TO_GAME.get("89") == "Riftbound"
    assert ps._VISION_GAME_TO_CATEGORY.get("riftbound") == "89"
    assert ps._XIMILAR_TAG_TO_CATEGORY.get("riftbound") == "89"


def test_vision_prompt_mentions_riftbound():
    assert "riftbound" in ps._VISION_IDENTIFY_PROMPT.lower()


def test_manual_fallback_covers_supported_games():
    ids = {c["id"] for c in ps._MANUAL_CATEGORY_FALLBACK}
    for must_have in ("3", "89", "1", "2", "68", "71"):
        assert must_have in ids, f"Manual fallback missing category {must_have}"
