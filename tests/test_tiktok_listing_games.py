import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.tiktok import listing_games


def test_catalog_filters_supplies_caches_and_preserves_known_choices_on_failure(monkeypatch):
    monkeypatch.setattr(listing_games, '_cache', [])
    monkeypatch.setattr(listing_games, '_expires', 0)
    rows = [None, 'invalid', {'id': 86, 'name': 'Gundam Card Game'}, {'id': 31, 'name': 'Card Sleeves'}, {'id': '../3', 'name': 'Invalid'}]
    response = httpx.Response(200, json={'categories': rows}, request=httpx.Request('GET', listing_games.CATALOG_URL))
    with patch.object(listing_games.httpx, 'Client') as http:
        http.return_value.__enter__.return_value.get.return_value = response
        first, warning = listing_games.catalog()
        second, _ = listing_games.catalog()
        assert first == second == [{'id': 'catalog:86', 'name': 'Gundam Card Game', 'category_ids': ['86']}]
        assert not warning
        http.return_value.__enter__.return_value.get.assert_called_once()
    monkeypatch.setattr(listing_games, '_expires', 0)
    with patch.object(listing_games.httpx, 'Client', side_effect=httpx.ConnectError('unavailable')):
        stale, warning = listing_games.catalog()
        assert stale == first
        assert 'unavailable' in warning
        assert listing_games.resolve('Riftbound')['category_ids'] == ['89']


@pytest.mark.parametrize('rows', [None, {}, 'invalid', [None]])
def test_malformed_catalog_exposes_manual_fallback(monkeypatch, rows):
    monkeypatch.setattr(listing_games, '_cache', [])
    monkeypatch.setattr(listing_games, '_expires', 0)
    response = httpx.Response(200, json={'categories': rows}, request=httpx.Request('GET', listing_games.CATALOG_URL))
    with patch.object(listing_games.httpx, 'Client') as http:
        http.return_value.__enter__.return_value.get.return_value = response
        games, warning = listing_games.catalog()
    assert games == []
    assert 'manually' in warning


@pytest.mark.parametrize('game,category', [('Dragon Ball Super: Fusion World', '80'), ('Riftbound', '89'), ('Gundam Card Game', '86')])
def test_real_sealed_search_pipeline_stays_in_requested_catalog(game, category):
    from app.inventory import routes
    requests = []

    def respond(request):
        requests.append(request.url.path)
        if request.url.path.endswith('/search'):
            data = {'sets': [{'id': 9, 'name': 'Origins'}]}
        elif request.url.path.endswith('/pricing'):
            data = {'prices': {}}
        else:
            data = {'products': [{'id': 123, 'name': 'Origins Booster Pack', 'image_url': 'https://cdn.tcgtracking.com/product/123.jpg'}]}
        return httpx.Response(200, json=data)

    original_client = httpx.AsyncClient
    def client(**kwargs):
        return original_client(transport=httpx.MockTransport(respond), **kwargs)

    with patch.object(routes.httpx, 'AsyncClient', side_effect=client), patch.object(
        routes, '_search_tcgtracking_sealed_catalog_products', new=AsyncMock(return_value=([], ''))
    ):
        products, warning = asyncio.run(routes._search_sealed_products('Origins booster pack', game=game, catalog_category_ids=(category,)))
    assert products and not warning
    assert all('/' + category + '/' in path for path in requests)
    assert all(p['category_id'] == category and p['game'] == game for p in products)
    assert all(p['kind'] == 'Booster Pack' for p in products)
