"""Browser regressions using real listing HTML/JS/CSS and isolated API responses.

Run with a Playwright Chromium install; no app server or external requests needed.
"""
import copy
from pathlib import Path

import pytest
from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
BASE = '/tiktok/products/assistant'


@pytest.fixture(scope='module')
def browser():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        yield browser
        browser.close()


def sample_draft():
    return dict(id='test', version=1, status='draft', updated_at='2026-09-11T12:00:00Z',
        fields=dict(product_name='Destined Rivals Booster Box', language='English', price='100.00', price_mode='0',
                    quantity='10', fulfillment_mode='both', title='Test product', description='One box.', attributes={}),
        selected_product=dict(external_id='624679', name='Destined Rivals Booster Box'),
        assets=dict(source='/source.png'), missing_defaults=[],
        defaults=dict(sources=dict(price=dict(label='TCGPlayer Market via TCGTracking', amount='100.00',
            language='English', product_name='Destined Rivals Booster Box', looked_up_at='2026-09-11T12:00:00Z',
            url='https://www.tcgplayer.com/product/624679'))))


@pytest.fixture
def ui(browser):
    context = browser.new_context(viewport={'width':390, 'height':844})
    page = context.new_page()
    errors = []
    page.on('pageerror', lambda e: errors.append(str(e)))
    state = {'draft': sample_draft(), 'market': '100.00', 'late_edit': False,
             'games': [{'id': 'catalog:86', 'name': 'Gundam Card Game', 'category_ids': ['86']}],
             'game_warning': '', 'searches': []}
    # Navigation itself is outside this feature; the listing template is unchanged.
    env = Environment(loader=ChoiceLoader([DictLoader({'_linear_sidebar.html': ''}), FileSystemLoader(ROOT / 'app/templates')]))
    html = env.get_template('tiktok_listing_assistant.html').render(csrf_token='test-only')

    def respond(route):
        from urllib.parse import urlparse
        path = urlparse(route.request.url).path
        if path == '/':
            return route.fulfill(body=html, content_type='text/html')
        if path.startswith('/static/'):
            return route.fulfill(path=ROOT / 'app' / path.lstrip('/'))
        if path == '/source.png':
            return route.fulfill(status=204)
        if path == BASE + '/drafts':
            result = {'drafts':[{'id':'test', 'title':'Test draft', 'status':'draft'}]}
        elif path == BASE + '/packaging':
            result = {'presets':[]}
        elif path == BASE + '/games':
            result = {'games': state['games'], 'warning': state['game_warning']}
        elif path == BASE + '/drafts/test/search':
            request = route.request.post_data_json
            state['searches'].append(request)
            game = next((g for g in state['games'] if g['id'] == request['game']), {'id': request['game'], 'name': request['game']})
            state['draft'].update(search_game=game, search_query=request['query'], candidates=[])
            result = state['draft']
        elif path == BASE + '/drafts/test':
            result = state['draft']
        elif path == BASE + '/drafts/test/save':
            state['draft']['fields'].update(route.request.post_data_json['fields'])
            state['draft']['version'] += 1
            result = state['draft']
        elif path == BASE + '/drafts/test/select':
            state['draft']['fields'].update(product_name='Another Booster Box', price='', price_mode='')
            state['draft']['selected_product'].update(external_id='123', name='Another Booster Box')
            state['draft']['defaults']['sources'] = {}
            result = state['draft']
        elif path == BASE + '/drafts/test/autofill':
            # Backend retention is tested through actual endpoints separately.
            if state['draft']['defaults']['sources'].get('price'):
                state['draft']['defaults']['sources']['price']['amount'] = state['market']
            state['draft']['version'] += 1
            result = state['draft']
            if state['late_edit']:
                page.eval_on_selector('#listing-price', "el => {el.value='177.77'; el.dispatchEvent(new Event('input', {bubbles:true}));}")
        else:
            errors.append('Unexpected request: ' + path)
            return route.abort()
        return route.fulfill(json=copy.deepcopy(result))

    # Page-level fixtures take precedence over the isolation harness guard.
    # Every response is fulfilled locally or aborted; no network is permitted.
    page.route('**/*', respond)
    def open_draft():
        page.goto('http://listing.test/')
        page.locator('#draft-list button').click()
        expect(page.locator('#listing-price')).to_be_visible()
        expect(page.locator('#listing-price')).to_be_enabled()
    state['open'] = open_draft
    yield page, state
    assert not errors
    context.close()


def button(page, percent):
    return page.locator(f'[data-price-adjustment="{percent}"]')


@pytest.mark.parametrize('width', [390, 1440])
def test_other_game_search_persists_exact_game_and_product_query(ui, width):
    page, state = ui
    page.set_viewport_size({'width': width, 'height': 900})
    state['open']()
    page.locator('[data-step="1"]').click()
    page.locator('#search-game').select_option('other')
    expect(page.locator('#other-game-picker')).to_be_visible()
    page.get_by_label('Search for a game', exact=True).fill('Gundam')
    page.get_by_label('Search for a game', exact=True).press('Enter')
    expect(page.locator('#catalog-game')).to_have_value('catalog:86')
    page.locator('#search-query').fill('Newtype Rising booster pack')
    page.locator('#search-products').click()
    expect(page.locator('#search-products')).to_be_enabled()
    assert state['searches'][-1]['game'] == 'catalog:86'
    assert state['searches'][-1]['query'] == 'Newtype Rising booster pack'
    state['open']()
    page.locator('[data-step="1"]').click()
    expect(page.locator('#search-game')).to_have_value('other')
    expect(page.locator('#catalog-game')).to_have_value('catalog:86')
    expect(page.locator('#search-query')).to_have_value('Newtype Rising booster pack')
    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')


def test_unknown_game_and_catalog_outage_leave_manual_path_available(ui):
    page, state = ui
    state['open']()
    page.locator('[data-step="1"]').click()
    page.locator('#search-game').select_option('other')
    page.locator('#search-query').fill('Random booster pack')
    page.locator('#search-products').click()
    expect(page.locator('#notice')).to_contain_text('choose its catalog')
    assert not state['searches']
    state['games'] = []
    state['game_warning'] = 'Game catalog is unavailable right now.'
    page.locator('#game-query').fill('Random game')
    page.locator('#find-games').click()
    expect(page.locator('#game-search-status')).to_contain_text('unavailable')
    expect(page.locator('#game-search-status')).to_contain_text('enter details manually')
    expect(page.locator('#source')).to_be_enabled()


@pytest.mark.parametrize('game', ['Riftbound', 'Dragon Ball Super: Fusion World'])
def test_primary_games_are_direct_choices_and_use_exact_search_value(ui, game):
    page, state = ui
    state['open']()
    page.locator('[data-step="1"]').click()
    page.locator('#search-game').select_option(game)
    expect(page.locator('#other-game-picker')).to_be_hidden()
    page.locator('#search-query').fill('Origins booster pack')
    page.locator('#search-products').click()
    expect(page.locator('#search-products')).to_be_enabled()
    assert state['searches'][-1]['game'] == game


def test_unlisted_ai_game_is_visible_in_other_picker_not_pokemon(ui):
    page, state = ui
    state['draft']['identification'] = {'name': 'Gundam pack', 'game': 'Gundam Card Game', 'search_query': 'Newtype Rising pack', 'confidence': 'high', 'uncertainties': []}
    state['open']()
    page.locator('[data-step="1"]').click()
    expect(page.locator('#search-game')).to_have_value('other')
    expect(page.locator('#game-query')).to_have_value('Gundam Card Game')
    expect(page.locator('#search-query')).to_have_value('Newtype Rising pack')


def test_adjustments_use_market_not_current_price_and_manual_survives_reload(ui):
    page, state = ui
    state['open']()
    price = page.locator('#listing-price')
    expect(button(page, 0)).to_have_attribute('aria-pressed', 'true')
    for percent, expected in [(5,'105.00'), (10,'110.00'), (10,'110.00'), (5,'105.00'), (0,'100.00')]:
        button(page, percent).click()
        expect(price).to_have_value(expected)
    price.fill('123.45')
    expect(page.locator('#price-state')).to_contain_text('Custom price')
    expect(button(page, 0)).to_have_attribute('aria-pressed', 'false')
    page.locator('#save-draft').click()
    expect(page.locator('#save-draft')).to_be_enabled()
    state['open']()
    expect(price).to_have_value('123.45')
    expect(page.locator('#price-state')).to_contain_text('Custom price')
    # A manual price equal to market remains explicitly custom too.
    price.fill('100.00')
    page.locator('#save-draft').click()
    expect(page.locator('#save-draft')).to_be_enabled()
    state['open']()
    expect(button(page, 0)).to_have_attribute('aria-pressed', 'false')


def test_refresh_preserves_price_then_intentional_adjustment_uses_new_market(ui):
    page, state = ui
    state['open']()
    button(page, 5).click()
    page.locator('#edit-details summary').click()
    state['market'] = '200.00'
    page.locator('#refresh-defaults').click()
    expect(button(page, 5)).to_be_enabled()
    expect(page.locator('#market-reference')).to_contain_text('$200.00')
    expect(page.locator('#listing-price')).to_have_value('105.00')
    expect(page.locator('#price-state')).to_contain_text('Custom price')
    button(page, 10).focus()
    page.keyboard.press('Enter')
    expect(page.locator('#listing-price')).to_have_value('220.00')
    expect(page.locator('#allocation-summary')).to_contain_text('Live Rip at $220.00')
    page.locator('#edit-details summary').click()
    page.locator('[data-field="rip_price"]').fill('180.00')
    button(page, 5).click()
    expect(page.locator('#allocation-summary')).to_contain_text('Shipped Sealed at $210.00')
    expect(page.locator('#allocation-summary')).to_contain_text('Live Rip at $180.00')


def test_late_defaults_response_does_not_discard_unsaved_price(ui):
    page, state = ui
    state['open']()
    state['late_edit'] = True
    page.locator('#edit-details summary').click()
    page.locator('#refresh-defaults').click()
    expect(page.locator('#refresh-defaults')).to_be_enabled()
    expect(page.locator('#listing-price')).to_have_value('177.77')
    expect(page.locator('#price-state')).to_contain_text('Custom price')
    page.locator('#save-draft').click()
    expect(page.locator('#save-draft')).to_be_enabled()
    assert state['draft']['fields']['price'] == '177.77'


@pytest.mark.parametrize('fault', ['missing','zero','nan','wrong-language','wrong-product','unverified'])
def test_unverified_market_disables_adjustments_but_keeps_manual_price(ui, fault):
    page, state = ui
    source = state['draft']['defaults']['sources']['price']
    if fault == 'missing':
        state['draft']['defaults']['sources'] = {}
    else:
        key, value = {'zero':('amount','0'), 'nan':('amount','NaN'), 'wrong-language':('language','French'),
                      'wrong-product':('product_name','Other box'), 'unverified':('label','TCGPlayer Low')}[fault]
        source[key] = value
    state['draft']['fields']['price'] = ''
    state['open']()
    for percent in [0,5,10]:
        expect(button(page, percent)).to_be_disabled()
    expect(page.locator('#market-reference')).to_contain_text('enter a price manually')
    expect(page.locator('#market-reference')).not_to_contain_text('$0.00')
    page.locator('#listing-price').fill('89.99')
    page.locator('#save-draft').click()
    expect(page.locator('#save-draft')).to_be_enabled()
    for percent in [0,5,10]:
        expect(button(page, percent)).to_be_disabled()
    expect(page.locator('#listing-price')).to_have_value('89.99')


@pytest.mark.parametrize('width', [320,375,390,430,1280])
def test_price_placement_accessibility_and_expanded_details_fit(ui, width):
    page, state = ui
    page.set_viewport_size({'width':width, 'height':900})
    state['open']()
    price = page.get_by_label('Price (USD)', exact=True)
    expect(price).to_be_visible()
    assert page.locator('#edit-details [data-field="price"]').count() == 0
    assert page.locator('[data-field="quantity"]').evaluate('el => el.closest("label").nextElementSibling.className') == 'listing-price'
    for percent in [0,5,10]:
        bounds = button(page, percent).bounding_box()
        assert bounds['height'] >= 44
    page.locator('#edit-details summary').click()
    page.locator('#category').evaluate("el => {el.add(new Option('An extremely long category name '.repeat(20), 'test')); el.value='test';}")
    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
    assert price.evaluate('el => getComputedStyle(el).fontSize') == ('16px' if width <= 700 else '15px')
    for selector in ['#listing-price', '#category', '[data-field="weight"]', '#preset-name']:
        bounds = page.locator(selector).bounding_box()
        assert bounds and bounds['x'] >= 0 and bounds['x'] + bounds['width'] <= width


@pytest.mark.parametrize('market,expected5,expected10', [('434.63','456.36','478.09'), ('0.10','0.11','0.11')])
def test_adjustments_round_to_cents(ui, market, expected5, expected10):
    page, state = ui
    state['draft']['defaults']['sources']['price']['amount'] = market
    state['open']()
    button(page, 5).click()
    expect(page.locator('#listing-price')).to_have_value(expected5)
    button(page, 10).click()
    expect(page.locator('#listing-price')).to_have_value(expected10)


def test_new_product_does_not_inherit_unsaved_price(ui):
    page, state = ui
    state['draft']['candidates'] = [{'name':'Another Booster Box', 'image_url':'', 'external_id':'123'}]
    state['open']()
    page.locator('#listing-price').fill('177.77')
    # Select an already-returned candidate, exercising the actual select handler.
    page.locator('[data-panel="1"]').evaluate('el => el.hidden=false')
    page.locator('#candidates button').click()
    expect(page.locator('#listing-price')).to_be_enabled()
    expect(page.locator('#listing-price')).to_have_value('')
    expect(button(page, 5)).to_be_disabled()


def test_large_adjustment_cannot_exceed_price_limit(ui):
    page, state = ui
    state['draft']['defaults']['sources']['price']['amount'] = '999999.99'
    state['open']()
    expect(button(page, 0)).to_be_enabled()
    expect(button(page, 5)).to_be_disabled()
    expect(button(page, 10)).to_be_disabled()
    page.locator('#listing-price').fill('500.00')
    expect(page.locator('#listing-price')).to_have_value('500.00')
