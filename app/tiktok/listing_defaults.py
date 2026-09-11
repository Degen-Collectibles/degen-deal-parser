"""Evidence-based listing defaults. No inventory writes or AI-invented facts."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from html.parser import HTMLParser

import httpx

from . import listing_assistant as service

LANGUAGES = {'en': 'English', 'ja': 'Japanese', 'jp': 'Japanese', 'fr': 'French',
             'de': 'German', 'it': 'Italian', 'pt': 'Portuguese', 'es': 'Spanish',
             'ko': 'Korean', 'zh-Hans': 'Simplified Chinese', 'zh-Hant': 'Traditional Chinese'}
PACKAGE_KEYS = ('weight', 'length', 'width', 'height')
SHARED_ATTRIBUTES = {'type', 'ca prop 65: repro. chems', 'ca prop 65: carcinogens',
                     'dangerous goods or hazardous materials'}


class PlainText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style'}:
            self.skip += 1
        if tag in {'p', 'li', 'br', 'h2', 'h3'}:
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if tag in {'script', 'style'}:
            self.skip = max(0, self.skip - 1)
        if tag in {'p', 'li', 'h2', 'h3'}:
            self.parts.append('\n')

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)


def plain(value):
    parser = PlainText()
    parser.feed(str(value or '')[:30000])
    return '\n'.join(line.strip() for line in ''.join(parser.parts).splitlines() if line.strip())


def product_contents(value):
    text = plain(value)
    # Catalog introductions can be copied from another expansion. Prefer the
    # contents section when present; keep its wording and quantities unchanged.
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if re.search(r'\b(?:includes?|contains?)\b', line, re.I):
            return '\n'.join(lines[index:])
    return text


def language(product):
    explicit = str(product.get('language') or '')
    if explicit:
        return LANGUAGES.get(explicit, explicit)
    # These are separate, verified TCGPlayer English and Japanese catalogs.
    return {'3': 'English', '85': 'Japanese'}.get(str(product.get('category_id')), '')


def catalog_details(product):
    """Refresh only the selected catalog ID; retain provenance and exact unit."""
    from ..inventory.pricing import TCGTRACKING_BASE, TCGTRACKING_HEADERS
    category, set_id, product_id = (str(product.get(k) or '') for k in ('category_id', 'set_id', 'external_id'))
    if not all(v.isdigit() for v in (category, set_id, product_id)):
        return {}
    with httpx.Client(timeout=12, headers=TCGTRACKING_HEADERS) as client:
        response = client.get(f'{TCGTRACKING_BASE}/{category}/sets/{set_id}')
        response.raise_for_status()
        raw = next((p for p in response.json().get('products', []) if str(p.get('id')) == product_id), None)
        if not raw:
            return {}
        # Refresh price by exact ID; a low offer is not a market price.
        pricing = client.get(f'{TCGTRACKING_BASE}/{category}/sets/{set_id}/pricing')
        pricing.raise_for_status()
        from ..inventory.routes import _tcgtracking_market_price
        market = _tcgtracking_market_price(product_id, pricing.json().get('prices', {}))
        try:
            market = service.decimal_field(market, 'Market price', '999999.99')
        except ValueError:
            market = None
        product.update(market_price=market, market_price_source='TCGPlayer Market' if market is not None else '',
                       price_checked_at=datetime.now(timezone.utc).isoformat())
        options = {language(product)} - {''}
        for match in raw.get('cardtrader') or []:
            if str(match.get('tcg_player_id')) == product_id and match.get('match_confidence') == 100:
                options.update(LANGUAGES[x] for x in match.get('languages', []) if x in LANGUAGES)
        return {'description': product_contents((raw.get('ext_data') or {}).get('CardText'))[:6000],
                'language_options': sorted(options), 'catalog_name': raw.get('clean_name') or raw.get('name')}


def title_language(title):
    for pattern, name in ((r'\b(english|eng)\b', 'English'), (r'\b(japanese|jp|jpn)\b', 'Japanese'),
                          (r'\b(korean|kor)\b', 'Korean'), (r'\b(french)\b', 'French'),
                          (r'\b(german)\b', 'German'), (r'\b(spanish)\b', 'Spanish')):
        if re.search(pattern, title, re.I):
            return name
    return ''


def game_name(title):
    text = title.lower().replace('é', 'e')
    for word, game in (('pokemon', 'Pokemon'), ('magic', 'Magic'), ('one piece', 'One Piece'),
                       ('yu-gi-oh', 'Yu-Gi-Oh'), ('lorcana', 'Lorcana')):
        if word in text:
            return game
    return ''


def unit_key(name):
    from ..inventory.routes import _sealed_kind_from_name
    kind = _sealed_kind_from_name(name)
    # Catalog's broad Booster Box kind includes half boxes. Keep special units apart.
    normalized = name.lower().replace('é', 'e')
    # Search aliases intentionally group collector packs with boxes. Shipping
    # cannot: preserve the explicit unit independently of that broad classifier.
    units = tuple(re.findall(r'\b(?:box(?:es)?|packs?|display|blister|bundle|tin|deck|case)\b', normalized))
    modifiers = tuple(word for word in ('half', 'pokemon center', 'sleeved', 'deluxe', 'case', 'mini', 'jumbo') if word in normalized)
    number = r'(?:\d+|single|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand)'
    amount = number + r'(?:[\s-]+(?:and\s+)?' + number + r')*'
    qualifier = r'(?:(?:booster|collector|play|draft|set|sleeved)[\s-]+)*'
    counts = tuple(re.findall(r'\b' + amount + r'[\s-]*(?:[x×][\s-]*)?' + qualifier + r'(?:packs?|box(?:es)?|count|ct|tins?|decks?)\b|\bset of ' + amount + r'\b', normalized))
    return (kind, modifiers, counts, units)


def listing_mode(title):
    sealed = bool(re.search(r'ship(?:ped)?\s*sealed', title, re.I))
    rip = bool(re.search(r'live\s*rip', title, re.I))
    return 'both' if sealed and rip else 'sealed' if sealed else 'rip' if rip else ''


def comparable(product, fields, listing):
    title = str(listing.get('title') or '')
    game = str(product.get('game') or '').replace(' Japan', '')
    return (bool(game) and game_name(title) == game and bool(unit_key(product.get('name', ''))[0])
            and unit_key(title) == unit_key(product.get('name', ''))
            and bool(fields.get('language')) and title_language(title) == fields['language']
            and listing_mode(title) == service.fulfillment(fields)
            and str(listing.get('status') or '') in {'ACTIVATE', 'SELLER_DEACTIVATED'})


def rank_listings(product, fields, listings):
    tokens = set(re.findall(r'\w+', product.get('name', '').lower()))
    matches = [p for p in listings if comparable(product, fields, p)]
    return sorted(matches, key=lambda p: (-len(tokens & set(re.findall(r'\w+', p.get('title', '').lower()))), str(p.get('id', ''))))


def description(product, fields, details):
    # Same Product Details / fulfillment structure as the store's existing listings.
    # Contents come only from this exact catalog product, never a neighboring box.
    lines = [product.get('name') or fields.get('product_name') or '',
             'Language: ' + fields['language'] if fields.get('language') else '',
             'Set: ' + product['set_name'] if product.get('set_name') else '',
             details.get('description') or 'Includes one ' + (product.get('name') or 'product') + '.']
    return '\n\n'.join(x for x in lines if x)


def suggestions(product, fields, details, listing, metadata):
    """Return defaults separately so refreshes can preserve staff overrides."""
    proposed, sources = {}, {}
    lang = fields.get('language') or language(product)
    proposed['language'] = lang
    game = str(product.get('game') or '').replace(' Japan', '')
    label = 'Pokémon TCG' if game == 'Pokemon' else game
    proposed['title'] = ' — '.join(x for x in (label, product.get('name'), lang,
                          'Live Rip Only' if service.fulfillment(fields) == 'rip' else service.FULFILLMENT_LABELS[service.fulfillment(fields)]) if x)
    proposed['description'] = description(product, {**fields, 'language': lang}, details)
    proposed['image_heading'] = product.get('name') or fields.get('product_name', '')
    market = None
    if lang and product.get('market_price_source') == 'TCGPlayer Market' and lang == language(product):
        try:
            market = service.decimal_field(product.get('market_price'), 'Market price', '999999.99')
        except ValueError:
            pass
    proposed['price'] = market or ''
    if market:
        sources['price'] = {'label': 'TCGPlayer Market via TCGTracking', 'amount': market,
                            'looked_up_at': product.get('price_checked_at'),
                            'url': 'https://www.tcgplayer.com/product/' + str(product.get('external_id', '')),
                            'language': lang, 'product_name': product.get('name')}
    cats = {str(x['id']) for x in metadata.get('categories', [])}
    warehouses = {str(x['id']) for x in metadata.get('warehouses', [])}
    if listing and comparable(product, {**fields, 'language': lang}, listing):
        category = str(listing.get('category_id') or next((c['id'] for c in listing.get('category_chains', []) if c.get('is_leaf')), ''))
        if category in cats and category == str(metadata.get('category_id', category)):
            proposed['category_id'] = category
            proposed['brand_id'] = str((listing.get('brand') or {}).get('id') or '')
            available_attrs = {str(a['id']): a for a in metadata.get('attributes', [])}
            proposed['attributes'] = {
                str(a['id']): str(a['values'][0].get('name') or '')
                for a in listing.get('product_attributes', [])
                if a.get('values') and str(a['id']) in available_attrs
                and str(available_attrs[str(a['id'])].get('name', '')).lower() in SHARED_ATTRIBUTES
            }
        package = listing.get('package_dimensions') or {}
        weight = listing.get('package_weight') or {}
        if weight.get('unit') == 'POUND' and package.get('unit') == 'INCH':
            try:
                packed = {'weight': service.decimal_field(weight.get('value'), 'Packed weight', '150', 3)}
                packed.update({k: service.decimal_field(package.get(k), 'Packed ' + k, '120', 0) for k in PACKAGE_KEYS[1:]})
                proposed.update(packed)
            except ValueError:
                pass
        inventory = [i for sku in listing.get('skus', []) for i in sku.get('inventory', []) if str(i.get('warehouse_id')) in warehouses]
        stocked = {str(i['warehouse_id']) for i in inventory if int(i.get('quantity') or 0) > 0}
        all_ids = {str(i['warehouse_id']) for i in inventory}
        chosen = stocked if len(stocked) == 1 else all_ids
        if len(chosen) == 1:
            proposed['warehouse_id'] = next(iter(chosen))
        sources['shop'] = {'id': str(listing.get('id')), 'title': listing.get('title'),
                           'brand_name': (listing.get('brand') or {}).get('name'),
                           'looked_up_at': datetime.now(timezone.utc).isoformat()}
    if not proposed.get('warehouse_id') and len(warehouses) == 1:
        proposed['warehouse_id'] = next(iter(warehouses))
    # Fill catalog-dependent attributes, leaving unsupported required answers visible.
    attrs = proposed.setdefault('attributes', {})
    for attr in metadata.get('attributes', []):
        if str(attr.get('name', '')).lower() in {'language', 'card language'} and lang:
            attrs[str(attr['id'])] = lang
    return proposed, sources


def apply_defaults(draft, proposed, sources, details, warning=''):
    fields = draft['fields']
    previous = draft.get('defaults', {}).get('values', {})
    applied = dict(previous)
    for key, value in proposed.items():
        if not fields.get(key) or fields.get(key) == previous.get(key):
            fields[key] = value
            applied[key] = value
    # A failed lookup must not turn previously generated fields into apparent
    # staff overrides, otherwise a later successful refresh cannot update them.
    draft['defaults'] = {'values': applied, 'sources': sources, 'warning': warning}
    draft['language_options'] = details.get('language_options') or ([language(draft.get('selected_product', {}))] if language(draft.get('selected_product', {})) else [])
    fields['review_confirmed'] = False


def missing_fields(fields, metadata):
    required = {'language': 'Language / edition', 'price': 'Price', 'quantity': 'Quantity',
                'category_id': 'Category', 'warehouse_id': 'Dispatch warehouse',
                'weight': 'Packed weight', 'length': 'Packed length', 'width': 'Packed width', 'height': 'Packed height'}
    missing = [label for key, label in required.items() if fields.get(key) in (None, '')]
    for attr in metadata.get('attributes', []):
        if (attr.get('is_required') or attr.get('is_requried') or attr.get('requirement', {}).get('is_required')) and not fields.get('attributes', {}).get(str(attr['id'])):
            missing.append(str(attr.get('name') or attr['id']))
    return missing
