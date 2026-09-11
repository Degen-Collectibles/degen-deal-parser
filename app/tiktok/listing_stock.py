"""Conservative manual transfers between two fulfillment SKUs.

TikTok's API sets absolute available stock, not deltas. Transfers require a
non-selling listing and a fresh unchanged snapshot; unknown writes never retry.
"""
import hashlib
import json

PAUSED_STATUSES = {"DRAFT", "SELLER_DEACTIVATED"}


def snapshot(draft, product):
    if str(product.get('id') or product.get('product_id')) != str(draft.get('product_id')):
        raise ValueError('TikTok returned a different product. No stock was changed.')
    if product.get('status') not in PAUSED_STATUSES:
        raise ValueError('Pause this listing in TikTok Seller Center before transferring stock. Active sales can change quantities during a transfer.')
    warehouse = str(draft['fields']['warehouse_id'])
    rows = {}
    for option in ('sealed', 'rip'):
        seller_sku = 'DGN-LST-' + draft['id'] + '-' + option
        matches = [s for s in product.get('skus', []) if s.get('seller_sku') == seller_sku]
        if len(matches) != 1 or not matches[0].get('id'):
            raise ValueError('Could not uniquely identify both fulfillment SKUs.')
        sku = matches[0]
        inventories = sku.get('inventory', [])
        if len({str(i.get('warehouse_id')) for i in inventories}) != len(inventories):
            raise ValueError('TikTok returned duplicate warehouses.')
        entries = [i for i in inventories if str(i.get('warehouse_id')) == warehouse]
        if len(entries) != 1:
            raise ValueError('Both variants must have stock records in the selected dispatch warehouse.')
        entry = entries[0]
        quantity = entry.get('quantity')
        if type(quantity) is not int or not 0 <= quantity <= 99999:
            raise ValueError('TikTok did not return a supported available stock count.')
        if any(i.get('backorder_quantity', 0) for i in inventories):
            raise ValueError('Transfers with backorder inventory require manual review in Seller Center.')
        for i in inventories:
            if not i.get('warehouse_id') or type(i.get('quantity')) is not int or not 0 <= i['quantity'] <= 99999:
                raise ValueError('TikTok did not return supported stock counts for every warehouse.')
        rows[option] = {'sku_id':str(sku['id']), 'quantity':quantity,
                        'inventory':sorted([{'warehouse_id':str(i['warehouse_id']), 'quantity':i['quantity']} for i in inventories],key=lambda i:i['warehouse_id'])}
    value = {'product_id':draft['product_id'], 'status':product['status'], 'warehouse_id':warehouse, 'variants':rows}
    value['fingerprint'] = hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
    return value


def plan(current, amount):
    if type(amount) is not int or amount < 1:
        raise ValueError('Enter a positive whole number of units to move.')
    sealed = current['variants']['sealed']['quantity']
    rip = current['variants']['rip']['quantity']
    if amount > sealed or rip + amount > 99999:
        raise ValueError('The transfer exceeds available sealed stock or the Live Rip stock limit.')
    return {'before':{'sealed':sealed,'rip':rip}, 'after':{'sealed':sealed-amount,'rip':rip+amount}, 'amount':amount}


def update_payload(current, proposal):
    return {'skus':[{'id':current['variants'][option]['sku_id'], 'inventory':[
        {**i, 'quantity':proposal['after'][option] if i['warehouse_id']==current['warehouse_id'] else i['quantity']}
        for i in current['variants'][option]['inventory']]} for option in ('sealed','rip')]}


def matches_plan(current, proposal, fresh):
    expected = update_payload(current, proposal)
    return all(any(row['sku_id']==sku['id'] and row['inventory']==sku['inventory'] for row in fresh['variants'].values()) for sku in expected['skus'])
