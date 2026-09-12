"""Shopify CLI web process adapter, DEV only. No secret from CLI env is consumed.

The parent provisions the private DEV secret and native-inputs.json in the named
handoff directory. Native inputs contain only verified dev Shop/customer IDs.
"""
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.loyalty_native import DEV_CLIENT, main, preview_host, samples

HANDOFF = Path('/tmp/degen-loyalty-native-handoff')


def native_arguments(inputs_path=HANDOFF / 'native-inputs.json'):
    try:
        with open(inputs_path, 'rb') as stream:
            raw = stream.read(4097)
        if len(raw) > 4096: raise ValueError()
        inputs = json.loads(raw)
        if not isinstance(inputs, dict) or set(inputs) - {'shop_id', 'sample_customers'}:
            raise ValueError()
        shop_id = inputs.get('shop_id')
        if not isinstance(shop_id, str): raise ValueError()
        mapping = inputs.get('sample_customers', {})
        if not isinstance(mapping, dict): raise ValueError()
        selected = samples([f'{customer}:{case}' for customer, case in mapping.items()])
    except (OSError, ValueError, TypeError):
        raise ValueError('Provide bounded native-inputs.json with verified dev shop_id and optional synthetic sample_customers') from None
    args = ['--secret-file', str(HANDOFF / 'dev-app-secret'), '--shop-id', shop_id,
            '--port', '8768', '--minutes', '30']
    for customer, case in selected.items():
        args += ['--sample', f'{customer}:{case}']
    return args


def cli_arguments(environment, inputs_path=HANDOFF / 'native-inputs.json'):
    if environment.get('SHOPIFY_API_KEY') != DEV_CLIENT or environment.get('PORT') != '8768':
        raise ValueError('Native web process requires the exact DEV client and port 8768')
    url = environment.get('APP_URL') or environment.get('HOST')
    if not url or not preview_host(url) or preview_host(url) == 'example.com':
        raise ValueError('A real temporary HTTPS preview origin is required')
    # This deliberately ignores SHOPIFY_API_SECRET and all other inherited keys.
    return native_arguments(inputs_path) + ['--public-url', url]


if __name__ == '__main__':
    try:
        args = cli_arguments(os.environ)
    except ValueError as error:
        raise SystemExit(str(error)) from None
    sys.argv = [sys.argv[0], *args]
    main()  # Clears inherited env/disables dotenv and network before app imports.
