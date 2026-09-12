"""Bounded, bearer-only DEV native preview. Never imported by the real app.

Requires an explicitly supplied private DEV app secret file and verified dev Shop
GID. No dotenv, inherited credentials, Shopify API, token mint or Ops routes.
"""
import argparse
import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DEV_SHOP = 'degen-loyalty-dev.myshopify.com'
DEV_CLIENT = '75c6c90117c95bd9f683cacaf3ed5c51'
CASES = {'earned': ([20], 'eligible'), 'refund': ([20, -1], 'eligible'),
         'pending': ([], 'pending'), 'review': ([12], 'review')}


def private_secret(path):
    """Read only the explicitly handed-off file; refuse links/shared permissions."""
    from pydantic import SecretStr
    message = 'DEV secret requires an owner-only regular file (0600), 32–512 bytes'
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                raise ValueError(message)
            value = stream.read(515)
            if value.endswith(b'\r\n'):
                value = value[:-2]
            elif value.endswith(b'\n'):
                value = value[:-1]
            if not 32 <= len(value) <= 512 or any(byte < 33 or byte > 126 for byte in value):
                raise ValueError(message)
            return SecretStr(value.decode('ascii'))
    except (OSError, UnicodeError):
        raise ValueError(message) from None


def preview_host(public_url):
    if not public_url:
        return None
    parsed = urlsplit(public_url)
    if (parsed.scheme != 'https' or parsed.username or parsed.password or parsed.port
            or parsed.path not in ('', '/') or parsed.query or parsed.fragment
            or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', parsed.hostname or '')
            or '.' not in parsed.hostname or parsed.hostname.endswith('.myshopify.com')):
        raise ValueError('Public URL must be the reviewed HTTPS tunnel origin without credentials/path/port')
    return parsed.hostname


def samples(values):
    result = {}
    if len(values) > 4:
        raise ValueError('At most four explicitly verified synthetic customer mappings')
    for value in values:
        customer, separator, case = value.partition(':')
        if (not separator or case not in CASES or customer in result
                or not re.fullmatch(r'[1-9][0-9]{0,15}', customer)
                or int(customer) > 9007199254740991):
            raise ValueError('Sample format: verified numeric synthetic customer ID:earned|refund|pending|review')
        result[customer] = case
    return result


def create_native(scratch, *, shop_id, secret, sample_customers=None, public_url=None, minutes=30):
    """Factory for isolated tests/launcher; caller installs process isolation first."""
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    from pydantic import SecretStr
    from sqlmodel import Session, create_engine
    from app.config import Settings
    from app.loyalty.models import LoyaltyAccount, LoyaltyEntitlement, LoyaltyLedger
    from app.loyalty.pos_auth import POSAccess
    from app.loyalty.schema import install_schema
    from app.routers.loyalty_pos import build_router

    if not re.fullmatch(r'gid://shopify/Shop/[1-9][0-9]{0,19}', shop_id or ''):
        raise ValueError('Verified DEV Shop GID is required')
    if not isinstance(secret, SecretStr) or not 1 <= minutes <= 60:
        raise ValueError('Private DEV secret and preview duration of 1–60 minutes required')
    host = preview_host(public_url)
    cases = samples([f'{customer}:{case}' for customer, case in (sample_customers or {}).items()])
    config = Settings(LOYALTY_SHOP_DOMAIN=DEV_SHOP, LOYALTY_SHOP_ID=shop_id,
                      LOYALTY_POS_CLIENT_ID=DEV_CLIENT, LOYALTY_POS_CLIENT_SECRET=secret,
                      LOYALTY_POS_READ_ENABLED=True, LOYALTY_POS_ALL_STAFF_ENABLED=True,
                      LOYALTY_RECEIVING_ENABLED=False, LOYALTY_PROCESSING_ENABLED=False,
                      LOYALTY_POSTING_ENABLED=False)
    POSAccess.from_settings(config)  # Fail before creating any DB or listener.
    database = Path(scratch) / 'native.db'
    # Never open an existing database; only this new scratch file can be seeded.
    fd = os.open(database, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    writer = create_engine('sqlite:///' + str(database))
    try:
        install_schema(writer)
        with Session(writer) as session:
            for number, (customer, case) in enumerate(cases.items(), 1):
                deltas, status = CASES[case]
                account = LoyaltyAccount(shop=DEV_SHOP, customer_id=f'gid://shopify/Customer/{customer}')
                session.add(account); session.flush()
                row = LoyaltyEntitlement(shop=DEV_SHOP, order_id=f'gid://shopify/Order/{number}',
                    account_id=account.id, customer_id=account.customer_id, status=status,
                    posted_points=sum(deltas), revision=len(deltas))
                session.add(row); session.flush()
                total = 0
                for revision, delta in enumerate(deltas, 1):
                    total += delta
                    session.add(LoyaltyLedger(shop=DEV_SHOP, account_id=account.id,
                        entitlement_id=row.id, revision=revision, business_key=f'native-synthetic-{number}-{revision}',
                        delta=delta, resulting_points=total, reason='refund' if delta < 0 else 'earned',
                        rule_version='synthetic-native-only', evidence_hash='a'*64, evidence_json='{}'))
            session.commit()
    finally:
        writer.dispose()
    # SQLite itself rejects writes after seeding, in addition to the read-only API.
    engine = create_engine('sqlite:///file:' + database.as_posix() + '?mode=ro&uri=true',
                           connect_args={'check_same_thread': False})
    deadline = time.monotonic() + minutes * 60

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            engine.dispose()

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.state.engine = engine
    app.include_router(build_router(engine, config))

    @app.middleware('http')
    async def exposure_bounds(request: Request, call_next):
        if time.monotonic() >= deadline:
            return JSONResponse({'error': 'preview_expired'}, status_code=503)
        request_host = request.headers.get('host', '')
        if not (re.fullmatch(r'127\.0\.0\.1(?::[0-9]{1,5})?', request_host)
                or (host and request_host == host)):
            return JSONResponse({'error': 'invalid_host'}, status_code=403)
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @app.get('/health', include_in_schema=False)
    async def health():
        return {'status': 'ok', 'mode': 'synthetic-native-preview'}

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--secret-file', type=Path, required=True)
    parser.add_argument('--shop-id', required=True, help='Verified GID of the named development shop')
    parser.add_argument('--sample', action='append', default=[], help='Verified synthetic customer ID:case (optional)')
    parser.add_argument('--public-url', help='Reviewed HTTPS tunnel origin; omit for local deny checks')
    parser.add_argument('--port', type=int, default=8768)
    parser.add_argument('--minutes', type=int, default=30)
    parser.add_argument('--check-only', action='store_true', help='Build scratch backend and run deny checks without binding')
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535 or args.port in (8766, 8767):
        parser.error('Choose an unprivileged port other than existing demos 8766/8767')
    from scripts.loyalty_synthetic import prepare_environment, block_external_network
    scratch = prepare_environment('loyalty-native-')
    block_external_network()
    try:
        app = create_native(scratch, shop_id=args.shop_id, secret=private_secret(args.secret_file),
                            sample_customers=samples(args.sample), public_url=args.public_url, minutes=args.minutes)
    except ValueError as error:
        parser.exit(2, f'{error}\n')
    if args.check_only:
        from fastapi.testclient import TestClient
        with TestClient(app, base_url='http://127.0.0.1:8768') as client:
            path = '/api/loyalty/pos/customers/1'
            checks = [client.get('/health').status_code == 200,
                      client.get(path).status_code == 401,
                      client.get(path, headers={'Cookie': 'session=invalid'}).status_code == 401,
                      client.get(path, headers={'Authorization': 'Bearer invalid'}).status_code == 401]
            checks += [client.get(route).status_code == 404 for route in ('/demo/token', '/loyalty', '/docs', '/openapi.json')]
            if not all(checks):
                parser.exit(2, 'Native backend deny check failed; do not expose\n')
        print('Native backend deny checks passed; no listener or tunnel started')
        return
    import uvicorn
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=args.port,
        access_log=False, log_level='critical', proxy_headers=False, limit_concurrency=16,
        timeout_keep_alive=5, h11_max_incomplete_event_size=8192))

    async def run():
        async def stop_at_deadline():
            await asyncio.sleep(args.minutes * 60)
            server.should_exit = True
        expiry = asyncio.create_task(stop_at_deadline())
        try:
            await server.serve()
        finally:
            expiry.cancel()

    print(f'Synthetic native backend: http://127.0.0.1:{args.port}/health; expires in {args.minutes} minutes', flush=True)
    asyncio.run(run())


if __name__ == '__main__':
    main()
