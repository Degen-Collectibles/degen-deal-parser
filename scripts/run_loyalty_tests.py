"""Run synthetic tests without dotenv, inherited credentials or external networking.

Uses the existing interpreter; changes only this process and a fresh temp directory.
"""
import os
from pathlib import Path
import socket
import sys
import tempfile

max_seconds=None
chromium=None
browser_cache=None
if '--loyalty-browser-cache' in sys.argv:
    index=sys.argv.index('--loyalty-browser-cache')
    browser_cache=sys.argv[index+1];del sys.argv[index:index+2]
    if not Path(browser_cache).is_dir():raise SystemExit('Select an existing Playwright binary cache')
if '--loyalty-chromium' in sys.argv:
    index=sys.argv.index('--loyalty-chromium')
    chromium=sys.argv[index+1];del sys.argv[index:index+2]
    if not Path(chromium).is_file():raise SystemExit('Select an existing Chromium executable')
if '--loyalty-max-seconds' in sys.argv:
    index=sys.argv.index('--loyalty-max-seconds')
    max_seconds=int(sys.argv[index+1]);del sys.argv[index:index+2]
    if not 10<=max_seconds<=7200:raise SystemExit('Test time bound must be 10..7200 seconds')

# Optional socket must belong to a freshly initialized disposable test cluster.
ui_check = '--loyalty-ui' in sys.argv
if ui_check: sys.argv.remove('--loyalty-ui')
pg_socket = None
if '--loyalty-pg-socket' in sys.argv:
    index = sys.argv.index('--loyalty-pg-socket')
    pg_socket = sys.argv[index+1]
    del sys.argv[index:index+2]
    if not pg_socket.startswith('/tmp/loyalty-worker-pg-') or '..' in pg_socket:
        raise SystemExit('Only a disposable loyalty-worker-pg socket is permitted')

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.loyalty_synthetic import prepare_environment, block_external_network
prepare_environment()
if browser_cache:os.environ['PLAYWRIGHT_BROWSERS_PATH']=browser_cache
if ui_check:
    os.environ['LOYALTY_TEST_UI'] = '1'
if pg_socket:
    os.environ['LOYALTY_TEST_PG_SOCKET'] = pg_socket
browser_url_allowed=block_external_network(pg_socket)
if chromium:
    from scripts.loyalty_synthetic import configure_test_chromium
    configure_test_chromium(chromium,browser_url_allowed)
import threading
import pytest
print('Synthetic test directory:', tempfile.gettempdir(), flush=True)
timer=None
if max_seconds:
    import _thread
    def stop_at_bound():
        print('Synthetic test runtime bound reached; interrupting pytest.',flush=True)
        _thread.interrupt_main()
    timer=threading.Timer(max_seconds,stop_at_bound);timer.daemon=True;timer.start()
try:
    result=pytest.main(sys.argv[1:] or ['tests/test_loyalty_domain.py','-q'])
finally:
    if timer:timer.cancel()
raise SystemExit(result)
