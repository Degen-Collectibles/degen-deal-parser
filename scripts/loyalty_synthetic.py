"""Process-local isolation for the loyalty tests and localhost synthetic demo.

Call before importing app modules. Never imports the production application.
"""
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading


def prepare_environment(prefix='degen-loyalty-tests-'):
    # Uvicorn's optional native loop bypasses Python socket.bind tracking.
    # Use the standard loop so only this test process's listeners are allowed.
    sys.modules['uvloop'] = None
    # Keep only OS plumbing required by Windows/Python. Never inspect secret values.
    keep = {k: os.environ[k] for k in os.environ if k.upper() in {
        'SYSTEMROOT', 'WINDIR', 'PATH', 'TEMP', 'TMP', 'COMSPEC', 'PATHEXT',
    }}
    os.environ.clear()
    os.environ.update(keep)
    tempfile.tempdir = tempfile.mkdtemp(prefix=prefix)
    os.environ.update({
        'DATABASE_URL': 'sqlite:///' + (Path(tempfile.gettempdir()) / 'app.db').as_posix(),
        'SESSION_SECRET': 'synthetic-session-' + 'x' * 40,
        'ADMIN_PASSWORD': 'synthetic-password-' + 'x' * 24,
        'SESSION_HTTPS_ONLY': 'false', 'SESSION_DOMAIN': '',
        'EMPLOYEE_TOKEN_HMAC_KEY': 'synthetic-token-' + 'y' * 40,
        'EMPLOYEE_EMAIL_HASH_SALT': 'synthetic-salt',
        'DISABLE_EXTERNAL_WARMUPS': 'true',
    })
    # Disable external worker/provider switches without changing pure domain rules
    # or portal routing that the repository tests explicitly exercise.
    for alias in (
        'DISCORD_INGEST_ENABLED', 'PARSER_WORKER_ENABLED', 'STARTUP_BACKFILL_ENABLED',
        'STARTUP_OFFLINE_AUDIT_ENABLED', 'PERIODIC_OFFLINE_AUDIT_ENABLED',
        'PERIODIC_STITCH_AUDIT_ENABLED', 'PERIODIC_ATTACHMENT_REPAIR_ENABLED',
        'TIKTOK_SYNC_ENABLED', 'TIKTOK_TOKEN_REFRESH_ENABLED',
        'INVENTORY_AUTO_PRICE_ENABLED', 'INVENTORY_SHOPIFY_SYNC_ENABLED',
        'SHOPIFY_POS_TAX_SENTINEL_ENABLED', 'CLOCKIFY_RECONCILE_ENABLED',
        'LOYALTY_RECEIVING_ENABLED', 'LOYALTY_PROCESSING_ENABLED', 'LOYALTY_POSTING_ENABLED',
        'SMS_DISPATCHER_ENABLED', 'SMS_OPERATIONAL_ALERTS_ENABLED', 'SMS_WEBHOOKS_ENABLED',
        'TEAM_REQUEST_ALERT_EMAIL_ENABLED', 'TEAM_SUPPLY_DISCORD_ENABLED',
        'INVENTORY_SLAB_RESTICKER_SMS_ENABLED',
    ):
        os.environ[alias] = 'false'
    os.environ['INVENTORY_AUTO_SHOPIFY_PUSH'] = 'false'
    scratch=Path(tempfile.gettempdir())
    os.environ['LOG_DIR']=str(scratch/'logs')
    os.environ['DATA_ROOT']=str(scratch/'data')
    synthetic_home=scratch/'home'
    for key,relative in {
        'HOME':'home','USERPROFILE':'home','LOCALAPPDATA':'home/AppData/Local',
        'APPDATA':'home/AppData/Roaming','XDG_CONFIG_HOME':'home/.config',
        'XDG_CACHE_HOME':'home/.cache','XDG_DATA_HOME':'home/.local/share',
    }.items():
        path=scratch/relative;path.mkdir(parents=True,exist_ok=True);os.environ[key]=str(path)
    if os.name=='nt':
        os.environ['HOMEDRIVE']=synthetic_home.drive
        os.environ['HOMEPATH']=str(synthetic_home)[len(synthetic_home.drive):]
    os.environ['TEMP']=os.environ['TMP']=str(scratch)
    os.environ['LOYALTY_SYNTHETIC_ROOT']=str(scratch)
    from pydantic_settings.sources import DotEnvSettingsSource
    DotEnvSettingsSource._read_env_files=lambda self: {}
    return scratch


def block_external_network(pg_socket=None):
    import weakref
    from urllib.parse import urlsplit
    bound_sockets=weakref.WeakSet()
    pair_context=threading.local()
    original_connect=socket.socket.connect
    original_connect_ex=socket.socket.connect_ex
    original_bind=socket.socket.bind
    original_pair=socket.socketpair
    def bind(self,address):
        result=original_bind(self,address)
        if isinstance(address,tuple) and address[0] in ('127.0.0.1','::1'):
            bound_sockets.add(self)
        return result
    def owned_address(address):
        if not isinstance(address,tuple) or address[0] not in ('127.0.0.1','::1'):return False
        for listener in list(bound_sockets):
            try:
                if listener.getsockname()[:2]==address[:2]:return True
            except OSError:pass
        return False
    def safe_pair(*args,**kwargs):
        pair_context.active=True
        try:return original_pair(*args,**kwargs)
        finally:pair_context.active=False
    def permitted(self,address):
        if pg_socket and self.family==socket.AF_UNIX and address==pg_socket+'/.s.PGSQL.55439':
            return True
        if getattr(pair_context,'active',False) and isinstance(address,tuple) and address[0] in ('127.0.0.1','::1'):
            return True
        if owned_address(address):return True
        raise RuntimeError('External network disabled by synthetic loyalty harness')
    def connect(self,address):
        permitted(self,address);return original_connect(self,address)
    def connect_ex(self,address):
        permitted(self,address);return original_connect_ex(self,address)
    def browser_url_allowed(url):
        parsed=urlsplit(url)
        return parsed.scheme in ('file','data','about') or (parsed.scheme in ('http','https') and owned_address((parsed.hostname,parsed.port or (443 if parsed.scheme=='https' else 80))))
    socket.socket.bind=bind
    socket.socketpair=safe_pair
    socket.socket.connect=connect
    socket.socket.connect_ex=connect_ex
    return browser_url_allowed


def configure_test_chromium(executable,browser_url_allowed):
    """Use an explicitly selected existing binary, never a real browser profile."""
    from playwright.sync_api import BrowserType,Browser,BrowserContext
    original_launch=BrowserType.launch
    def launch(self,*args,**kwargs):
        if self.name=='chromium':kwargs['executable_path']=executable
        return original_launch(self,*args,**kwargs)
    BrowserType.launch=launch
    def guard(original):
        def create(self,*args,**kwargs):
            result=original(self,*args,**kwargs)
            result.route('**/*',lambda route:route.continue_() if browser_url_allowed(route.request.url) else route.abort())
            return result
        return create
    Browser.new_context=guard(Browser.new_context)
    Browser.new_page=guard(Browser.new_page)
    BrowserContext.new_page=guard(BrowserContext.new_page)
