"""Launch exact DEV checkout preview with isolated CLI credentials, bounded to one hour."""
import fcntl,json,os,signal,subprocess,tempfile,time,tomllib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
PROJECT=ROOT/'shopify/loyalty-checkout-dev'
HOME=Path('/tmp/degen-loyalty-native-cli')
HANDOFF=Path('/tmp/degen-loyalty-native-handoff')
CLI=Path('/home/jeffr/degen-candidates/loyalty-release-review-20260911/shopify/loyalty-dev/node_modules/@shopify/cli')

def main():
    os.umask(0o077)
    config=tomllib.loads((PROJECT/'shopify.app.toml').read_text())
    client='75c6c90117c95bd9f683cacaf3ed5c51'
    assert config['client_id']==client and config['build']['dev_store_url']=='degen-loyalty-dev.myshopify.com'
    assert set(config['access_scopes']['scopes'].split(','))=={'read_customers','read_discounts','read_inventory','read_orders','read_products','read_returns','write_discounts'}
    assert json.loads((CLI/'package.json').read_text())['version']=='4.5.2'
    with (HANDOFF/'checkout.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        manifest=HANDOFF/'checkout-inputs.json'
        if manifest.exists():
            inputs=json.loads(manifest.read_text())
            if inputs['deadline']<=time.time():raise SystemExit('Previous checkout session expired; review its ledger before starting another.')
        else:
            inputs={'scratch':tempfile.mkdtemp(prefix='loyalty-checkout-'),'deadline':time.time()+3600}
            manifest.write_text(json.dumps(inputs));manifest.chmod(0o600)
        env={'PATH':'/home/jeffr/.local/bin:/usr/bin:/bin','BROWSER':'false',
             'SHOPIFY_CLI_NO_ANALYTICS':'1','NO_COLOR':'1'}
        for key,relative in {'HOME':'home','USERPROFILE':'home','LOCALAPPDATA':'local','APPDATA':'roaming',
            'XDG_CONFIG_HOME':'config','XDG_CACHE_HOME':'cache','XDG_DATA_HOME':'data','TMPDIR':'tmp','TEMP':'tmp','TMP':'tmp'}.items():
            directory=HOME/relative;directory.mkdir(mode=0o700,parents=True,exist_ok=True);env[key]=str(directory)
        print('DEV only: degen-loyalty-dev; one-hour session; ledger survives CLI restarts.',flush=True)
        process=subprocess.Popen(['/home/jeffr/.local/bin/node',str(CLI/'bin/run.js'),'app','dev','--path',str(PROJECT),
            '--client-id',client,'--store','degen-loyalty-dev.myshopify.com'],cwd=PROJECT,env=env,start_new_session=True)
        try:return process.wait(timeout=max(1,inputs['deadline']-time.time()))
        except (KeyboardInterrupt,subprocess.TimeoutExpired):
            os.killpg(process.pid,signal.SIGINT)
            try:return process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL);return process.wait()

if __name__=='__main__':raise SystemExit(main())
