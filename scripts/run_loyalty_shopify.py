"""Pinned local Shopify tooling for the exact loyalty DEV app; no release command.

Private isolated CLI HOME persists under /tmp for parent-assisted device login.
Never loads the repo .env or copies/prints another installation's auth store.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / 'shopify/loyalty-dev'
CLIENT = '75c6c90117c95bd9f683cacaf3ed5c51'
HOME_DIR = Path('/tmp/degen-loyalty-native-cli')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['install', 'version', 'build', 'link', 'login', 'dev'])
    args = parser.parse_args()
    os.umask(0o077)
    HOME_DIR.mkdir(mode=0o700, exist_ok=True)
    info = HOME_DIR.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        parser.error('CLI home must be a private, owned directory')
    env = {'PATH': '/home/jeffr/.local/bin:/usr/bin:/bin', 'CI': '1', 'BROWSER': 'false',
           'SHOPIFY_CLI_NO_ANALYTICS': '1', 'NO_COLOR': '1'}
    if args.action in ('link', 'login', 'dev') and sys.stdin.isatty() and sys.stdout.isatty():
        # Parent-assisted terminal login; local installation remains version-pinned.
        env.pop('CI')
    for key, relative in {'HOME': 'home', 'USERPROFILE': 'home', 'LOCALAPPDATA': 'local',
        'APPDATA': 'roaming', 'XDG_CONFIG_HOME': 'config', 'XDG_CACHE_HOME': 'cache',
        'XDG_DATA_HOME': 'data', 'TMPDIR': 'tmp', 'TEMP': 'tmp', 'TMP': 'tmp'}.items():
        directory = HOME_DIR / relative
        directory.mkdir(mode=0o700, exist_ok=True)
        env[key] = str(directory)
    for key in ['NPM_CONFIG_USERCONFIG', 'NPM_CONFIG_GLOBALCONFIG']:
        config = HOME_DIR / key.lower()
        if not config.exists(): config.write_text('')
        env[key] = str(config)
    env.update(NPM_CONFIG_CACHE=str(HOME_DIR / 'npm-cache'), NPM_CONFIG_AUDIT='false',
               NPM_CONFIG_FUND='false', NPM_CONFIG_REGISTRY='https://registry.npmjs.org')
    if args.action == 'install':
        npm = shutil.which('npm', path=env['PATH'])
        if not npm: parser.error('Existing WSL npm required')
        operation = 'ci' if (PROJECT / 'package-lock.json').exists() else 'install'
        command = [npm, operation, '--ignore-scripts', '--no-audit', '--no-fund']
    else:
        node = shutil.which('node', path=env['PATH'])
        package = PROJECT / 'node_modules/@shopify/cli'
        if not node or not (package / 'package.json').is_file():
            parser.error('Run install first using the project lockfile')
        if json.loads((package / 'package.json').read_text())['version'] != '4.5.2':
            parser.error('Expected locked Shopify CLI 4.5.2')
        command = [node, str(package / 'bin/run.js')]
        target = ['--path', str(PROJECT), '--client-id', CLIENT]
        command += {'version': ['version'], 'build': ['app', 'build', *target],
                    'link': ['app', 'config', 'link', *target],
                    'login': ['auth', 'login'],
                    'dev': ['app', 'dev', *target, '--store', 'degen-loyalty-dev.myshopify.com']}[args.action]
        if args.action == 'link':
            # Link can rewrite TOML. Keep the prepared public configuration for
            # comparison/restoration; CLI 4.5.2 forbids --config with --client-id.
            backup = HOME_DIR / 'prepared-app.toml'
            if not backup.exists():
                shutil.copyfile(PROJECT / 'shopify.app.toml', backup)
    if args.action == 'dev':
        config = tomllib.loads((PROJECT / 'shopify.app.toml').read_text())
        if (config.get('client_id') != CLIENT or config.get('access_scopes', {}).get('scopes') != ''
                or config.get('build', {}).get('dev_store_url') != 'degen-loyalty-dev.myshopify.com'
                or config.get('web_directories') != ['web']
                or config.get('extension_directories') != ['../../extensions/loyalty-pos']
                or config.get('build', {}).get('automatically_update_urls_on_dev') is not True):
            parser.error('Restore/review the prepared exact DEV configuration before preview')
        sys.path.insert(0, str(ROOT))
        from scripts.loyalty_native_web import native_arguments
        try:
            native_args = native_arguments()
        except ValueError as error:
            parser.error(str(error))
        check = subprocess.run(['/tmp/degen-loyalty-test-venv/bin/python',
            str(ROOT / 'scripts/loyalty_native.py'), *native_args, '--check-only'], env=env, cwd=ROOT)
        if check.returncode:
            parser.error('Native deny checks failed; no app dev or tunnel started')
        if not sys.stdin.isatty():
            parser.error('Parent-assisted terminal required to review DEV install/permission prompts')
        print('DEV only; stop at any install/scope approval prompt for parent review. Maximum 30 minutes.', flush=True)
        process = subprocess.Popen(command, cwd=PROJECT, env=env, start_new_session=True)
        try:
            result = process.wait(timeout=1800)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(process.pid, signal.SIGINT)
            try:
                result = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                result = process.wait()
        raise SystemExit(result)
    raise SystemExit(subprocess.call(command, cwd=PROJECT, env=env))


if __name__ == '__main__':
    main()
