"""Run local POS package tooling with a synthetic HOME and no inherited secrets.

Usage: python3 scripts/run_loyalty_pos_node.py npm ci
No Shopify CLI, app login, tunnel or production environment is used.
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parents[1]
if len(sys.argv) < 2 or sys.argv[1] not in ('npm','node'):
    raise SystemExit('Use npm or node for local package tooling')
executable = shutil.which(sys.argv[1])
if not executable:raise SystemExit('Local Node/npm installation required')
home = Path(tempfile.mkdtemp(prefix='loyalty-pos-node-'))
env = {key:os.environ[key] for key in ('PATH','SYSTEMROOT','WINDIR','COMSPEC','PATHEXT') if key in os.environ}
for key in ('HOME','USERPROFILE','LOCALAPPDATA','APPDATA','XDG_CONFIG_HOME','XDG_CACHE_HOME','XDG_DATA_HOME','TMPDIR','TEMP','TMP'):
    directory = home/key.lower(); directory.mkdir(); env[key] = str(directory)
for key in ('NPM_CONFIG_USERCONFIG','NPM_CONFIG_GLOBALCONFIG'):
    path = home/key.lower(); path.write_text(''); env[key] = str(path)
env.update(NPM_CONFIG_CACHE=str(home/'npm-cache'), NPM_CONFIG_AUDIT='false', NPM_CONFIG_FUND='false',
           NPM_CONFIG_REGISTRY='https://registry.npmjs.org', PLAYWRIGHT_BROWSERS_PATH='/tmp/loyalty-pos-browsers')
raise SystemExit(subprocess.call([executable,*sys.argv[2:]], cwd=root/'extensions/loyalty-pos', env=env))
