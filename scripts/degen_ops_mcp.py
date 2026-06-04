from pathlib import Path
import os
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["LOG_TO_FILE"] = "false"

from app.ops_mcp import main


if __name__ == "__main__":
    main()
