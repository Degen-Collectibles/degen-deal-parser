from __future__ import annotations

import argparse
from pathlib import Path
import sys


SCOPES = {"owner", "partner", "employee"}


def default_python_path(repo_root: Path) -> Path:
    windows_path = repo_root / ".venv" / "Scripts" / "python.exe"
    if windows_path.exists():
        return windows_path
    return repo_root / ".venv" / "bin" / "python"


def default_database_url(repo_root: Path) -> str:
    return "sqlite:///" + (repo_root / "data" / "degen_live.db").as_posix()


def config_path(path: Path) -> str:
    return path.as_posix()


def hermes_env_value(value: str) -> str:
    return f'"{value}"'


def codex_env_value(value: str) -> str:
    return f'"{value}"'


def render_hermes(
    *,
    server_name: str,
    scope: str,
    python_path: Path,
    script_path: Path,
    database_url: str,
) -> str:
    return "\n".join(
        [
            "mcp_servers:",
            f"  {server_name}:",
            f'    command: "{config_path(python_path)}"',
            "    args:",
            f'      - "{config_path(script_path)}"',
            "    env:",
            f'      DEGEN_OPS_MCP_SCOPE: "{scope}"',
            f"      DATABASE_URL: {hermes_env_value(database_url)}",
            '      LOG_TO_FILE: "false"',
            "    timeout: 120",
            "    connect_timeout: 60",
        ]
    )


def render_codex(
    *,
    server_name: str,
    scope: str,
    python_path: Path,
    script_path: Path,
    database_url: str,
) -> str:
    return "\n".join(
        [
            f"[mcp_servers.{server_name}]",
            f'command = "{config_path(python_path)}"',
            f'args = ["{config_path(script_path)}"]',
            "",
            f"[mcp_servers.{server_name}.env]",
            f'DEGEN_OPS_MCP_SCOPE = "{scope}"',
            f"DATABASE_URL = {codex_env_value(database_url)}",
            'LOG_TO_FILE = "false"',
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render Degen Ops MCP config snippets.")
    parser.add_argument("--client", choices=("hermes", "codex"), required=True)
    parser.add_argument("--scope", choices=tuple(sorted(SCOPES)), required=True)
    parser.add_argument("--server-name", default="")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--python-path", type=Path)
    parser.add_argument("--database-url", default="")
    parser.add_argument(
        "--database-url-env",
        default="",
        help="Use an environment-variable reference such as DEGEN_OPS_READONLY_DATABASE_URL instead of a literal URL.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    python_path = (args.python_path or default_python_path(repo_root)).resolve()
    script_path = (repo_root / "scripts" / "degen_ops_mcp.py").resolve()
    server_name = args.server_name or f"degen_ops_{args.scope}"
    database_url = (
        f"${{{args.database_url_env}}}"
        if args.database_url_env
        else args.database_url or default_database_url(repo_root)
    )

    if args.client == "hermes":
        print(
            render_hermes(
                server_name=server_name,
                scope=args.scope,
                python_path=python_path,
                script_path=script_path,
                database_url=database_url,
            )
        )
    else:
        print(
            render_codex(
                server_name=server_name,
                scope=args.scope,
                python_path=python_path,
                script_path=script_path,
                database_url=database_url,
            )
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
