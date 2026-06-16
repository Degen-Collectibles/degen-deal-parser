from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import sys
import tomllib
from typing import Any

import yaml
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.ops_mcp import DEGEN_OPS_SCOPE_TOOL_NAMES


EXPECTED_TOOLS_BY_SCOPE = {scope: set(tools) for scope, tools in DEGEN_OPS_SCOPE_TOOL_NAMES.items()}


SECRET_PATTERNS = [
    re.compile(r"(?i)(postgres(?:ql)?(?:\+psycopg)?://)([^:@\s]+):([^@\s]+)@"),
    re.compile(r"(?i)(password=)[^\s&;]+"),
    re.compile(r"(?i)(token=)[^\s&;]+"),
    re.compile(r"(?i)(api[_-]?key=)[^\s&;]+"),
]


def sanitize(text: Any) -> str:
    value = str(text)
    for pattern in SECRET_PATTERNS:
        if pattern.groups >= 3:
            value = pattern.sub(r"\1***:***@", value)
        else:
            value = pattern.sub(r"\1***", value)
    return value


def hermes_config_path() -> Path:
    return Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "hermes" / "config.yaml"


def codex_config_path() -> Path:
    return Path.home() / ".codex" / "config.toml"


def load_hermes_servers(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    return {
        name: server
        for name, server in (config.get("mcp_servers") or {}).items()
        if name.startswith("degen_ops_")
    }


def load_codex_servers(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("rb") as handle:
        config = tomllib.load(handle)
    return {
        name: server
        for name, server in (config.get("mcp_servers") or {}).items()
        if name.startswith("degen_ops_")
    }


async def probe_server(
    config_name: str,
    server_name: str,
    server: dict[str, Any],
    *,
    read_check: bool,
    database_url: str | None = None,
) -> bool:
    env = dict(server.get("env") or {})
    if database_url:
        env["DATABASE_URL"] = database_url
    params = StdioServerParameters(
        command=str(server["command"]),
        args=[str(arg) for arg in server.get("args", [])],
        env={str(key): str(value) for key, value in env.items()},
    )
    label = f"{config_name}:{server_name}"
    try:
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                tool_names = {tool.name for tool in tools.tools}
                manifest_result = await session.call_tool("get_ops_agent_manifest", {})
                manifest_text = getattr(manifest_result.content[0], "text", "{}")
                manifest = json.loads(manifest_text)
                scope = manifest.get("scope")
                expected_tools = EXPECTED_TOOLS_BY_SCOPE.get(scope)
                boundary_ok = expected_tools is not None and tool_names == expected_tools
                read_ok = True
                if read_check:
                    snapshot = await session.call_tool("get_inventory_snapshot", {})
                    read_ok = not bool(snapshot.isError)
                    if snapshot.isError:
                        detail = getattr(snapshot.content[0], "text", snapshot.content[0])
                        print(f"{label}: read_check_error={sanitize(detail)}")
                print(
                    f"{label}: scope={scope} tools={len(tool_names)} "
                    f"boundary_ok={boundary_ok} read_check={read_ok if read_check else 'skipped'}"
                )
                return boundary_ok and read_ok
    except Exception as exc:
        print(f"{label}: error={sanitize(exc)}")
        return False


async def run_smoke(
    configs: dict[str, dict[str, dict[str, Any]]],
    *,
    read_check: bool,
    database_url: str | None = None,
    scope: str | None = None,
) -> bool:
    ok = True
    for config_name, servers in configs.items():
        if not servers:
            print(f"{config_name}: no degen_ops_* MCP servers configured")
            ok = False
            continue
        for server_name in sorted(servers):
            configured_scope = str((servers[server_name].get("env") or {}).get("DEGEN_OPS_MCP_SCOPE") or "")
            if scope and configured_scope != scope:
                continue
            ok = await probe_server(
                config_name,
                server_name,
                servers[server_name],
                read_check=read_check,
                database_url=database_url,
            ) and ok
    return ok


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-test configured Degen Ops MCP servers.")
    parser.add_argument(
        "--config",
        choices=("hermes", "codex", "both"),
        default="both",
        help="Which local client config to inspect.",
    )
    parser.add_argument(
        "--read-check",
        action="store_true",
        help="Also call get_inventory_snapshot to verify the configured DATABASE_URL can serve read queries.",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="Temporarily override DATABASE_URL for this smoke run. The value is not written to config.",
    )
    parser.add_argument(
        "--database-url-env",
        default="",
        help="Read a temporary DATABASE_URL override from this environment variable.",
    )
    parser.add_argument(
        "--scope",
        choices=("owner", "partner", "manager", "employee", "tiktok"),
        default="",
        help="Only test configured servers with this DEGEN_OPS_MCP_SCOPE.",
    )
    parser.add_argument("--hermes-config", type=Path, default=hermes_config_path())
    parser.add_argument("--codex-config", type=Path, default=codex_config_path())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_url = args.database_url
    if args.database_url_env:
        database_url = os.getenv(args.database_url_env, "")
        if not database_url:
            print(f"Environment variable {args.database_url_env!r} is not set or is empty.")
            return 1
    configs: dict[str, dict[str, dict[str, Any]]] = {}
    if args.config in {"hermes", "both"}:
        configs["hermes"] = load_hermes_servers(args.hermes_config)
    if args.config in {"codex", "both"}:
        configs["codex"] = load_codex_servers(args.codex_config)
    return 0 if asyncio.run(
        run_smoke(
            configs,
            read_check=args.read_check,
            database_url=database_url or None,
            scope=args.scope or None,
        )
    ) else 1


if __name__ == "__main__":
    sys.exit(main())
