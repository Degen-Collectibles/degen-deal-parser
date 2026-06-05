from __future__ import annotations

import json
from typing import Any

from .ops_mcp import DegenOpsMcpHarness, DEGEN_OPS_SCOPE_TOOL_NAMES, _normalize_scope


DEGEN_OPS_CHAT_SYSTEM_PROMPT = """You are the Degen Ops Agent for Degen Collectibles.
You are a read-only business operator and CFO-style decision partner.
Use tools for current facts. Lead with the decision, then cite evidence.
Never claim that money, inventory, customer messages, partner messages, or production data changed.
If the current scope lacks a needed tool, say what cannot be answered from this scope.
Partner scope can evaluate buys but does not expose raw cash balances, account balances, reserve-gap dollars, or owner loan/payback totals.
In partner scope, describe cash as redacted cash-safety status from evaluate_inventory_buy, not exact cash position.
TikTok scope is a read-only TikTok operator: use TikTok tools for synced orders, products, buyer/product performance, live status, and endpoint coverage; do not claim product, token, webhook, inventory, or pricing mutations happened."""


TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "get_ops_agent_manifest": {
        "type": "function",
        "function": {
            "name": "get_ops_agent_manifest",
            "description": "Read-only manifest listing Degen Ops MCP scope, exposed tools, and guardrails.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    "get_finance_snapshot": {
        "type": "function",
        "function": {
            "name": "get_finance_snapshot",
            "description": "Read-only Degen finance snapshot from existing finance/reporting helpers.",
            "parameters": {
                "type": "object",
                "properties": {"days": {"type": "integer", "minimum": 1, "default": 90}},
                "additionalProperties": False,
            },
        },
    },
    "get_cash_snapshot": {
        "type": "function",
        "function": {
            "name": "get_cash_snapshot",
            "description": "Read-only latest known cash snapshot from bank rows with balances.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    "get_inventory_snapshot": {
        "type": "function",
        "function": {
            "name": "get_inventory_snapshot",
            "description": "Read-only inventory count, cost basis, and list-value snapshot.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    "get_channel_velocity": {
        "type": "function",
        "function": {
            "name": "get_channel_velocity",
            "description": "Read-only sell-through velocity by TikTok, Shopify, Discord, and show/channel sales.",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "default": 90},
                    "category": {"type": "string", "default": ""},
                },
                "additionalProperties": False,
            },
        },
    },
    "get_loan_and_payback_snapshot": {
        "type": "function",
        "function": {
            "name": "get_loan_and_payback_snapshot",
            "description": "Read-only loan proceeds, owner payback, and payout timing evidence.",
            "parameters": {
                "type": "object",
                "properties": {"days": {"type": "integer", "minimum": 1, "default": 90}},
                "additionalProperties": False,
            },
        },
    },
    "evaluate_inventory_buy": {
        "type": "function",
        "function": {
            "name": "evaluate_inventory_buy",
            "description": "Read-only proposed inventory-buy evaluation with evidence-backed routing and cash plan.",
            "parameters": {
                "type": "object",
                "properties": {
                    "scenario": {
                        "type": "object",
                        "description": (
                            "Proposed buy fields such as lot_name, category, purchase_cost, "
                            "expected_revenue, unit_count, minimum_cash_reserve, financing_amount, "
                            "and target_payback_weeks."
                        ),
                    },
                    "days": {"type": "integer", "minimum": 1, "default": 90},
                },
                "required": ["scenario"],
                "additionalProperties": False,
            },
        },
    },
    "generate_partner_update": {
        "type": "function",
        "function": {
            "name": "generate_partner_update",
            "description": "Read-only partner-ready weekly update generated from a proposed buy scenario.",
            "parameters": {
                "type": "object",
                "properties": {
                    "scenario": {
                        "type": "object",
                        "description": "Same proposed buy fields accepted by evaluate_inventory_buy.",
                    },
                    "days": {"type": "integer", "minimum": 1, "default": 90},
                },
                "required": ["scenario"],
                "additionalProperties": False,
            },
        },
    },
    "get_tiktok_agent_manifest": {
        "type": "function",
        "function": {
            "name": "get_tiktok_agent_manifest",
            "description": "Read-only TikTok agent manifest with callable tools and blocked write endpoints.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    "get_tiktok_status": {
        "type": "function",
        "function": {
            "name": "get_tiktok_status",
            "description": "Read-only TikTok integration status, sync status, webhook, and enrichment queue snapshot.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    "get_tiktok_orders": {
        "type": "function",
        "function": {
            "name": "get_tiktok_orders",
            "description": "Read-only TikTok order snapshot from local synced/enriched order rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "default": 7},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 250, "default": 50},
                    "status": {"type": "string", "default": ""},
                    "search": {"type": "string", "default": ""},
                },
                "additionalProperties": False,
            },
        },
    },
    "get_tiktok_products": {
        "type": "function",
        "function": {
            "name": "get_tiktok_products",
            "description": "Read-only TikTok product snapshot from local synced product rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 250, "default": 50},
                    "status": {"type": "string", "default": ""},
                    "search": {"type": "string", "default": ""},
                },
                "additionalProperties": False,
            },
        },
    },
    "get_tiktok_buyer_insights": {
        "type": "function",
        "function": {
            "name": "get_tiktok_buyer_insights",
            "description": "Read-only TikTok buyer intelligence from existing local reporting helpers.",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "default": 90},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 250, "default": 50},
                },
                "additionalProperties": False,
            },
        },
    },
    "get_tiktok_product_performance": {
        "type": "function",
        "function": {
            "name": "get_tiktok_product_performance",
            "description": "Read-only TikTok product performance from existing local reporting helpers.",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "default": 30},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 250, "default": 50},
                },
                "additionalProperties": False,
            },
        },
    },
    "get_tiktok_live_snapshot": {
        "type": "function",
        "function": {
            "name": "get_tiktok_live_snapshot",
            "description": "Read-only TikTok live-session, live analytics, and public-status cache snapshot.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
}


def tool_schemas_for_scope(scope: str | None = None) -> list[dict[str, Any]]:
    normalized_scope = _normalize_scope(scope)
    return [TOOL_SCHEMAS[name] for name in DEGEN_OPS_SCOPE_TOOL_NAMES[normalized_scope]]


def _loads_args(raw_args: Any) -> dict[str, Any]:
    if isinstance(raw_args, dict):
        return raw_args
    if not raw_args:
        return {}
    try:
        parsed = json.loads(str(raw_args))
    except json.JSONDecodeError:
        return {"_error": f"Invalid JSON tool arguments: {raw_args}"}
    return parsed if isinstance(parsed, dict) else {}


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, default=str, ensure_ascii=True)


def _dict_get(value: Any, key: str, default: Any = None) -> Any:
    return value.get(key, default) if isinstance(value, dict) else default


class DegenOpsChatToolRunner:
    def __init__(self, *, scope: str | None = None, harness: DegenOpsMcpHarness | None = None):
        self.scope = _normalize_scope(scope)
        self.harness = harness or DegenOpsMcpHarness()
        self.allowed_tools = set(DEGEN_OPS_SCOPE_TOOL_NAMES[self.scope])

    def call_tool(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        if name not in self.allowed_tools:
            return {
                "error": f"Tool {name!r} is not available in Degen Ops scope {self.scope!r}.",
                "read_only": True,
            }
        payload = args or {}
        if payload.get("_error"):
            return {"error": payload["_error"], "read_only": True}
        if name == "get_ops_agent_manifest":
            return self.harness.get_manifest(
                scope=self.scope,
                tools=DEGEN_OPS_SCOPE_TOOL_NAMES[self.scope],
            )
        if name == "get_finance_snapshot":
            return self.harness.get_finance_snapshot(days=payload.get("days", 90))
        if name == "get_cash_snapshot":
            return self.harness.get_cash_snapshot()
        if name == "get_inventory_snapshot":
            return self.harness.get_inventory_snapshot()
        if name == "get_channel_velocity":
            return self.harness.get_channel_velocity(
                days=payload.get("days", 90),
                category=payload.get("category", ""),
            )
        if name == "get_loan_and_payback_snapshot":
            return self.harness.get_loan_and_payback_snapshot(days=payload.get("days", 90))
        if name == "evaluate_inventory_buy":
            return self.harness.evaluate_inventory_buy(
                scenario=payload.get("scenario") or {},
                days=payload.get("days", 90),
                audience_scope=self.scope,
            )
        if name == "generate_partner_update":
            return self.harness.generate_partner_update(
                scenario=payload.get("scenario") or {},
                days=payload.get("days", 90),
                audience_scope=self.scope,
            )
        if name == "get_tiktok_agent_manifest":
            return self.harness.get_tiktok_agent_manifest()
        if name == "get_tiktok_status":
            return self.harness.get_tiktok_status()
        if name == "get_tiktok_orders":
            return self.harness.get_tiktok_orders(
                days=payload.get("days", 7),
                limit=payload.get("limit", 50),
                status=payload.get("status", ""),
                search=payload.get("search", ""),
            )
        if name == "get_tiktok_products":
            return self.harness.get_tiktok_products(
                limit=payload.get("limit", 50),
                status=payload.get("status", ""),
                search=payload.get("search", ""),
            )
        if name == "get_tiktok_buyer_insights":
            return self.harness.get_tiktok_buyer_insights(
                days=payload.get("days", 90),
                limit=payload.get("limit", 50),
            )
        if name == "get_tiktok_product_performance":
            return self.harness.get_tiktok_product_performance(
                days=payload.get("days", 30),
                limit=payload.get("limit", 50),
            )
        if name == "get_tiktok_live_snapshot":
            return self.harness.get_tiktok_live_snapshot()
        return {"error": f"Unsupported Degen Ops tool {name!r}.", "read_only": True}


def _tool_call_id(tool_call: Any, fallback: str) -> str:
    return str(getattr(tool_call, "id", None) or _dict_get(tool_call, "id") or fallback)


def _tool_call_name(tool_call: Any) -> str:
    function = getattr(tool_call, "function", None)
    if function is None and isinstance(tool_call, dict):
        function = tool_call.get("function") or {}
    return str(getattr(function, "name", None) or _dict_get(function, "name") or "")


def _tool_call_arguments(tool_call: Any) -> Any:
    function = getattr(tool_call, "function", None)
    if function is None and isinstance(tool_call, dict):
        function = tool_call.get("function") or {}
    return getattr(function, "arguments", None) or _dict_get(function, "arguments")


def _message_content(message: Any) -> str:
    return str(getattr(message, "content", None) or _dict_get(message, "content") or "")


def _message_tool_calls(message: Any) -> list[Any]:
    tool_calls = getattr(message, "tool_calls", None)
    if tool_calls is None and isinstance(message, dict):
        tool_calls = message.get("tool_calls")
    return list(tool_calls or [])


def _assistant_message_for_history(message: Any, tool_calls: list[Any]) -> dict[str, Any]:
    history_message: dict[str, Any] = {"role": "assistant", "content": _message_content(message)}
    if tool_calls:
        history_message["tool_calls"] = [
            {
                "id": _tool_call_id(tool_call, f"tool-{index}"),
                "type": "function",
                "function": {
                    "name": _tool_call_name(tool_call),
                    "arguments": str(_tool_call_arguments(tool_call) or "{}"),
                },
            }
            for index, tool_call in enumerate(tool_calls)
        ]
    return history_message


def _first_choice_message(response: Any) -> Any:
    choices = getattr(response, "choices", None) or _dict_get(response, "choices", [])
    if not choices:
        raise RuntimeError("Model response did not include choices.")
    choice = choices[0]
    return getattr(choice, "message", None) or _dict_get(choice, "message")


def run_chat_turn(
    *,
    client: Any,
    model: str,
    messages: list[dict[str, Any]],
    runner: DegenOpsChatToolRunner,
    temperature: float = 0.2,
    max_tool_rounds: int = 4,
) -> tuple[str, list[dict[str, Any]]]:
    tool_schemas = tool_schemas_for_scope(runner.scope)
    working_messages = list(messages)
    for _ in range(max_tool_rounds):
        response = client.chat.completions.create(
            model=model,
            messages=working_messages,
            tools=tool_schemas,
            tool_choice="auto",
            temperature=temperature,
        )
        message = _first_choice_message(response)
        tool_calls = _message_tool_calls(message)
        if not tool_calls:
            final_text = _message_content(message)
            working_messages.append({"role": "assistant", "content": final_text})
            return final_text, working_messages

        working_messages.append(_assistant_message_for_history(message, tool_calls))
        for index, tool_call in enumerate(tool_calls):
            tool_name = _tool_call_name(tool_call)
            tool_result = runner.call_tool(tool_name, _loads_args(_tool_call_arguments(tool_call)))
            working_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": _tool_call_id(tool_call, f"tool-{index}"),
                    "name": tool_name,
                    "content": _json_dumps(tool_result),
                }
            )

    raise RuntimeError(f"Model exceeded {max_tool_rounds} tool rounds without a final answer.")


def initial_chat_messages(system_prompt: str | None = None) -> list[dict[str, str]]:
    return [{"role": "system", "content": system_prompt or DEGEN_OPS_CHAT_SYSTEM_PROMPT}]
