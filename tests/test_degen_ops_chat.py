from scripts.degen_ops_chat import build_preflight_report, configure_environment, parse_args

from app.ops_chat import (
    DEGEN_OPS_CHAT_SYSTEM_PROMPT,
    DegenOpsChatToolRunner,
    initial_chat_messages,
    run_chat_turn,
    tool_schemas_for_scope,
)


class FakeHarness:
    def get_manifest(self, *, scope, tools):
        return {"scope": scope, "tools": tools, "read_only": True}

    def get_inventory_snapshot(self):
        return {
            "inventory_snapshot": {"active_items": 12, "estimated_list_value": 3456.0},
            "evidence": [{"source": "inventory_items", "url": "/inventory"}],
            "read_only": True,
        }

    def get_finance_snapshot(self, days=90):
        return {"finance_statement": {"revenue": 1000.0}, "read_only": True}

    def get_cash_snapshot(self):
        return {"cash_snapshot": {"latest_known_cash": 1000.0}, "read_only": True}

    def get_channel_velocity(self, days=90, category=""):
        return {"channel_velocity": [], "read_only": True}

    def get_loan_and_payback_snapshot(self, days=90):
        return {"loan_snapshot": {}, "read_only": True}

    def evaluate_inventory_buy(self, scenario, days=90, audience_scope="owner"):
        return {"verdict": "safe", "audience_scope": audience_scope, "read_only": True}

    def generate_partner_update(self, scenario, days=90, audience_scope="owner"):
        return {"partner_update": "Weekly business update", "audience_scope": audience_scope, "read_only": True}


class FailingHarness(FakeHarness):
    def get_inventory_snapshot(self):
        raise RuntimeError("postgresql+psycopg://user:secret@db.example.com/degen read failed")


class FakeCompletions:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class FakeClient:
    def __init__(self, responses):
        self.chat = type("Chat", (), {})()
        self.chat.completions = FakeCompletions(responses)


def _tool_call_response(name: str, arguments: str = "{}"):
    return {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": name, "arguments": arguments},
                        }
                    ],
                }
            }
        ]
    }


def _final_response(text: str):
    return {"choices": [{"message": {"content": text}}]}


def test_chat_tool_schemas_follow_employee_scope():
    schemas = tool_schemas_for_scope("employee")
    names = {schema["function"]["name"] for schema in schemas}

    assert names == {
        "get_ops_agent_manifest",
        "get_inventory_snapshot",
        "get_channel_velocity",
    }
    assert "get_cash_snapshot" not in names
    assert "evaluate_inventory_buy" not in names


def test_chat_system_prompt_names_partner_cash_redaction():
    assert "Partner scope can evaluate buys" in DEGEN_OPS_CHAT_SYSTEM_PROMPT
    assert "does not expose raw cash balances" in DEGEN_OPS_CHAT_SYSTEM_PROMPT
    assert "redacted cash-safety status" in DEGEN_OPS_CHAT_SYSTEM_PROMPT


def test_chat_tool_schemas_follow_partner_scope_without_owner_cash_tools():
    schemas = tool_schemas_for_scope("partner")
    names = {schema["function"]["name"] for schema in schemas}

    assert names == {
        "get_ops_agent_manifest",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_channel_velocity",
        "evaluate_inventory_buy",
        "generate_partner_update",
    }
    assert "get_cash_snapshot" not in names
    assert "get_loan_and_payback_snapshot" not in names


def test_chat_runner_refuses_out_of_scope_tool():
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    result = runner.call_tool("get_cash_snapshot", {})

    assert result["read_only"] is True
    assert "not available" in result["error"]


def test_chat_runner_passes_scope_to_buy_evaluation():
    runner = DegenOpsChatToolRunner(scope="partner", harness=FakeHarness())

    result = runner.call_tool("evaluate_inventory_buy", {"scenario": {"lot_name": "Test"}})

    assert result["audience_scope"] == "partner"


def test_run_chat_turn_executes_read_only_tool_and_returns_final_answer():
    client = FakeClient(
        [
            _tool_call_response("get_inventory_snapshot"),
            _final_response("Inventory checked from /inventory. No changes made."),
        ]
    )
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())
    messages = initial_chat_messages()
    messages.append({"role": "user", "content": "What does inventory look like?"})

    answer, history = run_chat_turn(
        client=client,
        model="fake-model",
        messages=messages,
        runner=runner,
    )

    assert answer == "Inventory checked from /inventory. No changes made."
    assert client.chat.completions.calls[0]["tools"]
    tool_messages = [message for message in history if message["role"] == "tool"]
    assert len(tool_messages) == 1
    assert "inventory_snapshot" in tool_messages[0]["content"]
    assert "read_only" in tool_messages[0]["content"]


def test_run_chat_turn_returns_tool_error_for_invalid_json_arguments():
    client = FakeClient(
        [
            _tool_call_response("get_inventory_snapshot", "{bad json"),
            _final_response("The tool arguments were invalid, so I cannot use that result."),
        ]
    )
    runner = DegenOpsChatToolRunner(scope="employee", harness=FakeHarness())

    _, history = run_chat_turn(
        client=client,
        model="fake-model",
        messages=[{"role": "user", "content": "Check inventory"}],
        runner=runner,
    )

    tool_messages = [message for message in history if message["role"] == "tool"]
    assert "Invalid JSON tool arguments" in tool_messages[0]["content"]


def test_chat_script_missing_database_url_env_fails(monkeypatch):
    args = type("Args", (), {"scope": "employee", "database_url": "", "database_url_env": "MISSING_DEGEN_DB_URL"})()
    monkeypatch.delenv("MISSING_DEGEN_DB_URL", raising=False)

    assert configure_environment(args) == 1


def test_preflight_report_lists_scope_tools_without_model_call():
    report = build_preflight_report(
        scope="employee",
        provider="nvidia",
        model="aws/anthropic/claude-haiku-4-5-v1",
        api_key_configured=True,
        runner=DegenOpsChatToolRunner(scope="employee", harness=FakeHarness()),
        read_check=False,
    )

    assert report["ok"] is True
    assert report["scope"] == "employee"
    assert report["provider"] == "nvidia"
    assert report["model"] == "aws/anthropic/claude-haiku-4-5-v1"
    assert report["api_key_configured"] is True
    assert report["read_check"] == "skipped"
    assert report["tools"] == [
        "get_channel_velocity",
        "get_inventory_snapshot",
        "get_ops_agent_manifest",
    ]


def test_preflight_report_read_check_exercises_partner_workflow_without_owner_cash_tools():
    report = build_preflight_report(
        scope="partner",
        provider="nvidia",
        model="aws/anthropic/claude-haiku-4-5-v1",
        api_key_configured=True,
        runner=DegenOpsChatToolRunner(scope="partner", harness=FakeHarness()),
        read_check=True,
    )

    checked_tools = [check["tool"] for check in report["read_checks"]]
    assert report["ok"] is True
    assert report["read_check"] == "passed"
    assert checked_tools == [
        "evaluate_inventory_buy",
        "generate_partner_update",
        "get_channel_velocity",
        "get_finance_snapshot",
        "get_inventory_snapshot",
        "get_ops_agent_manifest",
    ]
    assert "get_cash_snapshot" not in checked_tools
    assert "get_loan_and_payback_snapshot" not in checked_tools


def test_chat_script_defaults_to_employee_scope(monkeypatch):
    monkeypatch.setattr("sys.argv", ["degen_ops_chat.py"])

    args = parse_args()

    assert args.scope == "employee"


def test_preflight_report_read_check_redacts_database_errors():
    report = build_preflight_report(
        scope="employee",
        provider="nvidia",
        model="aws/anthropic/claude-haiku-4-5-v1",
        api_key_configured=True,
        runner=DegenOpsChatToolRunner(scope="employee", harness=FailingHarness()),
        read_check=True,
    )

    assert report["ok"] is False
    assert report["read_check"] == "failed"
    assert report["read_checks"]
    assert "secret" not in report["read_check_error"]
    assert "postgresql+psycopg://***:***@db.example.com/degen" in report["read_check_error"]
