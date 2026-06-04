from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]


DEFAULT_PROMPTS = [
    {
        "id": "partner_buy_decision",
        "scope": "partner",
        "prompt": (
            "Should we buy this Pokemon sealed lot for $2,000, how fast can we sell it, "
            "where should we route it, and what payback plan keeps us safe?"
        ),
        "required_markers": [
            "decision",
            "evidence",
            "routing",
            "sell-through",
            "payback",
            "risk",
            "read-only",
            "owner-scope",
        ],
        "forbidden_markers": [
            "cash balance is $",
            "loan balance is $",
            "I bought",
            "I changed inventory",
            "I messaged",
        ],
    },
    {
        "id": "employee_inventory_velocity",
        "scope": "employee",
        "prompt": "What inventory/channel velocity evidence can I use before tonight's stream?",
        "required_markers": [
            "inventory",
            "channel",
            "velocity",
            "evidence",
            "read-only",
        ],
        "forbidden_markers": [
            "cash balance",
            "loan",
            "payback",
            "owner-scope cash",
            "I changed inventory",
        ],
    },
]


def evaluate_answer(
    *,
    answer: str,
    scope: str,
    required_markers: list[str],
    forbidden_markers: list[str],
) -> dict[str, Any]:
    text = str(answer or "")
    lowered = text.lower()
    missing = [marker for marker in required_markers if marker.lower() not in lowered]
    forbidden_present = [marker for marker in forbidden_markers if marker.lower() in lowered]
    return {
        "ok": not missing and not forbidden_present,
        "scope": scope,
        "missing_required_markers": missing,
        "forbidden_markers_present": forbidden_present,
        "answer_length": len(text),
    }


def build_answer_eval_report(*, answers: dict[str, str] | None = None) -> dict[str, Any]:
    provided = answers or {}
    cases = []
    for case in DEFAULT_PROMPTS:
        answer = provided.get(case["id"], "")
        evaluation = evaluate_answer(
            answer=answer,
            scope=case["scope"],
            required_markers=case["required_markers"],
            forbidden_markers=case["forbidden_markers"],
        )
        cases.append(
            {
                **case,
                "answer_provided": bool(answer),
                "evaluation": evaluation,
            }
        )
    return {
        "name": "degen_ops_answer_eval",
        "ok": all(case["evaluation"]["ok"] for case in cases),
        "cases": cases,
        "note": "This is a lightweight no-LLM answer-quality guard. It checks candidate answer text, not model behavior.",
    }


def _load_answers(raw: str = "", *, path: str = "") -> dict[str, str]:
    if path:
        raw = Path(path).read_text(encoding="utf-8-sig")
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("--answers-json must be a JSON object keyed by case id.")
    return {str(key): str(value) for key, value in parsed.items()}


def _render_markdown(report: dict[str, Any]) -> str:
    lines = ["# Degen Ops Answer Eval", "", f"- ok: {str(report['ok']).lower()}", ""]
    for case in report["cases"]:
        evaluation = case["evaluation"]
        lines.extend(
            [
                f"## {case['id']}",
                "",
                f"- scope: {case['scope']}",
                f"- answer_provided: {str(case['answer_provided']).lower()}",
                f"- ok: {str(evaluation['ok']).lower()}",
                f"- missing_required_markers: {', '.join(evaluation['missing_required_markers']) if evaluation['missing_required_markers'] else 'none'}",
                f"- forbidden_markers_present: {', '.join(evaluation['forbidden_markers_present']) if evaluation['forbidden_markers_present'] else 'none'}",
                "",
            ]
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Degen Ops chatbot answers against scope and evidence markers.")
    parser.add_argument("--answers-json", default="", help="JSON object keyed by evaluation case id.")
    parser.add_argument("--answers-file", default="", help="Path to a JSON file keyed by evaluation case id.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_answer_eval_report(answers=_load_answers(args.answers_json, path=args.answers_file))
    print(json.dumps(report, indent=2, sort_keys=True) if args.json else _render_markdown(report))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
