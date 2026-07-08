from __future__ import annotations

import ast
import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PLANNER = ROOT / "deploy" / "linux" / "degen-prod-db-retention.py"
PREFIX = "degen_green_prod_green_"
NOW = datetime(2026, 6, 29, 23, 0, tzinfo=timezone.utc)
NOW_STAMP = "20260629T230000Z"


def load_planner():
    spec = importlib.util.spec_from_file_location("degen_prod_db_retention", PLANNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pair(stamp: str, *, prefix: str = PREFIX) -> list[str]:
    dump = f"{prefix}{stamp}.dump"
    return [dump, f"{dump}.sha256"]


def record(stamp: str, reasons: list[str], *, prefix: str = PREFIX) -> dict[str, object]:
    dump = f"{prefix}{stamp}.dump"
    return {
        "dump": dump,
        "checksum": f"{dump}.sha256",
        "timestamp": stamp,
        "reasons": reasons,
    }


def run_cli(*args: str, names: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(PLANNER), *args],
        input="\n".join(names) + "\n",
        text=True,
        capture_output=True,
        check=False,
    )


def test_sources_parse_with_python_310_grammar() -> None:
    for path in (PLANNER, Path(__file__), ROOT / "tests/test_degen_prod_db_backup_script.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path), feature_version=(3, 10))


def test_local_keeps_exactly_two_newest_complete_pairs() -> None:
    module = load_planner()
    names = pair("20260627T031500Z") + pair("20260628T031500Z") + pair("20260629T031500Z")

    plan = module.plan_inventory(names, mode="local", prefix=PREFIX, now=NOW, local_count=2)

    assert plan["mode"] == "local"
    assert plan["prefix"] == PREFIX
    assert plan["keep"] == [
        record("20260629T031500Z", ["local-newest", "newest"]),
        record("20260628T031500Z", ["local-newest"]),
    ]
    assert plan["delete"] == [record("20260627T031500Z", ["expired"])]
    assert plan["protected"] == []


def test_remote_unions_latest_dates_iso_weeks_and_months() -> None:
    module = load_planner()
    stamps = [
        "20260430T031500Z",
        "20260503T031500Z",
        "20260531T031500Z",
        "20260601T031500Z",
        "20260607T031500Z",
        "20260614T031500Z",
        "20260621T031500Z",
        "20260622T031500Z",
        "20260623T031500Z",
        "20260624T031500Z",
        "20260625T031500Z",
        "20260626T031500Z",
        "20260627T031500Z",
        "20260628T031500Z",
        "20260629T021500Z",
        "20260629T031500Z",
    ]
    names = [name for stamp in stamps for name in pair(stamp)]

    plan = module.plan_inventory(
        names,
        mode="remote",
        prefix=PREFIX,
        now=NOW,
        daily=7,
        weekly=4,
        monthly=3,
    )

    kept = {item["timestamp"]: item["reasons"] for item in plan["keep"]}
    assert list(kept) == [
        "20260629T031500Z",
        "20260628T031500Z",
        "20260627T031500Z",
        "20260626T031500Z",
        "20260625T031500Z",
        "20260624T031500Z",
        "20260623T031500Z",
        "20260621T031500Z",
        "20260614T031500Z",
        "20260531T031500Z",
        "20260430T031500Z",
    ]
    assert kept["20260629T031500Z"] == ["daily", "monthly", "newest", "weekly"]
    assert kept["20260628T031500Z"] == ["daily", "weekly"]
    assert kept["20260621T031500Z"] == ["weekly"]
    assert kept["20260614T031500Z"] == ["weekly"]
    assert kept["20260531T031500Z"] == ["monthly"]
    assert kept["20260430T031500Z"] == ["monthly"]
    assert sum("daily" in reasons for reasons in kept.values()) == 7
    assert sum("weekly" in reasons for reasons in kept.values()) == 4
    assert sum("monthly" in reasons for reasons in kept.values()) == 3
    assert [item["timestamp"] for item in plan["delete"]] == [
        "20260503T031500Z",
        "20260601T031500Z",
        "20260607T031500Z",
        "20260622T031500Z",
        "20260629T021500Z",
    ]


def test_remote_handles_iso_year_boundary() -> None:
    module = load_planner()
    names = (
        pair("20251228T031500Z")
        + pair("20251229T031500Z")
        + pair("20260104T031500Z")
        + pair("20260105T031500Z")
    )

    plan = module.plan_inventory(
        names,
        mode="remote",
        prefix=PREFIX,
        now=datetime(2026, 1, 6, tzinfo=timezone.utc),
        daily=0,
        weekly=2,
        monthly=0,
    )

    weekly = {item["timestamp"] for item in plan["keep"] if "weekly" in item["reasons"]}
    assert weekly == {"20260104T031500Z", "20260105T031500Z"}
    assert "20251229T031500Z" not in weekly


def test_unknown_incomplete_temporary_malformed_and_future_objects_are_protected() -> None:
    module = load_planner()
    complete = pair("20260629T031500Z")
    incomplete_dump = f"{PREFIX}20260628T031500Z.dump"
    incomplete_checksum = f"{PREFIX}20260627T031500Z.dump.sha256"
    invalid = pair("20260230T031500Z")
    future = pair("20260701T031500Z")
    malformed = f"{PREFIX}2026-06-26T031500Z.dump"
    temporary = f".{complete[0]}.partial"
    extra_suffix = f"{complete[1]}.sha256"
    manual = "manual-preserve.dump"
    names = (
        complete
        + [incomplete_dump, incomplete_checksum]
        + invalid
        + future
        + [malformed, temporary, extra_suffix, manual]
    )

    plan = module.plan_inventory(names, mode="local", prefix=PREFIX, now=NOW, local_count=2)

    assert plan["delete"] == []
    assert plan["protected"] == sorted(
        [
            {"name": incomplete_dump, "reason": "incomplete-pair"},
            {"name": incomplete_checksum, "reason": "incomplete-pair"},
            {"name": invalid[0], "reason": "unparseable-timestamp"},
            {"name": invalid[1], "reason": "unparseable-timestamp"},
            {"name": future[0], "reason": "future-timestamp"},
            {"name": future[1], "reason": "future-timestamp"},
            {"name": malformed, "reason": "unknown-name"},
            {"name": temporary, "reason": "unknown-name"},
            {"name": extra_suffix, "reason": "unknown-name"},
            {"name": manual, "reason": "unknown-name"},
        ],
        key=lambda item: (item["name"], item["reason"]),
    )


def test_future_classification_is_symmetric_for_incomplete_recognized_objects() -> None:
    module = load_planner()
    future_dump = pair("20260701T031500Z")[0]
    future_checksum = pair("20260702T031500Z")[1]

    plan = module.plan_inventory(
        [future_dump, future_checksum],
        mode="local",
        prefix=PREFIX,
        now=NOW,
    )

    assert plan["protected"] == [
        {"name": future_dump, "reason": "future-timestamp"},
        {"name": future_checksum, "reason": "future-timestamp"},
    ]


def test_decision_is_deterministic_across_input_order_and_duplicates() -> None:
    module = load_planner()
    unique_names = (
        pair("20260627T031500Z")
        + pair("20260628T031500Z")
        + pair("20260629T031500Z")
        + ["unknown"]
    )
    names_with_duplicates = unique_names + unique_names[:3] + ["unknown"]
    kwargs = {
        "mode": "remote",
        "prefix": PREFIX,
        "now": NOW,
        "daily": 1,
        "weekly": 1,
        "monthly": 1,
    }

    unique_plan = module.plan_inventory(unique_names, **kwargs)
    assert module.plan_inventory(names_with_duplicates, **kwargs) == unique_plan
    assert module.plan_inventory(reversed(names_with_duplicates), **kwargs) == unique_plan


@pytest.mark.parametrize(
    ("mode", "policy"),
    [
        ("local", {"local_count": 0}),
        ("remote", {"daily": 0, "weekly": 0, "monthly": 0}),
    ],
)
def test_zero_counts_still_keep_newest_complete_pair(mode: str, policy: dict[str, int]) -> None:
    module = load_planner()
    names = pair("20260628T031500Z") + pair("20260629T031500Z")

    plan = module.plan_inventory(names, mode=mode, prefix=PREFIX, now=NOW, **policy)

    assert plan["keep"] == [record("20260629T031500Z", ["newest"])]
    assert plan["delete"] == [record("20260628T031500Z", ["expired"])]


def test_prefix_with_regular_expression_characters_is_literal() -> None:
    module = load_planner()
    prefix = "degen.+[green](prod)_"
    exact = pair("20260629T031500Z", prefix=prefix)
    near_match = "degenZZgreenprod_20260628T031500Z.dump"

    plan = module.plan_inventory(exact + [near_match], mode="local", prefix=prefix, now=NOW)

    assert plan["keep"] == [record("20260629T031500Z", ["local-newest", "newest"], prefix=prefix)]
    assert plan["protected"] == [{"name": near_match, "reason": "unknown-name"}]


def test_cli_json_output_is_exact_compact_sorted_json() -> None:
    names = (
        pair("20260627T031500Z")
        + pair("20260628T031500Z")
        + pair("20260629T031500Z")
        + ["manual-preserve.dump"]
    )
    expected = {
        "mode": "local",
        "prefix": PREFIX,
        "keep": [record("20260629T031500Z", ["local-newest", "newest"])],
        "delete": [
            record("20260627T031500Z", ["expired"]),
            record("20260628T031500Z", ["expired"]),
        ],
        "protected": [{"name": "manual-preserve.dump", "reason": "unknown-name"}],
    }

    result = run_cli(
        "--mode",
        "local",
        "--prefix",
        PREFIX,
        "--now",
        NOW_STAMP,
        "--local-count",
        "1",
        "--format",
        "json",
        names=names,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == json.dumps(expected, sort_keys=True, separators=(",", ":")) + "\n"


def test_cli_delete_names_outputs_oldest_pairs_first() -> None:
    names = pair("20260627T031500Z") + pair("20260628T031500Z") + pair("20260629T031500Z")
    expected_names = pair("20260627T031500Z") + pair("20260628T031500Z")

    result = run_cli(
        "--mode",
        "local",
        "--prefix",
        PREFIX,
        "--now",
        NOW_STAMP,
        "--local-count",
        "1",
        "--format",
        "delete-names",
        names=names,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == "\n".join(expected_names) + "\n"


def test_cli_keep_names_emits_newest_complete_pairs_first() -> None:
    names = pair("20260628T031500Z") + pair("20260629T031500Z")
    expected_names = pair("20260629T031500Z") + pair("20260628T031500Z")

    result = run_cli(
        "--mode",
        "local",
        "--prefix",
        PREFIX,
        "--now",
        "20260630T000000Z",
        "--local-count",
        "2",
        "--format",
        "keep-names",
        names=names,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.splitlines() == expected_names


def test_cli_keep_and_delete_views_partition_complete_nonfuture_pairs() -> None:
    current = pair("20260629T230000Z")
    prior = pair("20260628T230000Z")
    expired = pair("20260627T230000Z")
    future = pair("20260630T230000Z")
    names = expired + prior + current + future
    common = (
        "--mode",
        "local",
        "--prefix",
        PREFIX,
        "--now",
        NOW_STAMP,
        "--local-count",
        "2",
    )

    kept = run_cli(*common, "--format", "keep-names", names=names)
    deleted = run_cli(*common, "--format", "delete-names", names=names)

    assert kept.returncode == deleted.returncode == 0
    keep_names = kept.stdout.splitlines()
    delete_names = deleted.stdout.splitlines()
    assert keep_names == current + prior
    assert delete_names == expired
    assert set(keep_names).isdisjoint(delete_names)
    assert not set(future) & (set(keep_names) | set(delete_names))


def test_cli_invalid_now_uses_concise_argparse_error() -> None:
    result = run_cli(
        "--mode",
        "local",
        "--prefix",
        PREFIX,
        "--now",
        "not-a-timestamp",
        names=[],
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert "--now must be a valid UTC timestamp in YYYYMMDDTHHMMSSZ format" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize("option", ["--local-count", "--daily", "--weekly", "--monthly"])
def test_cli_negative_counts_use_concise_argparse_error(option: str) -> None:
    result = run_cli(
        "--mode",
        "local",
        "--prefix",
        PREFIX,
        option,
        "-1",
        names=[],
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert f"argument {option}: must be non-negative" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize("field", ["local_count", "daily", "weekly", "monthly"])
def test_negative_policy_counts_are_rejected(field: str) -> None:
    module = load_planner()
    policy = {"local_count": 2, "daily": 7, "weekly": 4, "monthly": 3}
    policy[field] = -1

    with pytest.raises(ValueError, match=rf"^{field} must be non-negative$"):
        module.plan_inventory([], mode="local", prefix=PREFIX, now=NOW, **policy)


def test_naive_now_is_rejected() -> None:
    module = load_planner()

    with pytest.raises(ValueError, match=r"^now must be timezone-aware$"):
        module.plan_inventory(
            pair("20260629T031500Z"),
            mode="local",
            prefix=PREFIX,
            now=datetime(2026, 6, 29, 23, 0),
        )


def test_invalid_mode_is_rejected_by_api_and_cli() -> None:
    module = load_planner()

    with pytest.raises(ValueError, match=r"^mode must be local or remote$"):
        module.plan_inventory([], mode="archive", prefix=PREFIX, now=NOW)

    result = run_cli("--mode", "archive", "--prefix", PREFIX, names=[])
    assert result.returncode != 0
    assert result.stdout == ""
    assert "invalid choice" in result.stderr
