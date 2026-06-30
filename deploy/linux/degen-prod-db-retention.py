#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable


def _stamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _record(dump: str, timestamp: datetime, reasons: set[str]) -> dict[str, object]:
    return {
        "dump": dump,
        "checksum": f"{dump}.sha256",
        "timestamp": timestamp.strftime("%Y%m%dT%H%M%SZ"),
        "reasons": sorted(reasons),
    }


def plan_inventory(
    names: Iterable[str],
    *,
    mode: str,
    prefix: str,
    now: datetime,
    local_count: int = 2,
    daily: int = 7,
    weekly: int = 4,
    monthly: int = 3,
) -> dict[str, object]:
    if mode not in {"local", "remote"}:
        raise ValueError("mode must be local or remote")
    for label, value in {
        "local_count": local_count,
        "daily": daily,
        "weekly": weekly,
        "monthly": monthly,
    }.items():
        if value < 0:
            raise ValueError(f"{label} must be non-negative")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    unique = sorted({name.rstrip("\r\n") for name in names if name.rstrip("\r\n")})
    pattern = re.compile(rf"^{re.escape(prefix)}(?P<stamp>\d{{8}}T\d{{6}}Z)\.dump$")
    dumps: dict[str, tuple[str, datetime]] = {}
    checksums: set[str] = set()
    recognized: dict[str, datetime] = {}
    protected: list[dict[str, str]] = []

    for name in unique:
        base = name[:-7] if name.endswith(".sha256") else name
        match = pattern.fullmatch(base)
        if not match:
            protected.append({"name": name, "reason": "unknown-name"})
            continue
        try:
            parsed = _stamp(match.group("stamp"))
        except ValueError:
            protected.append({"name": name, "reason": "unparseable-timestamp"})
            continue
        recognized[name] = parsed
        if name.endswith(".sha256"):
            checksums.add(name)
        else:
            dumps[name] = (match.group("stamp"), parsed)

    complete: list[tuple[str, str, datetime]] = []
    future_names = {name for name, parsed in recognized.items() if parsed > now}
    for dump, (stamp, parsed) in dumps.items():
        checksum = f"{dump}.sha256"
        if dump not in future_names and checksum in checksums:
            complete.append((dump, stamp, parsed))

    for name in sorted(future_names):
        protected.append({"name": name, "reason": "future-timestamp"})

    complete_names = {
        name
        for dump, _, _ in complete
        for name in (dump, f"{dump}.sha256")
    }
    paired_or_future = complete_names | future_names
    for name in sorted(set(recognized) - paired_or_future):
        protected.append({"name": name, "reason": "incomplete-pair"})

    complete.sort(key=lambda item: (item[2], item[0]), reverse=True)
    reasons: dict[str, set[str]] = defaultdict(set)
    if complete:
        reasons[complete[0][0]].add("newest")

    if mode == "local":
        for dump, _, _ in complete[:local_count]:
            reasons[dump].add("local-newest")
    else:
        group_specs = [
            ("daily", daily, lambda value: value.date()),
            ("weekly", weekly, lambda value: value.isocalendar()[:2]),
            ("monthly", monthly, lambda value: (value.year, value.month)),
        ]
        for reason, count, key in group_specs:
            seen: set[object] = set()
            for dump, _, parsed in complete:
                bucket = key(parsed)
                if bucket in seen:
                    continue
                if len(seen) >= count:
                    break
                seen.add(bucket)
                reasons[dump].add(reason)

    keep = [
        _record(dump, parsed, reasons[dump])
        for dump, _, parsed in complete
        if dump in reasons
    ]
    delete = [
        _record(dump, parsed, {"expired"})
        for dump, _, parsed in reversed(complete)
        if dump not in reasons
    ]
    return {
        "mode": mode,
        "prefix": prefix,
        "keep": keep,
        "delete": delete,
        "protected": sorted(protected, key=lambda item: (item["name"], item["reason"])),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("local", "remote"), required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--now")
    parser.add_argument("--local-count", type=_non_negative_int, default=2)
    parser.add_argument("--daily", type=_non_negative_int, default=7)
    parser.add_argument("--weekly", type=_non_negative_int, default=4)
    parser.add_argument("--monthly", type=_non_negative_int, default=3)
    parser.add_argument("--format", choices=("json", "delete-names", "keep-names"), default="json")
    args = parser.parse_args(argv)
    try:
        now = _stamp(args.now) if args.now else datetime.now(timezone.utc)
    except ValueError:
        parser.error("--now must be a valid UTC timestamp in YYYYMMDDTHHMMSSZ format")
    try:
        plan = plan_inventory(
            sys.stdin,
            mode=args.mode,
            prefix=args.prefix,
            now=now,
            local_count=args.local_count,
            daily=args.daily,
            weekly=args.weekly,
            monthly=args.monthly,
        )
    except ValueError as exc:
        parser.error(str(exc))
    if args.format in {"delete-names", "keep-names"}:
        key = "delete" if args.format == "delete-names" else "keep"
        for item in plan[key]:
            print(item["dump"])
            print(item["checksum"])
    else:
        json.dump(plan, sys.stdout, sort_keys=True, separators=(",", ":"))
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
