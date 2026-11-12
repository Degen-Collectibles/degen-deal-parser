"""Validate the public deployment health response from stdin; no secrets or writes."""
import json
import sys


def healthy(payload):
    return isinstance(payload, dict) and all(payload.get(key) is True
        for key in ('ok', 'db_ok', 'loyalty_ready'))


if __name__ == '__main__':
    try:
        passed = healthy(json.load(sys.stdin))
    except (ValueError, TypeError):
        passed = False
    raise SystemExit(0 if passed else 1)
