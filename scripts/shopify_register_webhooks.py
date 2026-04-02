from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import httpx

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

SHOPIFY_API_VERSION = "2024-01"
DEFAULT_TOPICS = ("orders/create", "orders/updated")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register Shopify app-managed webhooks.")
    parser.add_argument(
        "--address",
        default="https://ops.degencollectibles.com/webhooks/shopify/orders",
        help="Webhook destination URL.",
    )
    parser.add_argument(
        "--api-version",
        default=SHOPIFY_API_VERSION,
        help="Shopify Admin API version to use for webhook registration.",
    )
    parser.add_argument(
        "--topic",
        action="append",
        dest="topics",
        help="Webhook topic to register. Repeat to add multiple topics. Defaults to orders/create and orders/updated.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be registered without calling Shopify.",
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def build_base_url(store_domain: str, api_version: str) -> str:
    normalized = store_domain.strip().rstrip("/")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        normalized = f"https://{normalized}"
    if normalized.startswith("http://"):
        normalized = normalized.replace("http://", "https://", 1)
    return f"{normalized}/admin/api/{api_version}"


def main() -> int:
    args = parse_args()
    store_domain = require_env("SHOPIFY_STORE_DOMAIN")
    access_token = require_env("SHOPIFY_API_KEY")
    topics = args.topics or list(DEFAULT_TOPICS)

    print(f"Store: {store_domain}")
    print(f"Destination: {args.address}")
    print(f"Topics: {', '.join(topics)}")
    print(f"API version: {args.api_version}")

    if args.dry_run:
        print("Dry run only. No webhook registrations were sent.")
        return 0

    base_url = build_base_url(store_domain, args.api_version)
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        existing_response = client.get(f"{base_url}/webhooks.json", headers=headers)
        existing_response.raise_for_status()
        existing_rows = existing_response.json().get("webhooks") or []
        existing_pairs = {
            (str(row.get("topic") or "").strip(), str(row.get("address") or "").strip())
            for row in existing_rows
        }

        for topic in topics:
            pair = (topic, args.address)
            if pair in existing_pairs:
                print(f"Already exists: {topic} -> {args.address}")
                continue

            payload = {
                "webhook": {
                    "topic": topic,
                    "address": args.address,
                    "format": "json",
                }
            }
            response = client.post(f"{base_url}/webhooks.json", headers=headers, json=payload)
            response.raise_for_status()
            webhook = response.json().get("webhook") or {}
            print(
                "Created: "
                f"{webhook.get('topic', topic)} -> {webhook.get('address', args.address)} "
                f"(id={webhook.get('id')}, api_version={webhook.get('api_version')})"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
