import json
import asyncio
import re
from typing import Any, Dict, List

from openai import OpenAI

from .config import get_settings

settings = get_settings()

MODEL = "gpt-5-nano"
client_openai = OpenAI(
    api_key=settings.openai_api_key,
    timeout=60.0,
)

api_semaphore = asyncio.Semaphore(3)


class TimedOutRowError(Exception):
    pass


def is_image_url(url: str) -> bool:
    url_lower = url.lower()
    return any(ext in url_lower for ext in [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"])


def choose_image_urls(urls: List[str], use_first_image_only: bool = True, max_images: int = 2) -> List[str]:
    image_urls = [u for u in urls if is_image_url(u)]
    if use_first_image_only:
        return image_urls[:1]
    return image_urls[:max_images]


def parse_trade_hint(message_text: str) -> Dict[str, Any] | None:
    text = (message_text or "").strip()
    lower = text.lower()

    if not lower:
        return None

    has_in = bool(re.search(r"\b(in)\b", lower))
    has_out = bool(re.search(r"\b(out)\b", lower))
    has_trade_word = "trade" in lower

    # Only treat as a trade shortcut if it clearly looks like trade flow
    if not ((has_in and has_out) or has_trade_word):
        return None

    payment_match = re.search(
        r"(?:plus|\+)\s*(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap)?",
        lower,
        re.I,
    )

    amount = None
    payment = "unknown"
    cash_direction = "none"

    if payment_match:
        try:
            amount = float(payment_match.group(1))
        except Exception:
            amount = None

        payment_raw = (payment_match.group(2) or "").lower()
        if payment_raw == "tap":
            payment = "card"
        else:
            payment = payment_raw or "unknown"

        # Your store convention:
        # "plus 50 zelle" during a trade means the store receives $50
        cash_direction = "to_store"

    items_in = []
    items_out = []

    # Parse side-based shorthand
    if "top out" in lower:
        items_out.append("top case items")
    if "bottom out" in lower:
        items_out.append("bottom case items")
    if "left side out" in lower or "left out" in lower:
        items_out.append("left side items")
    if "right side out" in lower or "right out" in lower:
        items_out.append("right side items")

    if "top in" in lower:
        items_in.append("top case items")
    if "bottom in" in lower:
        items_in.append("bottom case items")
    if "left side in" in lower or "left in" in lower:
        items_in.append("left side items")
    if "right side in" in lower or "right in" in lower:
        items_in.append("right side items")

    # More general category hints
    if "singles in" in lower:
        items_in.append("singles")
    if "singles out" in lower:
        items_out.append("singles")
    if "packs in" in lower:
        items_in.append("packs")
    if "packs out" in lower:
        items_out.append("packs")
    if "slabs in" in lower:
        items_in.append("slabs")
    if "slabs out" in lower:
        items_out.append("slabs")
    if "graded guard in" in lower:
        items_in.append("graded guard")
    if "graded guard out" in lower:
        items_out.append("graded guard")

    category = "mixed"
    if items_in == ["slabs"] and not items_out:
        category = "slabs"
    elif items_out == ["slabs"] and not items_in:
        category = "slabs"
    elif items_in == ["singles"] and not items_out:
        category = "singles"
    elif items_out == ["singles"] and not items_in:
        category = "singles"
    elif items_in == ["packs"] and not items_out:
        category = "packs"
    elif items_out == ["packs"] and not items_in:
        category = "packs"

    trade_summary_parts = []
    if items_out:
        trade_summary_parts.append(f"out: {', '.join(items_out)}")
    if items_in:
        trade_summary_parts.append(f"in: {', '.join(items_in)}")
    if amount is not None:
        trade_summary_parts.append(f"plus ${amount:g} {payment}")

    return {
        "parsed_type": "trade",
        "parsed_amount": amount,
        "parsed_payment_method": payment,
        "parsed_cash_direction": cash_direction,
        "parsed_category": category,
        "parsed_items": [],
        "parsed_items_in": items_in,
        "parsed_items_out": items_out,
        "parsed_trade_summary": " | ".join(trade_summary_parts) if trade_summary_parts else "trade detected from in/out wording",
        "parsed_notes": "rule-based trade parse",
        "image_summary": "no image used",
        "confidence": 0.96,
        "needs_review": False if has_in and has_out else True,
    }


def parse_by_rules(message_text: str) -> Dict[str, Any] | None:
    text = (message_text or "").strip()

    trade_hint = parse_trade_hint(text)
    if trade_hint:
        return trade_hint
    lower = text.lower()

    amount_first_match = re.fullmatch(
        r"\$?\s*(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap)",
        lower,
        re.I,
    )

    payment_first_match = re.fullmatch(
        r"(zelle|venmo|paypal|cash|card|tap)\s*\$?\s*(\d+(?:\.\d{1,2})?)",
        lower,
        re.I,
    )

    if amount_first_match or payment_first_match:
        if amount_first_match:
            amount = float(amount_first_match.group(1))
            payment = amount_first_match.group(2).lower()
        else:
            payment = payment_first_match.group(1).lower()
            amount = float(payment_first_match.group(2))

        if payment == "tap":
            payment = "card"

        return {
            "parsed_type": "sell",
            "parsed_amount": amount,
            "parsed_payment_method": payment,
            "parsed_cash_direction": "to_store",
            "parsed_category": "unknown",
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "rule-based payment-only sell default",
            "image_summary": "no image used",
            "confidence": 0.85,
            "needs_review": False,
        }
    patterns = [
        re.compile(r"\b(sold|sell)\b\s*\$?(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap)?", re.I),
        re.compile(r"\b(bought|buy|paid)\b\s*\$?(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap)?", re.I),
        re.compile(r"\btrade\b", re.I),
    ]

    for pat in patterns:
        m = pat.search(text)
        if not m:
            continue

        verb = (m.group(1) or "").lower() if m.lastindex and m.lastindex >= 1 else ""
        amount = None
        payment = None

        if m.lastindex and m.lastindex >= 2:
            try:
                amount = float(m.group(2))
            except Exception:
                amount = None

        if m.lastindex and m.lastindex >= 3:
            payment = (m.group(3) or "").lower() or None
            if payment == "tap":
                payment = "card"

        if "sold" in verb or "sell" in verb:
            parsed_type = "sell"
            cash_direction = "to_store"
        elif "buy" in verb or "bought" in verb or "paid" in verb:
            parsed_type = "buy"
            cash_direction = "from_store"
        elif "trade" in text.lower():
            parsed_type = "trade"
            cash_direction = "none"
        else:
            parsed_type = "unknown"
            cash_direction = "unknown"

        return {
            "parsed_type": parsed_type,
            "parsed_amount": amount,
            "parsed_payment_method": payment or "unknown",
            "parsed_cash_direction": cash_direction,
            "parsed_category": "unknown",
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "rule-based fallback parse",
            "image_summary": "no image used",
            "confidence": 0.70,
            "needs_review": True,
        }

    return None


def build_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "parsed_type": {
                "type": ["string", "null"],
                "enum": ["buy", "sell", "trade", "unknown", None]
            },
            "parsed_amount": {
                "type": ["number", "null"]
            },
            "parsed_payment_method": {
                "type": ["string", "null"],
                "enum": ["cash", "zelle", "venmo", "paypal", "card", "trade", "unknown", None]
            },
            "parsed_cash_direction": {
                "type": ["string", "null"],
                "enum": ["to_store", "from_store", "none", "unknown", None]
            },
            "parsed_category": {
                "type": ["string", "null"],
                "enum": ["slabs", "singles", "sealed", "packs", "mixed", "accessories", "unknown", None]
            },
            "parsed_items": {
                "type": "array",
                "items": {"type": "string"}
            },
            "parsed_items_in": {
                "type": "array",
                "items": {"type": "string"}
            },
            "parsed_items_out": {
                "type": "array",
                "items": {"type": "string"}
            },
            "parsed_trade_summary": {
                "type": "string"
            },
            "parsed_notes": {
                "type": "string"
            },
            "image_summary": {
                "type": "string"
            },
            "confidence": {
                "type": "number"
            },
            "needs_review": {
                "type": "boolean"
            }
        },
        "required": [
            "parsed_type",
            "parsed_amount",
            "parsed_payment_method",
            "parsed_cash_direction",
            "parsed_category",
            "parsed_items",
            "parsed_items_in",
            "parsed_items_out",
            "parsed_trade_summary",
            "parsed_notes",
            "image_summary",
            "confidence",
            "needs_review"
        ]
    }


def build_prompt(author_name: str, message_text: str, rule_hint: Dict[str, Any] | None, has_images: bool) -> str:
    hint_block = ""
    if rule_hint:
        hint_block = f"""
Optional weak hint from a rule parser:
- parsed_type: {rule_hint.get("parsed_type")}
- parsed_amount: {rule_hint.get("parsed_amount")}
- parsed_payment_method: {rule_hint.get("parsed_payment_method")}
- parsed_cash_direction: {rule_hint.get("parsed_cash_direction")}

Use this only if it matches the actual stitched conversation and image(s).
""".strip()

    image_block = (
        "There are attached images. Use them to identify category, visible items, and trade direction when possible."
        if has_images else
        "There are no usable images. Infer only from the stitched text."
    )

    return f"""
You are parsing a Discord deal log for a trading card store.

Important: the input may contain MULTIPLE nearby Discord messages stitched together into one deal.
Treat the full stitched sequence as one transaction unless it is clearly describing separate transactions.

How stitched input works:
- The text may look like:
  Message 1: ...
  Message 2: ...
  Message 3: ...
- One message may only contain images.
- One message may only contain payment details like "zelle $11" or "+30 tap".
- One later message may clarify an earlier message.
- Use the FULL stitched sequence and all attached images together.

Store-specific trade conventions:
- "out" means items leaving the store
- "in" means items coming into the store
- "top out bottom in" means a trade where top case items go out and bottom case items come in
- "plus 195 zelle" or "+ 195 zelle" in a trade usually means the store is receiving $195 unless wording clearly says otherwise
- "tap" usually means card payment

Interpret shorthand intelligently:
- "bought 40 cash"
- "sold psa 10 zard 220"
- "guy came in sold us binder 300 zelle"
- "trade + 50 cash"
- "picked up 10 sleeved packs"
- "sold 2 slabs 140 venmo"
- image in one message, payment in another
- trade notes split across 2-3 messages

Rules:
- Infer the deal even if wording is shorthand or inconsistent.
- If the sequence implies the store bought something, parsed_type should usually be "buy".
- If the sequence implies the store sold something, parsed_type should usually be "sell".
- If the sequence implies items in and out, parsed_type should usually be "trade".
- If truly unclear, use "unknown".
- Extract one clear cash amount if present.
- parsed_cash_direction must be:
  - "to_store" if the store receives money
  - "from_store" if the store pays money
  - "none" if no cash is involved
  - "unknown" if unclear
- Payment method must be one of:
  cash / zelle / venmo / paypal / card / trade / unknown
- parsed_category must be one of:
  slabs / singles / sealed / packs / mixed / accessories / unknown
- If multiple categories are clearly involved, use "mixed".
- parsed_items should contain clear named products/cards if visible or stated.
- parsed_items_in should list items coming into the store.
- parsed_items_out should list items leaving the store.
- parsed_trade_summary should briefly describe the trade flow.
- parsed_notes should be a short overall summary of the stitched transaction.
- image_summary should describe what is visible, or say "no image used" if none was used.
- confidence should be between 0 and 1.
- needs_review should be true if shorthand is ambiguous or confidence < 0.85.
- If the text contains both "in" and "out" describing store item flow, classify it as "trade", not "buy" or "sell".
- If a message is only an amount plus payment method, default it to a sell unless other context indicates otherwise.
{image_block}

Author: {author_name}
Stitched transaction text:
{message_text}

{hint_block}
""".strip()


def parse_deal_with_ai(
    author_name: str,
    message_text: str,
    image_urls: List[str] | None = None
) -> Dict[str, Any]:
    image_urls = image_urls or []
    rule_hint = parse_by_rules(message_text)
    schema = build_schema()
    prompt = build_prompt(
        author_name=author_name,
        message_text=message_text,
        rule_hint=rule_hint,
        has_images=bool(image_urls),
    )

    content = [{"type": "input_text", "text": prompt}]

    for url in image_urls:
        content.append({
            "type": "input_image",
            "image_url": url,
            "detail": "auto"
        })

    response = client_openai.responses.create(
        model=MODEL,
        input=[{
            "role": "user",
            "content": content
        }],
        text={
            "format": {
                "type": "json_schema",
                "name": "deal_parse",
                "schema": schema,
                "strict": True,
            }
        },
    )

    return json.loads(response.output_text)


async def parse_deal_with_ai_async(
    author_name: str,
    message_text: str,
    image_urls: List[str] | None = None
) -> Dict[str, Any]:
    async with api_semaphore:
        try:
            return await asyncio.to_thread(
                parse_deal_with_ai,
                author_name,
                message_text,
                image_urls
            )
        except Exception as e:
            error_text = str(e).lower()
            if "timed out" in error_text or "timeout" in error_text:
                raise TimedOutRowError(str(e))
            raise


async def parse_message(content: str, attachment_urls: list[str], author_name: str = "") -> Dict[str, Any]:
    image_urls = choose_image_urls(
        attachment_urls,
        use_first_image_only=True,
        max_images=2,
    )

    try:
        return await parse_deal_with_ai_async(
            author_name=author_name,
            message_text=content or "",
            image_urls=image_urls,
        )
    except TimedOutRowError:
        raise
    except Exception:
        fallback = parse_by_rules(content or "")
        if fallback:
            return fallback
        raise