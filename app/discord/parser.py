import asyncio
import json
import logging
import re
from typing import Any, Dict, List

from ..ai_client import get_ai_client, get_model, get_provider, is_nvidia
from ..config import get_settings
from .corrections import get_exact_correction_match, get_learned_rule_match, get_relevant_correction_hints

logger = logging.getLogger(__name__)
settings = get_settings()

MODEL = get_model(default="gpt-5-nano")
# Public list prices per 1M tokens. NVIDIA-hosted Claude models are priced
# per Anthropic's Bedrock list rates; NVIDIA inference credits may differ.
# Adjust values here if your billing source differs.
MODEL_PRICING_PER_MILLION = {
    "gpt-5-nano": {
        "input": 0.05,
        "cached_input": 0.005,
        "output": 0.40,
    },
    "openai/openai/gpt-5.5": {
        "input": 1.25,
        "cached_input": 0.125,
        "output": 10.00,
    },
    "openai/gpt-5.5": {
        "input": 1.25,
        "cached_input": 0.125,
        "output": 10.00,
    },
    "gpt-5.5": {
        "input": 1.25,
        "cached_input": 0.125,
        "output": 10.00,
    },
    "aws/anthropic/bedrock-claude-opus-4-7": {
        "input": 15.00,
        "cached_input": 1.50,
        "output": 75.00,
    },
    "aws/anthropic/bedrock-claude-opus-4-6": {
        "input": 15.00,
        "cached_input": 1.50,
        "output": 75.00,
    },
    "aws/anthropic/claude-haiku-4-5-v1": {
        "input": 1.00,
        "cached_input": 0.10,
        "output": 5.00,
    },
}

api_semaphore = asyncio.Semaphore(3)


class TimedOutRowError(Exception):
    pass


def estimate_usage_cost_usd(
    *,
    model: str,
    input_tokens: int = 0,
    cached_input_tokens: int = 0,
    output_tokens: int = 0,
) -> float:
    pricing = MODEL_PRICING_PER_MILLION.get(model) or MODEL_PRICING_PER_MILLION.get(MODEL) or MODEL_PRICING_PER_MILLION["gpt-5-nano"]
    billable_input_tokens = max(input_tokens - cached_input_tokens, 0)
    cost = (
        (billable_input_tokens / 1_000_000) * pricing["input"] +
        (cached_input_tokens / 1_000_000) * pricing["cached_input"] +
        (output_tokens / 1_000_000) * pricing["output"]
    )
    return round(cost, 6)


def _get_field(source: Any, *names: str) -> Any:
    if source is None:
        return None
    for name in names:
        if isinstance(source, dict) and name in source:
            return source.get(name)
        value = getattr(source, name, None)
        if value is not None:
            return value
    return None


def _to_plain_mapping(source: Any) -> Any:
    if source is None or isinstance(source, (dict, list, tuple)):
        return source
    for method_name in ("model_dump", "to_dict", "dict"):
        method = getattr(source, method_name, None)
        if not callable(method):
            continue
        try:
            return method()
        except TypeError:
            continue
    return source


def _find_usage_payload(source: Any, *, depth: int = 0) -> Any:
    if source is None or depth > 4:
        return None
    usage = _get_field(source, "usage")
    if usage is not None:
        return usage
    plain = _to_plain_mapping(source)
    if plain is not source:
        found = _find_usage_payload(plain, depth=depth + 1)
        if found is not None:
            return found
    if isinstance(plain, dict):
        for value in plain.values():
            found = _find_usage_payload(value, depth=depth + 1)
            if found is not None:
                return found
    elif isinstance(plain, (list, tuple)):
        for value in plain:
            found = _find_usage_payload(value, depth=depth + 1)
            if found is not None:
                return found
    return None


def _coerce_token_count(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _first_token_count(source: Any, *names: str) -> int:
    for name in names:
        value = _get_field(source, name)
        if value is not None:
            return _coerce_token_count(value)
    return 0


def _nested_token_count(source: Any, *paths: tuple[str, ...]) -> int:
    for path in paths:
        current = source
        for key in path:
            current = _get_field(current, key)
            if current is None:
                break
        value = _coerce_token_count(current)
        if value:
            return value
    return 0


def extract_usage_metrics(response: Any, *, model: str) -> Dict[str, Any]:
    usage = _find_usage_payload(response)
    if usage is None:
        return {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "estimated_cost_usd": 0.0,
        }

    input_tokens = _first_token_count(usage, "input_tokens", "prompt_tokens")
    output_tokens = _first_token_count(usage, "output_tokens", "completion_tokens")
    total_tokens = _first_token_count(usage, "total_tokens")

    if not input_tokens:
        input_tokens = _nested_token_count(
            usage,
            ("input", "tokens"),
            ("prompt", "tokens"),
            ("input_tokens", "total_tokens"),
        )
    if not output_tokens:
        output_tokens = _nested_token_count(
            usage,
            ("output", "tokens"),
            ("completion", "tokens"),
            ("output_tokens", "total_tokens"),
        )
    if not total_tokens:
        total_tokens = input_tokens + output_tokens
    if total_tokens and not input_tokens and not output_tokens:
        input_tokens = total_tokens

    cached_input_tokens = _first_token_count(
        usage,
        "cached_input_tokens",
        "cache_read_input_tokens",
    )
    if not cached_input_tokens:
        cached_input_tokens = _nested_token_count(
            usage,
            ("input_tokens_details", "cached_tokens"),
            ("prompt_tokens_details", "cached_tokens"),
            ("input_token_details", "cached_tokens"),
            ("prompt_token_details", "cached_tokens"),
            ("input", "cached_tokens"),
            ("prompt", "cached_tokens"),
        )

    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": cached_input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "estimated_cost_usd": estimate_usage_cost_usd(
            model=model,
            input_tokens=input_tokens,
            cached_input_tokens=cached_input_tokens,
            output_tokens=output_tokens,
        ),
    }


def strip_message_prefixes(text: str) -> str:
    return re.sub(r"(?im)^\s*message\s+\d+:\s*", "", text or "")


def strip_urls(text: str) -> str:
    return re.sub(r"https?://\S+", " ", text or "", flags=re.I)


def normalize_detector_text(text: str) -> str:
    normalized = strip_message_prefixes(text)
    normalized = strip_urls(normalized)
    normalized = re.sub(r"\s+", " ", normalized or "").strip()
    return normalized


def split_stitched_messages(text: str) -> List[str]:
    raw_text = text or ""
    matches = list(re.finditer(r"(?im)^\s*message\s+\d+:\s*", raw_text))
    if not matches:
        cleaned = raw_text.strip()
        return [cleaned] if cleaned else []

    parts: List[str] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw_text)
        part = raw_text[start:end].strip()
        if part:
            parts.append(part)
    return parts


def normalize_message_part(text: str) -> str:
    cleaned = normalize_detector_text(text or "")
    if cleaned == "[no text]":
        return ""
    return cleaned


def has_no_text_placeholder(text: str) -> bool:
    return normalize_message_part(text) == ""


def first_nonempty_value(*values: Any) -> Any:
    for value in values:
        if value not in {None, "", "unknown"}:
            return value
    return None


def is_image_url(url: str) -> bool:
    url_lower = url.lower()
    if url_lower.startswith("data:image/"):
        return True
    return any(ext in url_lower for ext in [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"])


def choose_image_urls(urls: List[str], use_first_image_only: bool = True, max_images: int = 2) -> List[str]:
    image_urls = [u for u in urls if is_image_url(u)]
    if use_first_image_only:
        return image_urls[:1]
    return image_urls[:max_images]


def parse_trade_hint(message_text: str) -> Dict[str, Any] | None:
    text = (message_text or "").strip()
    lower = _normalize_payment_tokens(_normalize_amount_text(text.lower()))

    if not lower:
        return None

    has_in = bool(re.search(r"\b(in)\b", lower))
    has_out = bool(re.search(r"\b(out)\b", lower))
    has_trade_word = "trade" in lower

    # Only treat as a trade shortcut if it clearly looks like trade flow
    if not ((has_in and has_out) or has_trade_word):
        return None

    payment_match = re.search(
        r"(?:plus|\+|&)\s*\$?\s*(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap|apple_pay)?",
        lower,
        re.I,
    )
    if not payment_match:
        payment_match = re.search(
            r"\$\s*(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap|apple_pay)?",
            lower,
            re.I,
        )
    if not payment_match:
        payment_match = re.search(
            r"\b(\d+(?:\.\d{1,2})?)\s+on\s+(zelle|venmo|paypal|cash|card|tap|apple_pay)\b",
            lower,
            re.I,
        )
    if not payment_match:
        payment_match = re.search(
            r"\b(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap|apple_pay)?\s*(?:on\s+top|top)\b",
            lower,
            re.I,
        )

    amount = None
    payment = "unknown"
    cash_direction = "none"

    if payment_match:
        try:
            amount = float(payment_match.group(1))
        except Exception as e:
            logger.warning(
                "parser.parse_trade_hint: float(payment_match group) failed, amount=None: %s",
                e,
                exc_info=True,
            )
            amount = None

        payment_raw = (payment_match.group(2) or "").lower()
        if payment_raw == "tap":
            payment = "card"
        else:
            payment = payment_raw or "unknown"

        # Store convention: "plus 50 zelle", "+ 50", "& $100" during a trade
        # means the store receives that amount
        cash_direction = "to_store"

    items_in = []
    items_out = []

    # Parse side-based shorthand
    if "top out" in lower or re.search(r"\btop\b.*?\b(?:store\s+)?credit\s+out\b", lower, re.I):
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

    credit_out_match = re.search(
        r"\b(\d+(?:\.\d{1,2})?)\s*(?:store\s+)?credit\s+out\b",
        lower,
        re.I,
    )
    if credit_out_match:
        items_out.append(f"${float(credit_out_match.group(1)):g} store credit")

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
    if re.search(r"\betb(?:s)?\s+out\b", lower):
        items_out.append("etb")
    if re.search(r"\betb(?:s)?\s+in\b", lower):
        items_in.append("etb")
    if re.search(r"\bbox(?:es)?\s+out\b", lower):
        items_out.append("booster boxes")
    if re.search(r"\bbox(?:es)?\s+in\b", lower):
        items_in.append("booster boxes")
    if re.search(r"\bbundle(?:s)?\s+out\b", lower):
        items_out.append("bundles")
    if re.search(r"\bbundle(?:s)?\s+in\b", lower):
        items_in.append("bundles")

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

    # Require at least one concrete trade signal to avoid misfiring on
    # casual text like "I'll be in and out all day" or "in stock, shipping
    # out tomorrow" where `has_in and has_out` is incidentally True.
    captured_anything = bool(items_in or items_out or amount is not None)
    explicit_trade_word = has_trade_word
    if not captured_anything and not explicit_trade_word:
        return None

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


def extract_payment_amount_method(text: str) -> tuple[float | None, str | None]:
    lower = _normalize_payment_tokens(_normalize_amount_text(normalize_message_part(text).lower()))
    if not lower:
        return None, None

    patterns = [
        r"^(?:plus|\+)?\s*\$?\s*(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)$",
        r"^(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)\s*\$?\s*(\d+(?:\.\d{1,2})?)$",
    ]

    for pattern in patterns:
        match = re.fullmatch(pattern, lower, re.I)
        if not match:
            continue
        try:
            if match.group(1).replace(".", "", 1).isdigit():
                amount = float(match.group(1))
                payment = match.group(2).lower()
            else:
                payment = match.group(1).lower()
                amount = float(match.group(2))
        except Exception as e:
            logger.warning(
                "parser.extract_payment_amount_method: parse failed, returning None: %s",
                e,
                exc_info=True,
            )
            return None, None
        return amount, normalize_payment_method(payment)

    return None, None


QUANTITY_UNITS = (
    "box",
    "boxes",
    "booster",
    "booster box",
    "booster boxes",
    "pack",
    "packs",
    "slab",
    "slabs",
    "case",
    "cases",
    "card",
    "cards",
    "binder",
    "binders",
    "lot",
    "lots",
)

GRADE_WORDS = ("psa", "bgs", "sgc", "cgc", "grade")


_APPLE_PAY_RE = re.compile(r'\bapple\s+pay\b|\bapplepay\b|\bappstd\b', re.I)
_PAYPAL_RE = re.compile(r'\bpay\s*pal\b', re.I)
_ZELLED_RE = re.compile(r'\bzelled\b', re.I)
_REGISTER_RE = re.compile(r'\bregister\b', re.I)


def _normalize_payment_tokens(text: str) -> str:
    """Collapse common payment variants to canonical one-word tokens."""
    normalized = _APPLE_PAY_RE.sub('apple_pay', text)
    normalized = _PAYPAL_RE.sub('paypal', normalized)
    normalized = _ZELLED_RE.sub('zelle', normalized)
    normalized = _REGISTER_RE.sub('card', normalized)
    return normalized


def normalize_payment_method(payment_method: str) -> str:
    if payment_method in {"tap", "cc", "dc"}:
        return "card"
    if payment_method in {"apple pay", "applepay", "appstd", "apple_pay"}:
        return "apple_pay"
    return payment_method


def _normalize_amount_text(text: str) -> str:
    """Normalize number tokens so the amount regex works on shorthand input.

    Applied at the start of every amount extractor. Two transformations:

    1. Strip thousand-separator commas. ``$11,050`` -> ``$11050``.
       Without this, ``\\d+`` stops at the comma and we keep only
       ``050`` (= 50), which is how ``$11,050 bought 13 cases`` got
       parsed as ``$50``.

    2. Expand ``k`` and ``M`` suffixes to their full integers.
       ``6k`` -> ``6000``, ``1.5k`` -> ``1500``, ``2M`` -> ``2000000``.
       Without this, ``Give company 6k cash`` was parsed as ``$6``.

    The regex-based extractors downstream are left unchanged, so the
    only new surface area is this pre-processing pass.
    """
    if not text:
        return text
    # Collapse thousand-separator commas. Two passes handle $1,250,000
    # and other multi-group numbers.
    normalized = re.sub(r"(\d),(\d{3})\b", r"\1\2", text)
    normalized = re.sub(r"(\d),(\d{3})\b", r"\1\2", normalized)
    normalized = re.sub(r"(?<=\d)\s*\$(?=\s|$)", "", normalized)

    def _expand(m: re.Match) -> str:
        num = float(m.group(1))
        multiplier = 1000 if m.group(2).lower() == "k" else 1_000_000
        result = num * multiplier
        return str(int(result)) if result == int(result) else f"{result:.2f}"

    return re.sub(r"(\d+(?:\.\d+)?)([km])\b", _expand, normalized, flags=re.I)


def is_payment_method_only_message_text(text: str) -> bool:
    lower = _normalize_payment_tokens(normalize_message_part(text).lower())
    return bool(re.fullmatch(r"(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)", lower, re.I))


def extract_bare_amount_message(text: str) -> float | None:
    lower = _normalize_amount_text(normalize_message_part(text).lower()).strip()
    match = re.fullmatch(r"(\$?)\s*(\d+(?:\.\d{1,2})?)", lower)
    if not match:
        return None
    if not match.group(1) and len(match.group(2).split(".", 1)[0]) < 2:
        return None
    return float(match.group(2))


def extract_payment_segments(text: str) -> list[tuple[float, str]]:
    lower = _normalize_payment_tokens(_normalize_amount_text(normalize_message_part(text).lower()))
    if not lower:
        return []

    patterns = [
        r"(?<![#\w])(?:plus|\+)?\s*\$?\s*(\d+(?:\.\d{1,2})?)\s*(cash|zelle|venmo|paypal|card|tap|cc|dc|apple_pay)\b",
        r"\b(cash|zelle|venmo|paypal|card|tap|cc|dc|apple_pay)\s*\$?\s*(\d+(?:\.\d{1,2})?)\b",
    ]

    candidate_segments: list[tuple[int, int, float, str]] = []
    for pattern in patterns:
        for match in re.finditer(pattern, lower, re.I):
            if match.group(1).replace(".", "", 1).isdigit():
                if has_quantity_multiplier_before(lower, match.start(1)):
                    continue
                amount = float(match.group(1))
                method = normalize_payment_method(match.group(2).lower())
            else:
                if has_quantity_multiplier_before(lower, match.start(2)):
                    continue
                method = normalize_payment_method(match.group(1).lower())
                amount = float(match.group(2))
            start, end = match.span()
            candidate_segments.append((start, end, amount, method))

    segments: list[tuple[float, str]] = []
    seen: set[tuple[str, str]] = set()
    accepted_spans: list[tuple[int, int]] = []
    for start, end, amount, method in sorted(candidate_segments, key=lambda item: (item[0], item[1])):
        if any(start < accepted_end and end > accepted_start for accepted_start, accepted_end in accepted_spans):
            continue
        key = (f"{amount:.2f}", method)
        if key in seen:
            continue
        seen.add(key)
        accepted_spans.append((start, end))
        segments.append((amount, method))

    return segments


def has_quantity_unit_after(text: str, number_end: int) -> bool:
    lower = normalize_message_part(text).lower()
    after = lower[number_end:]
    return bool(re.match(r"\s*(?:x\s*)?(?:%s)\b" % "|".join(re.escape(unit) for unit in QUANTITY_UNITS), after, re.I))


def has_quantity_multiplier_after(text: str, number_end: int) -> bool:
    lower = normalize_message_part(text).lower()
    after = lower[number_end:]
    return bool(re.match(r"\s*x\b", after, re.I))


def has_grade_context_before(text: str, number_start: int) -> bool:
    lower = normalize_message_part(text).lower()
    before = lower[max(0, number_start - 12):number_start]
    return any(re.search(rf"\b{re.escape(word)}\s*$", before, re.I) for word in GRADE_WORDS)


def has_quantity_multiplier_before(text: str, number_start: int) -> bool:
    lower = normalize_message_part(text).lower()
    before = lower[max(0, number_start - 16):number_start]
    return bool(re.search(r"(?:^|\s)\d+\s*x\s*$", before, re.I))


def extract_unlabeled_amount(text: str) -> float | None:
    lower = _normalize_amount_text(normalize_message_part(text).lower())
    if not lower:
        return None

    candidates: list[tuple[bool, float]] = []
    for match in re.finditer(r"\$?\d+(?:\.\d{1,2})?", lower):
        raw_token = match.group(0)
        token = raw_token.replace("$", "")
        try:
            amount = float(token)
        except Exception as e:
            logger.warning(
                "parser.extract_unlabeled_amount: float(%r) skipped: %s",
                token,
                e,
                exc_info=True,
            )
            continue
        if has_quantity_unit_after(lower, match.end()):
            continue
        if has_grade_context_before(lower, match.start()):
            continue
        if has_quantity_multiplier_before(lower, match.start()):
            continue
        if has_quantity_multiplier_after(lower, match.end()):
            continue
        candidates.append((raw_token.startswith("$"), amount))

    if not candidates:
        return None

    currency_candidates = [amount for has_currency, amount in candidates if has_currency]
    if currency_candidates:
        return currency_candidates[-1]
    return candidates[-1][1]


def extract_payment_summary(text: str) -> dict[str, Any] | None:
    segments = extract_payment_segments(text)
    return summarize_payment_breakdown(segments)


def summarize_payment_breakdown(segments: list[tuple[float, str]]) -> dict[str, Any] | None:
    if not segments:
        return None

    if len(segments) == 1:
        amount, method = segments[0]
        return {
            "amount": amount,
            "payment_method": method,
            "payment_breakdown": segments,
        }

    total_amount = round(sum(amount for amount, _ in segments), 2)
    return {
        "amount": total_amount,
        "payment_method": "mixed",
        "payment_breakdown": segments,
    }


def infer_remainder_payment_summary(
    text: str,
    payment_summary: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not payment_summary:
        return None

    lower = _normalize_payment_tokens(_normalize_amount_text(normalize_message_part(text).lower()))
    remainder_match = re.search(
        r"\brest\s+(cash|zelle|venmo|paypal|card|tap|cc|dc|apple_pay|register)\b",
        lower,
        re.I,
    )
    if not remainder_match:
        return None

    total_match = re.search(
        r"\b(?:buy|bought|bougjt|sell|sold)\b.*?\$?\s*(\d+(?:\.\d{1,2})?)\s*[,;]",
        lower,
        re.I,
    )
    if not total_match:
        return None

    total_amount = float(total_match.group(1))
    known_amount = round(sum(amount for amount, _ in payment_summary["payment_breakdown"]), 2)
    remainder_amount = round(total_amount - known_amount, 2)
    if remainder_amount <= 0:
        return None

    remainder_method = normalize_payment_method(remainder_match.group(1).lower())
    breakdown = [*payment_summary["payment_breakdown"], (remainder_amount, remainder_method)]
    return {
        "amount": total_amount,
        "payment_method": "mixed",
        "payment_breakdown": breakdown,
    }


def infer_category_from_text(message_text: str) -> str | None:
    lower = (message_text or "").lower()
    if not lower:
        return None
    if any(token in lower for token in ("box", "boxes", "booster box", "sealed", "case")):
        return "sealed"
    if any(token in lower for token in ("pack", "packs")):
        return "packs"
    if any(token in lower for token in ("slab", "slabs", "psa", "bgs")):
        return "slabs"
    if any(token in lower for token in ("single", "singles", "binder", "lot", "cards", "card")):
        return "singles"
    return None


def extract_multi_payment_summary(text: str) -> dict[str, Any] | None:
    summary = extract_payment_summary(text)
    if not summary or len(summary["payment_breakdown"]) < 2:
        return None
    methods = sorted({method for _, method in summary["payment_breakdown"]})
    if len(methods) < 2:
        return None
    return summary


FINANCIALS_CHANNEL_NAMES = {"financials"}
LOAN_CHANNEL_NAMES = {"loan", "loans"}
FINANCIAL_REVIEW_CATEGORY = "uncategorized"
FINANCIAL_PAYEE_AMOUNT_STOPWORDS = {
    "and",
    "april",
    "aug",
    "august",
    "card",
    "cash",
    "dec",
    "december",
    "feb",
    "february",
    "for",
    "front",
    "jan",
    "january",
    "jul",
    "july",
    "jun",
    "june",
    "mar",
    "march",
    "may",
    "nov",
    "november",
    "oct",
    "october",
    "paid",
    "pay",
    "row",
    "sent",
    "sep",
    "september",
    "show",
    "table",
    "tables",
    "tax",
    "the",
    "total",
    "zelle",
}


def _canonical_channel_leaf_name(channel_name: str | None) -> str:
    lower = (channel_name or "").strip().lower()
    if "#" in lower:
        lower = lower.rsplit("#", 1)[-1]
    return lower.strip()


def _base_channel_financial_parse(
    *,
    parsed_type: str | None,
    amount: float | None,
    payment_method: str | None,
    category: str | None,
    notes: str,
    confidence: float,
    needs_review: bool,
    ignore_message: bool = False,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "parsed_type": parsed_type,
        "parsed_amount": amount,
        "parsed_payment_method": payment_method,
        "parsed_cash_direction": None,
        "parsed_category": category,
        "parsed_items": [],
        "parsed_items_in": [],
        "parsed_items_out": [],
        "parsed_trade_summary": "",
        "parsed_notes": notes,
        "image_summary": "financial channel rule",
        "confidence": confidence,
        "needs_review": needs_review,
    }
    if ignore_message:
        payload["ignore_message"] = True
    return payload


def _financial_review_parse(
    *,
    amount: float | None,
    payment_method: str | None,
    category: str | None,
    notes: str,
) -> Dict[str, Any]:
    return _base_channel_financial_parse(
        parsed_type="unknown",
        amount=amount,
        payment_method=payment_method,
        category=category or FINANCIAL_REVIEW_CATEGORY,
        notes=notes,
        confidence=0.72,
        needs_review=True,
    )


def _financial_ignore_parse(notes: str) -> Dict[str, Any]:
    return _base_channel_financial_parse(
        parsed_type=None,
        amount=None,
        payment_method=None,
        category=None,
        notes=notes,
        confidence=0.98,
        needs_review=False,
        ignore_message=True,
    )


def _financial_amount_candidates(text: str) -> list[float]:
    cleaned = _normalize_amount_text(strip_urls(text or "").lower())
    cleaned = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", " ", cleaned)
    cleaned = re.sub(r"\b\d{1,2}\s*-\s*\d{1,2}\b", " ", cleaned)
    candidates: list[float] = []

    for match in re.finditer(r"\$?\s*(\d+(?:\.\d{1,2})?)", cleaned):
        token = match.group(1)
        try:
            amount = float(token)
        except ValueError:
            continue

        after = cleaned[match.end():match.end() + 30]
        before = cleaned[max(0, match.start() - 12):match.start()]
        if "%" in after[:2]:
            continue
        if has_quantity_unit_after(cleaned, match.end()):
            continue
        if re.match(r"\s*(?:x\b|days?\b|front\s+row\b|tables?\b)", after, re.I) and amount <= 31:
            continue
        if amount <= 31 and re.search(r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s*$", before, re.I):
            continue
        candidates.append(amount)

    return candidates


def _financial_amount_candidate_matches(text: str) -> list[re.Match[str]]:
    cleaned = _normalize_amount_text(strip_urls(text or "").lower())
    cleaned = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", " ", cleaned)
    cleaned = re.sub(r"\b\d{1,2}\s*-\s*\d{1,2}\b", " ", cleaned)
    matches: list[re.Match[str]] = []

    for match in re.finditer(r"\$?\s*(\d+(?:\.\d{1,2})?)", cleaned):
        try:
            amount = float(match.group(1))
        except ValueError:
            continue

        after = cleaned[match.end():match.end() + 30]
        before = cleaned[max(0, match.start() - 12):match.start()]
        if "%" in after[:2]:
            continue
        if has_quantity_unit_after(cleaned, match.end()):
            continue
        if re.match(r"\s*(?:x\b|days?\b|front\s+row\b|tables?\b)", after, re.I) and amount <= 31:
            continue
        if amount <= 31 and re.search(r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s*$", before, re.I):
            continue
        matches.append(match)

    return matches


def _looks_like_financial_payee_amount_list(text: str) -> bool:
    cleaned = _normalize_amount_text(strip_urls(text or "").lower())
    cleaned = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", " ", cleaned)
    cleaned = re.sub(r"\b\d{1,2}\s*-\s*\d{1,2}\b", " ", cleaned)
    payee_pair_count = 0

    for match in _financial_amount_candidate_matches(cleaned):
        prefix = cleaned[:match.start()]
        words = re.findall(r"[a-z][a-z0-9_'-]*", prefix)
        if not words:
            continue
        for token in reversed(words[-5:]):
            if token in FINANCIAL_PAYEE_AMOUNT_STOPWORDS:
                continue
            payee_pair_count += 1
            break

    return payee_pair_count >= 2


def _financial_multi_payee_amount_total(text: str) -> float | None:
    if re.search(r"\btotal\b", text or "", re.I):
        return None
    if not _looks_like_financial_payee_amount_list(text):
        return None
    candidates = _financial_amount_candidates(text)
    if len(candidates) < 2:
        return None
    return round(sum(candidates), 2)


def _financial_each_payee_amount_total(text: str) -> float | None:
    cleaned = _normalize_amount_text(strip_urls(text or "").lower())
    cleaned = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", " ", cleaned)
    cleaned = re.sub(r"\b\d{1,2}\s*-\s*\d{1,2}\b", " ", cleaned)
    match = re.search(r"\$?\s*(\d+(?:\.\d{1,2})?)\s*each\b", cleaned)
    if not match:
        return None

    try:
        amount = float(match.group(1))
    except ValueError:
        return None

    prefix = cleaned[:match.start()]
    payee_part = re.split(r"\b(?:pay|paid|sent|zelle)\b", prefix)[-1]
    payee_part = re.split(
        r"\b(?:for|show|card|today|tonight|sunday|monday|tuesday|wednesday|thursday|friday|saturday)\b",
        payee_part,
        maxsplit=1,
    )[0]
    payee_tokens = [
        token
        for token in re.findall(r"[a-z][a-z0-9_'-]*", payee_part)
        if token not in FINANCIAL_PAYEE_AMOUNT_STOPWORDS
    ]
    if len(payee_tokens) < 2:
        return None

    return round(amount * len(payee_tokens), 2)


def _extract_financial_channel_amount(text: str) -> float | None:
    normalized = _normalize_amount_text(normalize_message_part(text or ""))
    if not normalized:
        return None

    total_equals = re.search(r"\btotal\b[^|=\n]*=\s*\$?\s*(\d+(?:\.\d{1,2})?)", normalized, re.I)
    if total_equals:
        return round(float(total_equals.group(1)), 2)

    total_match = re.search(r"\btotal\b([^|\n]*)", normalized, re.I)
    if total_match:
        total_candidates = _financial_amount_candidates(total_match.group(1))
        if total_candidates:
            return round(max(total_candidates), 2)

    payment_summary = extract_payment_summary(normalized)
    if payment_summary:
        return round(float(payment_summary["amount"]), 2)

    candidates = _financial_amount_candidates(normalized)
    if not candidates:
        return None
    return round(candidates[0], 2)


def _financial_channel_amount_for_category(text: str, category: str | None) -> float | None:
    if category == "payroll":
        each_payee_total = _financial_each_payee_amount_total(text)
        if each_payee_total is not None:
            return each_payee_total
        multi_payee_total = _financial_multi_payee_amount_total(text)
        if multi_payee_total is not None:
            return multi_payee_total
    return _extract_financial_channel_amount(text)


def _financial_channel_payment_method(text: str) -> str:
    summary = extract_payment_summary(text)
    if summary:
        return summary["payment_method"]

    lower = _normalize_payment_tokens((text or "").lower())
    found = [
        normalize_payment_method(method)
        for method in ("zelle", "venmo", "paypal", "cash", "card", "apple_pay")
        if re.search(rf"\b{re.escape(method)}\b", lower)
    ]
    if not found:
        return "unknown"
    return found[0] if len(set(found)) == 1 else "mixed"


def _looks_like_financial_statement_evidence(text: str) -> bool:
    lower = (text or "").lower()
    statement_hints = (
        "financial statement",
        "financials up to",
        "red tab",
        "yellow tab",
        "profit overview",
        "spreadsheet",
        "1drv.ms",
        "onedrive",
    )
    return any(hint in lower for hint in statement_hints) or bool(
        re.fullmatch(r"\s*(jan|january|feb|february|mar|march|apr|april|may|june?|july?|aug|august|sep|september|oct|october|nov|november|dec|december)\s*:\s*https?://\S+\s*", text or "", re.I)
    )


def _parse_loans_channel_message(message_text: str, image_urls: list[str]) -> Dict[str, Any]:
    text = normalize_message_part(message_text or "")
    lower = _normalize_payment_tokens(text.lower())

    if not text:
        return _financial_review_parse(
            amount=None,
            payment_method=None,
            category="loan_owner_payments",
            notes="loans channel attachment needs review" if image_urls else "loans channel blank note needs review",
        )

    if _looks_like_financial_statement_evidence(message_text):
        return _financial_ignore_parse("ignored loan statement evidence link or summary")

    if looks_like_date_marker(text):
        return _financial_ignore_parse("ignored loans channel date marker")

    amount = _extract_financial_channel_amount(text)
    payment_method = _financial_channel_payment_method(text)

    if "interest" in lower:
        if amount is None:
            return _financial_review_parse(
                amount=None,
                payment_method=payment_method,
                category="loan_interest",
                notes="loans channel interest note needs amount review",
            )
        return _base_channel_financial_parse(
            parsed_type="expense",
            amount=amount,
            payment_method=payment_method,
            category="loan_interest",
            notes="loans channel interest expense",
            confidence=0.94,
            needs_review=False,
        )

    if re.search(r"\b(paid\s+back|pay\s+back|payback|repaid|paid\s+loan|pay\s+loan)\b", lower):
        if amount is None:
            return _financial_review_parse(
                amount=None,
                payment_method=payment_method,
                category="loan_owner_payments",
                notes="loans channel repayment needs amount review",
            )
        return _base_channel_financial_parse(
            parsed_type="loan_repayment",
            amount=amount,
            payment_method=payment_method,
            category="loan_owner_payments",
            notes="loans channel principal repayment",
            confidence=0.95,
            needs_review=False,
        )

    if re.search(r"\b(take\s+out|took\s+out|loan)\b", lower):
        if amount is None:
            return _financial_review_parse(
                amount=None,
                payment_method=payment_method,
                category="loan_owner_payments",
                notes="loans channel draw needs amount review",
            )
        return _base_channel_financial_parse(
            parsed_type="loan_draw",
            amount=amount,
            payment_method=payment_method,
            category="loan_owner_payments",
            notes="loans channel principal draw",
            confidence=0.95,
            needs_review=False,
        )

    return _financial_review_parse(
        amount=amount,
        payment_method=payment_method,
        category="loan_owner_payments",
        notes="loans channel note needs review",
    )


def _parse_financials_channel_message(message_text: str, image_urls: list[str]) -> Dict[str, Any] | None:
    text = normalize_message_part(message_text or "")
    lower = _normalize_payment_tokens(text.lower())
    amount = _extract_financial_channel_amount(text)
    payment_method = _financial_channel_payment_method(text)

    if not text:
        if image_urls:
            return _financial_review_parse(
                amount=None,
                payment_method=None,
                category=FINANCIAL_REVIEW_CATEGORY,
                notes="financials channel attachment needs review",
            )
        return None

    if _looks_like_financial_statement_evidence(message_text):
        return _financial_ignore_parse("ignored financial statement evidence link or summary")

    if "withdraw" in lower:
        return _financial_review_parse(
            amount=amount,
            payment_method=payment_method,
            category="transfers",
            notes="financials channel cash withdrawal/allocation needs review",
        )

    category: str | None = None
    notes = "financials channel expense"
    needs_review = False

    if re.search(r"\b(pay\s*roll|payroll|labor|wages?|salary|commission|days?\s+work)\b", lower):
        category = "payroll"
        notes = "financials channel payroll/labor expense"
    elif re.search(r"\brent\b", lower):
        category = "rent_facilities"
        notes = "financials channel rent/facilities expense"
    elif re.search(r"\btax(?:es)?\b", lower):
        category = "taxes_licenses"
        notes = "financials channel tax/license expense"
    elif "insurance" in lower:
        category = "insurance"
        notes = "financials channel insurance expense"
    elif re.search(r"\b(table|tables|front\s+row|booth|vendor\s+fee|card\s+show|show)\b", lower):
        if (
            (
                re.search(r"\b(?:pay|paid|sent|zelle)\s+(?!rent\b|tax\b)[a-z0-9_'-]+", lower)
                or _looks_like_financial_payee_amount_list(text)
            )
            and not re.search(r"\b(table|tables|booth|vendor\s+fee)\b", lower)
        ):
            category = "payroll"
            notes = "financials channel show labor expense"
        else:
            category = "show_fees"
            notes = "financials channel show/table expense"
    elif re.search(r"\bpay\s+(?!rent\b|tax\b)[a-z0-9_'-]+", lower):
        category = "payroll"
        notes = "financials channel payee note needs payroll review"
        needs_review = True
    elif re.search(r"\b(deposit|deposited)\b", lower):
        return _financial_review_parse(
            amount=amount,
            payment_method=payment_method,
            category=FINANCIAL_REVIEW_CATEGORY,
            notes="financials channel deposit/prepayment needs review",
        )
    elif re.search(r"\b(bought|buy)\b", lower):
        return _financial_review_parse(
            amount=amount,
            payment_method=payment_method,
            category=FINANCIAL_REVIEW_CATEGORY,
            notes="financials channel purchase note needs review",
        )
    elif amount is not None and re.search(r"\b(pay|paid|sent|zelle|venmo|cash|expense|costco)\b", lower):
        category = "other_business_expense"
        notes = "financials channel business expense needs review"
        needs_review = True

    if category is None:
        if amount is not None or image_urls:
            return _financial_review_parse(
                amount=amount,
                payment_method=payment_method,
                category=FINANCIAL_REVIEW_CATEGORY,
                notes="financials channel note needs review",
            )
        return _financial_ignore_parse("ignored financials channel informational note without ledger amount")

    amount = _financial_channel_amount_for_category(text, category)
    if amount is None:
        return _financial_review_parse(
            amount=None,
            payment_method=payment_method,
            category=category,
            notes=f"{notes}; amount needs review",
        )

    return _base_channel_financial_parse(
        parsed_type="expense",
        amount=amount,
        payment_method=payment_method,
        category=category,
        notes=notes,
        confidence=0.91 if needs_review else 0.94,
        needs_review=needs_review,
    )


def parse_channel_financial_message(
    message_text: str,
    *,
    channel_name: str | None,
    image_urls: list[str] | None = None,
) -> Dict[str, Any] | None:
    channel = _canonical_channel_leaf_name(channel_name)
    image_urls = image_urls or []
    if channel in LOAN_CHANNEL_NAMES:
        return _parse_loans_channel_message(message_text, image_urls)
    if channel in FINANCIALS_CHANNEL_NAMES:
        return _parse_financials_channel_message(message_text, image_urls)
    return None


def _financial_parse_needs_image_amount(parse: Dict[str, Any] | None, image_urls: list[str]) -> bool:
    if not parse or not image_urls:
        return False
    if parse.get("ignore_message"):
        return False
    return parse.get("parsed_amount") is None


def _merge_financial_image_parse(rule_parse: Dict[str, Any], image_parse: Dict[str, Any] | None) -> Dict[str, Any]:
    if not image_parse or image_parse.get("parsed_amount") is None:
        return rule_parse

    merged = dict(rule_parse)
    rule_type = (merged.get("parsed_type") or "").strip().lower()
    image_type = (image_parse.get("parsed_type") or "").strip().lower()
    if rule_type in {"", "unknown"} and image_type:
        merged["parsed_type"] = image_parse.get("parsed_type")

    rule_method = (merged.get("parsed_payment_method") or "").strip().lower()
    image_method = (image_parse.get("parsed_payment_method") or "").strip().lower()
    if rule_method in {"", "unknown"} and image_method:
        merged["parsed_payment_method"] = image_parse.get("parsed_payment_method")

    rule_category = (merged.get("parsed_category") or "").strip().lower()
    image_category = (image_parse.get("parsed_category") or "").strip()
    if rule_category in {"", "uncategorized"} and image_category:
        merged["parsed_category"] = image_category

    merged["parsed_amount"] = image_parse.get("parsed_amount")
    merged["image_summary"] = image_parse.get("image_summary") or merged.get("image_summary")
    merged["confidence"] = min(float(image_parse.get("confidence") or 0.78), 0.90)
    merged["needs_review"] = True

    existing_notes = (merged.get("parsed_notes") or "").strip()
    image_notes = (image_parse.get("parsed_notes") or "").strip()
    review_note = "image amount extracted by AI; review before approving"
    note_parts = [part for part in (existing_notes, image_notes, review_note) if part]
    merged["parsed_notes"] = " | ".join(dict.fromkeys(note_parts))

    for key in ("_openai_usage", "_openai_model", "_ai_provider"):
        if key in image_parse:
            merged[key] = image_parse[key]
    return merged


def parse_stitched_rule_hint(message_text: str) -> Dict[str, Any] | None:
    message_parts = split_stitched_messages(message_text)
    if len(message_parts) <= 1:
        return None

    normalized_parts = [normalize_message_part(part) for part in message_parts]
    nonempty_parts = [part for part in normalized_parts if part]
    if not nonempty_parts:
        return None

    explicit_type: str | None = None
    explicit_part: str | None = None
    trade_part: str | None = None
    payment_breakdown: list[tuple[float, str]] = []
    payment_method_only: str | None = None
    bare_amount_followup: float | None = None
    saw_image_only_lead = bool(message_parts and has_no_text_placeholder(message_parts[0]))

    for original_part, normalized_part in zip(message_parts, normalized_parts):
        if not normalized_part:
            continue

        trade_hint = parse_trade_hint(normalized_part)
        if trade_hint and trade_part is None:
            trade_part = normalized_part

        inferred_type = infer_explicit_buy_sell_type(normalized_part)
        if inferred_type:
            explicit_type = inferred_type
            explicit_part = normalized_part

        payment_summary = extract_payment_summary(normalized_part)
        if payment_summary:
            payment_breakdown.extend(payment_summary["payment_breakdown"])
        elif is_payment_method_only_message_text(normalized_part) and payment_method_only is None:
            payment_method_only = normalize_payment_method(normalized_part.lower())
        else:
            bare_amount = extract_bare_amount_message(normalized_part)
            if bare_amount is not None:
                bare_amount_followup = bare_amount

    combined_payment = summarize_payment_breakdown(payment_breakdown)
    payment_amount = combined_payment["amount"] if combined_payment else None
    payment_method = combined_payment["payment_method"] if combined_payment else None
    if payment_amount is None:
        payment_amount = bare_amount_followup
    if payment_amount is None and explicit_part:
        payment_amount = extract_unlabeled_amount(explicit_part)

    if explicit_type and explicit_part and not any(has_explicit_trade_signal(part) for part in nonempty_parts):
        notes = "stitched explicit buy/sell override"
        if saw_image_only_lead:
            notes = "image-first stitched explicit buy/sell override"
        return {
            "parsed_type": explicit_type,
            "parsed_amount": payment_amount,
            "parsed_payment_method": payment_method or payment_method_only or "unknown",
            "parsed_cash_direction": None,
            "parsed_category": "unknown",
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": notes,
            "image_summary": "no image used",
            "confidence": 0.94 if saw_image_only_lead else 0.92,
            "needs_review": False,
        }

    if trade_part:
        trade_hint = parse_trade_hint(trade_part)
        if trade_hint:
            if payment_amount is not None and trade_hint.get("parsed_amount") is None:
                trade_hint["parsed_amount"] = payment_amount
                trade_hint["parsed_payment_method"] = payment_method or trade_hint.get("parsed_payment_method") or "unknown"
                trade_hint["parsed_cash_direction"] = "to_store"
                trade_hint["parsed_trade_summary"] = (
                    (trade_hint.get("parsed_trade_summary") or "trade detected from in/out wording")
                    + f" | plus ${payment_amount:g} {payment_method or 'unknown'}"
                )
            if saw_image_only_lead:
                trade_hint["parsed_notes"] = "image-first stitched trade parse"
                trade_hint["confidence"] = max(float(trade_hint.get("confidence", 0.0)), 0.97)
            else:
                trade_hint["parsed_notes"] = "stitched trade parse"
            return trade_hint

    descriptive_parts = [
        part for part in nonempty_parts
        if not is_payment_only_message_text(part)
    ]
    if payment_amount is not None and descriptive_parts:
        for part in descriptive_parts:
            inferred_type = infer_explicit_buy_sell_type(part)
            if inferred_type and not has_explicit_trade_signal(part):
                notes = "stitched payment followup"
                if saw_image_only_lead:
                    notes = "image-first stitched payment followup"
                return {
                    "parsed_type": inferred_type,
                    "parsed_amount": payment_amount,
                    "parsed_payment_method": payment_method or payment_method_only or "unknown",
                    "parsed_cash_direction": None,
                    "parsed_category": "unknown",
                    "parsed_items": [],
                    "parsed_items_in": [],
                    "parsed_items_out": [],
                    "parsed_trade_summary": "",
                    "parsed_notes": notes,
                    "image_summary": "no image used",
                    "confidence": 0.93 if saw_image_only_lead else 0.9,
                    "needs_review": False,
                }

    if payment_method_only and descriptive_parts:
        for part in descriptive_parts:
            if has_explicit_trade_signal(part):
                continue
            inferred_amount = extract_unlabeled_amount(part)
            if inferred_amount is None:
                continue
            inferred_category = infer_category_from_text(part) or "unknown"
            return {
                "parsed_type": infer_explicit_buy_sell_type(part) or "unknown",
                "parsed_amount": inferred_amount,
                "parsed_payment_method": payment_method_only,
                "parsed_cash_direction": "unknown",
                "parsed_category": inferred_category,
                "parsed_items": [],
                "parsed_items_in": [],
                "parsed_items_out": [],
                "parsed_trade_summary": "",
                "parsed_notes": "stitched payment method followup",
                "image_summary": "no image used",
                "confidence": 0.87,
                "needs_review": True,
            }

    return None


def is_payment_only_message_text(text: str) -> bool:
    amount, method = extract_payment_amount_method(text)
    return amount is not None and method is not None


def channel_defaults_to_buy(channel_name: str | None) -> bool:
    lower = (channel_name or "").strip().lower()
    if not lower:
        return False
    return "store-buys" in lower or lower.endswith("purchases")


def has_reimbursement_buy_signal(message_text: str) -> bool:
    lower = normalize_detector_text(message_text).lower()
    if not lower:
        return False
    reimbursement_patterns = [
        r"\bowe (?:me|us)\b",
        r"\bpay (?:me|us) back\b",
        r"\breimburse(?: me| us)?\b",
        r"\bfront(?:ed|ing)?(?: me| us)?\b",
        r"\bspot(?: me| us)?\b",
    ]
    return any(re.search(pattern, lower, re.I) for pattern in reimbursement_patterns)


def parse_by_rules(message_text: str, channel_name: str | None = None) -> Dict[str, Any] | None:
    raw_text = (message_text or "").strip()

    stitched_hint = parse_stitched_rule_hint(raw_text)
    if stitched_hint:
        return stitched_hint

    text = normalize_message_part(raw_text).strip()
    if not text:
        return None

    trade_hint = parse_trade_hint(text)
    if trade_hint:
        return trade_hint
    lower = _normalize_payment_tokens(_normalize_amount_text(text.lower()))

    explicit_type = infer_explicit_buy_sell_type(text)
    payment_summary = extract_payment_summary(text)
    remainder_payment_summary = infer_remainder_payment_summary(text, payment_summary)
    if remainder_payment_summary:
        payment_summary = remainder_payment_summary
    multi_payment = remainder_payment_summary or extract_multi_payment_summary(text)
    inferred_category = infer_category_from_text(text) or "unknown"
    if (
        payment_summary
        and re.search(r"\bout\b", lower, re.I)
        and re.search(r"\b(cash|zelle|venmo|paypal|card|apple_pay)\s+in\b", lower, re.I)
        and not re.search(r"\btrade(?:d)?\b|\bplus\b|(^|\s)\+", lower, re.I)
    ):
        return {
            "parsed_type": "sell",
            "parsed_amount": payment_summary["amount"],
            "parsed_payment_method": payment_summary["payment_method"],
            "parsed_cash_direction": None,
            "parsed_category": inferred_category,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "rule-based item-out payment-in sale flow",
            "image_summary": "no image used",
            "confidence": 0.9,
            "needs_review": False,
        }

    if explicit_type and payment_summary and not has_explicit_trade_signal(text):
        if payment_summary["payment_method"] == "mixed":
            breakdown = " + ".join(f"${amount:g} {method}" for amount, method in payment_summary["payment_breakdown"])
            notes = f"rule-based multi-payment {explicit_type}: {breakdown}"
        else:
            notes = f"rule-based explicit {explicit_type} with payment amount"
        return {
            "parsed_type": explicit_type,
            "parsed_amount": payment_summary["amount"],
            "parsed_payment_method": payment_summary["payment_method"],
            "parsed_cash_direction": None,
            "parsed_category": inferred_category,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": notes,
            "image_summary": "no image used",
            "confidence": 0.94,
            "needs_review": False,
        }
    if explicit_type and multi_payment and not has_explicit_trade_signal(text):
        breakdown = " + ".join(f"${amount:g} {method}" for amount, method in multi_payment["payment_breakdown"])
        return {
            "parsed_type": explicit_type,
            "parsed_amount": multi_payment["amount"],
            "parsed_payment_method": multi_payment["payment_method"],
            "parsed_cash_direction": None,
            "parsed_category": inferred_category,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": f"rule-based multi-payment {explicit_type}: {breakdown}",
            "image_summary": "no image used",
            "confidence": 0.94,
            "needs_review": False,
        }

    if explicit_type and not has_explicit_trade_signal(text):
        unlabeled_amount = extract_unlabeled_amount(text)
        if unlabeled_amount is not None:
            return {
                "parsed_type": explicit_type,
                "parsed_amount": unlabeled_amount,
                "parsed_payment_method": "unknown",
                "parsed_cash_direction": None,
                "parsed_category": inferred_category,
                "parsed_items": [],
                "parsed_items_in": [],
                "parsed_items_out": [],
                "parsed_trade_summary": "",
                "parsed_notes": f"rule-based explicit {explicit_type} with inferred amount",
                "image_summary": "no image used",
                "confidence": 0.92,
                "needs_review": False,
            }

    payment_only_amount, payment_only_method = extract_payment_amount_method(text)
    if payment_only_amount is not None and payment_only_method is not None:
        default_type = "buy" if channel_defaults_to_buy(channel_name) else "sell"
        return {
            "parsed_type": default_type,
            "parsed_amount": payment_only_amount,
            "parsed_payment_method": payment_only_method,
            "parsed_cash_direction": None,
            "parsed_category": "unknown",
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": f"rule-based payment-only {default_type} default",
            "image_summary": "no image used",
            "confidence": 0.85,
            "needs_review": False,
        }

    if (
        payment_summary
        and not explicit_type
        and not has_explicit_trade_signal(text)
        and not looks_like_internal_cash_transfer(text)
    ):
        default_type = "buy" if channel_defaults_to_buy(channel_name) else "sell"
        if payment_summary["payment_method"] == "mixed":
            breakdown = " + ".join(
                f"${amount:g} {method}" for amount, method in payment_summary["payment_breakdown"]
            )
            notes = f"rule-based payment shorthand {default_type} default: {breakdown}"
        else:
            notes = f"rule-based payment shorthand {default_type} default"
        return {
            "parsed_type": default_type,
            "parsed_amount": payment_summary["amount"],
            "parsed_payment_method": payment_summary["payment_method"],
            "parsed_cash_direction": None,
            "parsed_category": inferred_category,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": notes,
            "image_summary": "no image used",
            "confidence": 0.88,
            "needs_review": False,
        }

    amount_first_match = re.fullmatch(
        r"\$?\s*(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)",
        lower,
        re.I,
    )

    payment_first_match = re.fullmatch(
        r"(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)\s*\$?\s*(\d+(?:\.\d{1,2})?)",
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

        payment = normalize_payment_method(payment)

        default_type = "buy" if channel_defaults_to_buy(channel_name) else "sell"
        return {
            "parsed_type": default_type,
            "parsed_amount": amount,
            "parsed_payment_method": payment,
            "parsed_cash_direction": None,
            "parsed_category": "unknown",
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": f"rule-based payment-only {default_type} default",
            "image_summary": "no image used",
            "confidence": 0.85,
            "needs_review": False,
        }
    patterns = [
        re.compile(r"\b(sold|sell)\b\s*\$?(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)?", re.I),
        re.compile(r"\b(bought|buy|paid)\b\s*\$?(\d+(?:\.\d{1,2})?)\s*(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)?", re.I),
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
            except Exception as e:
                logger.warning(
                    "parser.parse_by_rules: float(verb-pattern group 2) failed, amount=None: %s",
                    e,
                    exc_info=True,
                )
                amount = None
            if amount is not None and (
                has_quantity_unit_after(text, m.end(2))
                or has_quantity_multiplier_after(text, m.end(2))
            ):
                amount = None

        if m.lastindex and m.lastindex >= 3:
            payment = (m.group(3) or "").lower() or None
            if payment:
                payment = normalize_payment_method(payment)

        if "sold" in verb or "sell" in verb:
            parsed_type = "sell"
        elif "buy" in verb or "bought" in verb or "paid" in verb:
            parsed_type = "buy"
        elif "trade" in text.lower():
            parsed_type = "trade"
            cash_direction = "none"
        else:
            parsed_type = "unknown"
            cash_direction = "unknown"
        if parsed_type in {"buy", "sell"}:
            cash_direction = None

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


def has_transaction_signal(message_text: str) -> bool:
    lower = _normalize_payment_tokens(normalize_detector_text(message_text).lower())
    if not lower:
        return False

    transaction_patterns = [
        r"\b(sold|sell|sale|bought|buy|bougjt|baught|paid|trade|traded)\b",
        r"\b(zelle|venmo|paypal|cash|tap|card|cc|dc|apple_pay)\b\s*\$?\d",
        r"\$?\d+(?:\.\d{1,2})?\s*\b(zelle|venmo|paypal|cash|tap|card|cc|dc|apple_pay)\b",
        r"\b(top|bottom|left|right)\b.*\b(in|out)\b",
        r"\b(in)\b.*\b(out)\b",
        r"\b(out)\b.*\b(in)\b",
        r"\b(psa|bgs|slab|slabs|single|singles|packs|sealed|binder|collection)\b.*\b\d",
        r"\b(owe me|pay me back|reimburse(?: me)?|front(?:ed|ing)?(?: me)?|spot me)\b",
    ]
    return any(re.search(pattern, lower, re.I) for pattern in transaction_patterns)


def looks_like_date_marker(message_text: str) -> bool:
    lower = normalize_detector_text(message_text).lower().rstrip(":")
    if not lower:
        return False

    patterns = [
        r"^(jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\s+\d{1,2}$",
        r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$",
        r"^\d{1,2}/\d{1,2}(?:/\d{2,4})?$",
    ]
    return any(re.fullmatch(pattern, lower, re.I) for pattern in patterns)


def looks_like_internal_cash_transfer(message_text: str) -> bool:
    """True when the message describes an internal money flow (not a transaction).

    Internal transfers are things like "Give company 6k cash (owe me)" --
    an employee or owner lending money to the business. The company's
    balance sheet changes (cash in, loan payable) but this is NOT a
    sell/buy/trade and must not appear in revenue/expense reporting.

    Important: an explicit "give/gave/loan <X> company cash" pattern
    WINS over any "owe me" reimbursement signal. "Owe me" in this
    context means "the company owes me that loan back later," not
    "the store still owes me reimbursement for inventory I bought".
    """
    lower = normalize_detector_text(message_text).lower()
    if not lower:
        return False

    company_terms = r"(company|shop|store|business)"
    loan_terms = r"(loan(?:ed|ing)?|floating?)"
    transfer_terms = r"(give|gave|hand(?:ed)?|brought|put|loan(?:ed|ing)?)"

    patterns = [
        rf"\b{transfer_terms}\b.*\b{company_terms}\b.*\b\d[\dk,\.]*\s*(cash|zelle|venmo|paypal|cc|dc|card)?\b.*\b{loan_terms}\b",
        rf"\b{company_terms}\b.*\b\d[\dk,\.]*\s*(cash|zelle|venmo|paypal|cc|dc|card)?\b.*\b{loan_terms}\b",
        rf"\b{transfer_terms}\b.*\b{company_terms}\b.*\b\d[\dk,\.]*\s*(cash|zelle|venmo|paypal|cc|dc|card)?\b",
        # Supports the "Put 3k cash into the company" ordering where
        # the amount precedes the company term.
        rf"\b{transfer_terms}\b.*\b\d[\dk,\.]*\s*(cash|zelle|venmo|paypal|cc|dc|card)\b.*\b(?:in|into|to)\b\s+(?:the\s+)?{company_terms}\b",
        rf"\b{loan_terms}\b.*\b{company_terms}\b",
    ]
    if any(re.search(pattern, lower, re.I) for pattern in patterns):
        return True

    # If none of the explicit transfer patterns fired, fall back to
    # treating "owe me" as a reimbursement signal (store bought
    # inventory and owes the logger back). That is NOT an internal
    # transfer, so we return False here too -- but only after confirming
    # no transfer pattern matched.
    return False


def _detect_conversational_noise(lower: str, image_urls: List[str] | None = None) -> str | None:
    """Return an ignore reason if the message is conversational noise, else None.

    Only called AFTER has_transaction_signal returned False, so we know
    there are no buy/sell/trade/payment signals in the text.
    """
    if not lower:
        return None

    if not re.search(r"[a-zA-Z0-9$]", lower):
        return "ignored emoji-only message"

    if re.search(r"\bwrong\s+(chat|channel|image)\b", lower, re.I):
        return "ignored wrong-channel message"

    if re.fullmatch(r"(?:texted|called|dm(?:ed)?|messaged)\s+\d{1,2}/\d{1,2}/\d{2,4}", lower, re.I):
        return "ignored contact follow-up note"

    is_payment_word = bool(re.fullmatch(
        r"(zelle|venmo|paypal|cash|card|tap|cc|dc|apple_pay)", lower.strip(), re.I,
    ))
    if is_payment_word:
        return None

    if len(lower) < 25 and not re.search(r"\d", lower) and not image_urls:
        return "ignored short conversational message"

    return None


def detect_non_transaction_message(message_text: str, image_urls: List[str] | None = None) -> Dict[str, Any] | None:
    normalized = normalize_detector_text(message_text)
    lower = _normalize_payment_tokens(normalized.lower())
    image_urls = image_urls or []

    if not lower:
        return {
            "parsed_type": None,
            "parsed_amount": None,
            "parsed_payment_method": None,
            "parsed_cash_direction": None,
            "parsed_category": None,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "ignored blank or non-transaction message",
            "image_summary": "no image used",
            "confidence": 0.99,
            "needs_review": False,
            "ignore_message": True,
        }

    non_transaction_keywords = [
        "profit overview",
        "daily profit",
        "profit recap",
        "summary",
        "recap",
        "spreadsheet",
        "google sheet",
        "docs.google.com",
        "screenshot",
        "overview",
        "report",
        "totals",
    ]

    if looks_like_date_marker(lower):
        return {
            "parsed_type": None,
            "parsed_amount": None,
            "parsed_payment_method": None,
            "parsed_cash_direction": None,
            "parsed_category": None,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "ignored date marker or non-transaction heading",
            "image_summary": "no image used" if not image_urls else "non-transaction image ignored",
            "confidence": 0.99,
            "needs_review": False,
            "ignore_message": True,
        }

    if looks_like_internal_cash_transfer(lower):
        return {
            "parsed_type": None,
            "parsed_amount": None,
            "parsed_payment_method": None,
            "parsed_cash_direction": None,
            "parsed_category": None,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "ignored internal cash transfer or partner loan",
            "image_summary": "no image used" if not image_urls else "internal transfer image ignored",
            "confidence": 0.99,
            "needs_review": False,
            "ignore_message": True,
        }

    if has_transaction_signal(lower):
        return None

    if any(keyword in lower for keyword in non_transaction_keywords):
        return {
            "parsed_type": None,
            "parsed_amount": None,
            "parsed_payment_method": None,
            "parsed_cash_direction": None,
            "parsed_category": None,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "ignored non-transaction summary or screenshot",
            "image_summary": "non-transaction image ignored" if image_urls else "no image used",
            "confidence": 0.98,
            "needs_review": False,
            "ignore_message": True,
        }

    noise_reason = _detect_conversational_noise(lower, image_urls)
    if noise_reason:
        return {
            "parsed_type": None,
            "parsed_amount": None,
            "parsed_payment_method": None,
            "parsed_cash_direction": None,
            "parsed_category": None,
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": noise_reason,
            "image_summary": "no image used",
            "confidence": 0.99,
            "needs_review": False,
            "ignore_message": True,
        }

    return None


def has_explicit_trade_signal(message_text: str) -> bool:
    lower = (message_text or "").lower()
    if not lower:
        return False

    trade_patterns = [
        r"\btrade\b",
        r"\btop\b.*\bout\b",
        r"\bbottom\b.*\bin\b",
        r"\bleft\b.*\b(out|in)\b",
        r"\bright\b.*\b(out|in)\b",
        r"\b(out)\b.*\b(in)\b",
        r"\b(in)\b.*\b(out)\b",
        r"^\s*(?:plus|\+)\s*\$?\d+",
    ]
    return any(re.search(pattern, lower, re.I) for pattern in trade_patterns)


def has_explicit_buy_signal(message_text: str) -> bool:
    lower = (message_text or "").lower()
    if not lower:
        return False
    return bool(
        re.search(r"\b(bought|buy|bougjt|baught|paid|sold us|sold to us|customer sold|bought from)\b", lower, re.I)
    ) or has_reimbursement_buy_signal(lower)


def infer_explicit_buy_sell_type(message_text: str) -> str | None:
    lower = (message_text or "").lower()
    if not lower:
        return None

    buy_patterns = [
        r"\b(bought|buy|bougjt|baught|paid|bought from|picked up|picked up from|sold us|sold to us|customer sold)\b",
    ]
    sell_patterns = [
        r"\b(sold|sell|sale|sold to)\b",
    ]

    if has_reimbursement_buy_signal(lower):
        return "buy"
    if any(re.search(pattern, lower, re.I) for pattern in buy_patterns):
        return "buy"
    if any(re.search(pattern, lower, re.I) for pattern in sell_patterns):
        return "sell"
    return None


def has_explicit_sell_signal(message_text: str) -> bool:
    lower = (message_text or "").lower()
    if not lower:
        return False
    return bool(re.search(r"\b(sold|sell|sold to)\b", lower, re.I))


def enforce_store_conventions(
    message_text: str,
    parsed: Dict[str, Any],
    rule_hint: Dict[str, Any] | None,
    channel_name: str | None = None,
) -> Dict[str, Any]:
    if not rule_hint:
        return parsed

    explicit_buy_sell = infer_explicit_buy_sell_type(message_text)
    message_parts = split_stitched_messages(message_text)
    explicit_parts = [
        part for part in message_parts
        if infer_explicit_buy_sell_type(part) is not None
    ]
    has_trade_in_any_part = any(has_explicit_trade_signal(part) for part in message_parts)
    if parsed.get("parsed_type") == "trade" and explicit_buy_sell and not has_explicit_trade_signal(message_text):
        return {
            **parsed,
            "parsed_type": explicit_buy_sell,
            "parsed_cash_direction": None,
            "parsed_notes": "explicit buy/sell wording overrode trade guess",
            "confidence": max(float(parsed.get("confidence", 0.0)), 0.9),
        }

    # Hard store rule:
    # payment-only shorthand defaults to a sale unless there is clear contrary context.
    if (
        rule_hint.get("parsed_type") in {"buy", "sell"}
        and str(rule_hint.get("parsed_notes") or "").startswith("rule-based payment-only")
        and not has_explicit_trade_signal(message_text)
        and not explicit_buy_sell
    ):
        default_type = "buy" if channel_defaults_to_buy(channel_name) else "sell"
        return {
            **parsed,
            "parsed_type": default_type,
            "parsed_amount": rule_hint.get("parsed_amount"),
            "parsed_payment_method": rule_hint.get("parsed_payment_method"),
            "parsed_cash_direction": None,
            "parsed_category": parsed.get("parsed_category") or "unknown",
            "parsed_notes": f"payment-only {default_type} default (store rule)",
            "needs_review": bool(parsed.get("needs_review", False)),
            "confidence": max(float(parsed.get("confidence", 0.0)), 0.9),
        }

    # Hard store rule:
    # explicit buy/sell text in the stitched message should outrank image-based AI guesses,
    # unless the text also clearly signals a trade.
    if (
        rule_hint.get("parsed_type") in {"buy", "sell"}
        and not has_trade_in_any_part
    ):
        rule_type = rule_hint.get("parsed_type")
        explicit_signal_matches = (
            (rule_type == "buy" and any(has_explicit_buy_signal(part) for part in explicit_parts or [message_text]))
            or (rule_type == "sell" and any(has_explicit_sell_signal(part) for part in explicit_parts or [message_text]))
        )
        if explicit_signal_matches:
            return {
                **parsed,
                "parsed_type": rule_type,
                "parsed_amount": first_nonempty_value(rule_hint.get("parsed_amount"), parsed.get("parsed_amount")),
                "parsed_payment_method": first_nonempty_value(rule_hint.get("parsed_payment_method"), parsed.get("parsed_payment_method")) or "unknown",
                "parsed_cash_direction": None,
                "parsed_category": first_nonempty_value(parsed.get("parsed_category"), rule_hint.get("parsed_category"), infer_category_from_text(message_text)) or "unknown",
                "parsed_notes": f"explicit {rule_type} text override (store rule)",
                "confidence": max(float(parsed.get("confidence", 0.0)), 0.9),
                "needs_review": False,
            }

    multi_payment = extract_multi_payment_summary(message_text)
    if multi_payment and explicit_buy_sell and not has_trade_in_any_part:
        return {
            **parsed,
            "parsed_type": explicit_buy_sell,
            "parsed_amount": multi_payment["amount"],
            "parsed_payment_method": "mixed",
            "parsed_cash_direction": None,
            "parsed_category": first_nonempty_value(parsed.get("parsed_category"), infer_category_from_text(message_text)) or "unknown",
            "parsed_notes": "explicit buy/sell multi-payment override",
            "confidence": max(float(parsed.get("confidence", 0.0)), 0.93),
            "needs_review": False,
        }

    if explicit_buy_sell and parsed.get("parsed_type") in {None, "unknown"} and not has_trade_in_any_part:
        return {
            **parsed,
            "parsed_type": explicit_buy_sell,
            "parsed_cash_direction": None,
            "parsed_notes": "explicit buy/sell wording",
            "confidence": max(float(parsed.get("confidence", 0.0)), 0.9),
            "needs_review": False,
        }

    return parsed


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
                "enum": ["cash", "zelle", "venmo", "paypal", "card", "apple_pay", "mixed", "trade", "unknown", None]
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


def build_prompt(
    author_name: str,
    message_text: str,
    rule_hint: Dict[str, Any] | None,
    has_images: bool,
    channel_name: str = "",
    correction_hints: List[Dict[str, Any]] | None = None,
) -> str:
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

    correction_block = ""
    if correction_hints:
        correction_lines = []
        for index, hint in enumerate(correction_hints, start=1):
            correction_lines.append(
                f"- correction {index}: text='{hint.get('normalized_text')}', "
                f"type={hint.get('deal_type')}, amount={hint.get('amount')}, "
                f"payment={hint.get('payment_method')}, cash_direction={hint.get('cash_direction')}, "
                f"category={hint.get('category')}, entry_kind={hint.get('entry_kind')}, "
                f"notes={hint.get('notes')}"
            )
        correction_block = (
            "Relevant past manual corrections from this store. "
            "Use them as strong guidance when the new message looks materially similar:\n"
            + "\n".join(correction_lines)
        )

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
- "cc" means credit card and should map to card payment
- "dc" means debit card and should map to card payment

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
- Return only valid JSON matching the requested fields. Do not include markdown or explanatory text outside JSON.
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
  cash / zelle / venmo / paypal / card / mixed / trade / unknown
- If the deal clearly uses multiple payment methods, sum them into parsed_amount and use payment method "mixed".
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
  - If the text contains explicit buy/sell wording, prefer "buy" or "sell" over a trade guess from images alone.
  - If the text contains both "in" and "out" describing store item flow, classify it as "trade", not "buy" or "sell".
- If a message is only an amount plus payment method, default it to a sell unless other context indicates otherwise.
- If the channel is a store-buys or *purchases style channel, default payment-only messages to a buy instead.
- Phrases like "owe me", "pay me back", or "reimburse me" mean the store still bought the inventory and owes reimbursement.
{image_block}

Author: {author_name}
Channel: {channel_name or "unknown"}
Stitched transaction text:
{message_text}

{hint_block}

{correction_block}
""".strip()


def parse_deal_with_ai(
    author_name: str,
    message_text: str,
    image_urls: List[str] | None = None,
    channel_name: str = "",
) -> Dict[str, Any]:
    image_urls = image_urls or []
    correction_hints = get_relevant_correction_hints(message_text)
    rule_hint = parse_by_rules(message_text, channel_name=channel_name)
    schema = build_schema()
    prompt = build_prompt(
        author_name=author_name,
        message_text=message_text,
        rule_hint=rule_hint,
        has_images=bool(image_urls),
        channel_name=channel_name,
        correction_hints=correction_hints,
    )

    content: list[dict] = [{"type": "text", "text": prompt}]
    for url in image_urls:
        content.append({
            "type": "image_url",
            "image_url": {"url": url, "detail": "auto"},
        })

    client = get_ai_client().with_options(timeout=60.0)

    if is_nvidia():
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": content}],
            response_format={"type": "json_object"},
            max_tokens=2048,
        )
        raw_text = response.choices[0].message.content or "{}"
    else:
        response = client.responses.create(
            model=MODEL,
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt}
                ] + [
                    {"type": "input_image", "image_url": url, "detail": "auto"}
                    for url in image_urls
                ],
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
        raw_text = response.output_text

    parsed = json.loads(raw_text)
    usage_metrics = extract_usage_metrics(response, model=MODEL)
    return enforce_store_conventions(
        message_text=message_text,
        parsed=parsed,
        rule_hint=rule_hint,
        channel_name=channel_name,
    ) | {"_openai_usage": usage_metrics, "_openai_model": MODEL, "_ai_provider": get_provider()}


def _normalize_financial_image_parse(parsed: Dict[str, Any]) -> Dict[str, Any]:
    parsed_type = str(parsed.get("parsed_type") or "unknown").strip().lower()
    if parsed_type == "sale":
        parsed_type = "sell"
    if parsed_type not in {"sell", "buy", "expense", "loan_draw", "loan_repayment", "transfer", "unknown"}:
        parsed_type = "unknown"

    amount = parsed.get("parsed_amount")
    try:
        normalized_amount = round(abs(float(amount)), 2) if amount not in (None, "") else None
    except (TypeError, ValueError):
        normalized_amount = None

    payment_method = str(parsed.get("parsed_payment_method") or "unknown").strip().lower()
    payment_method = normalize_payment_method(payment_method) if payment_method else "unknown"
    category = str(parsed.get("parsed_category") or "").strip() or None

    try:
        confidence = float(parsed.get("confidence") or 0.78)
    except (TypeError, ValueError):
        confidence = 0.78

    return {
        "parsed_type": parsed_type,
        "parsed_amount": normalized_amount,
        "parsed_payment_method": payment_method,
        "parsed_cash_direction": None,
        "parsed_category": category,
        "parsed_items": [],
        "parsed_items_in": [],
        "parsed_items_out": [],
        "parsed_trade_summary": "",
        "parsed_notes": str(parsed.get("parsed_notes") or "").strip(),
        "image_summary": str(parsed.get("image_summary") or "financial image reviewed").strip(),
        "confidence": max(0.0, min(confidence, 0.90)),
        "needs_review": True,
    }


def parse_financial_image_with_ai(
    author_name: str,
    message_text: str,
    image_urls: List[str] | None = None,
    channel_name: str = "",
) -> Dict[str, Any]:
    image_urls = image_urls or []
    channel_label = _canonical_channel_leaf_name(channel_name) or "financials"
    prompt = f"""
You are parsing a Discord financial ledger message for Degen Collectibles.

Channel: {channel_label}
Author: {author_name or "unknown"}
Text: {message_text or "(no text)"}

Use the attached image(s) only to extract the visible financial transaction details.
Return JSON only with these fields:
- parsed_type: one of "expense", "sell", "buy", "loan_draw", "loan_repayment", "transfer", "unknown"
- parsed_amount: the main visible dollar amount as a positive number, or null if no amount is visible
- parsed_payment_method: "cash", "zelle", "venmo", "paypal", "card", "check", "mixed", or "unknown"
- parsed_category: one of "payroll", "rent_facilities", "show_fees", "taxes_licenses", "insurance", "loan_interest", "loan_owner_payments", "other_business_expense", "transfers", "uncategorized"
- parsed_notes: short reason for the parse
- image_summary: short description of the evidence visible in the image
- confidence: number from 0 to 1

If the visible amount is ambiguous, cropped, or not readable, set parsed_amount to null and confidence below 0.70.
Do not invent card inventory details. This is not the normal deal parser.
""".strip()

    content: list[dict] = [{"type": "text", "text": prompt}]
    for url in image_urls:
        content.append({
            "type": "image_url",
            "image_url": {"url": url, "detail": "auto"},
        })

    client = get_ai_client().with_options(timeout=60.0)
    if is_nvidia():
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": content}],
            response_format={"type": "json_object"},
            max_tokens=1024,
        )
        raw_text = response.choices[0].message.content or "{}"
    else:
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "parsed_type": {"type": "string"},
                "parsed_amount": {"type": ["number", "null"]},
                "parsed_payment_method": {"type": "string"},
                "parsed_category": {"type": "string"},
                "parsed_notes": {"type": "string"},
                "image_summary": {"type": "string"},
                "confidence": {"type": "number"},
            },
            "required": [
                "parsed_type",
                "parsed_amount",
                "parsed_payment_method",
                "parsed_category",
                "parsed_notes",
                "image_summary",
                "confidence",
            ],
        }
        response = client.responses.create(
            model=MODEL,
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt}
                ] + [
                    {"type": "input_image", "image_url": url, "detail": "auto"}
                    for url in image_urls
                ],
            }],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "financial_image_parse",
                    "schema": schema,
                    "strict": True,
                }
            },
        )
        raw_text = response.output_text

    normalized = _normalize_financial_image_parse(json.loads(raw_text))
    usage_metrics = extract_usage_metrics(response, model=MODEL)
    normalized.update({"_openai_usage": usage_metrics, "_openai_model": MODEL, "_ai_provider": get_provider()})
    return normalized


async def parse_financial_image_with_ai_async(
    author_name: str,
    message_text: str,
    image_urls: List[str] | None = None,
    channel_name: str = "",
) -> Dict[str, Any]:
    async with api_semaphore:
        try:
            return await asyncio.to_thread(
                parse_financial_image_with_ai,
                author_name,
                message_text,
                image_urls,
                channel_name,
            )
        except Exception as e:
            error_text = str(e).lower()
            if "timed out" in error_text or "timeout" in error_text:
                raise TimedOutRowError(str(e))
            raise


async def parse_deal_with_ai_async(
    author_name: str,
    message_text: str,
    image_urls: List[str] | None = None,
    channel_name: str = "",
) -> Dict[str, Any]:
    async with api_semaphore:
        try:
            return await asyncio.to_thread(
                parse_deal_with_ai,
                author_name,
                message_text,
                image_urls,
                channel_name,
            )
        except Exception as e:
            error_text = str(e).lower()
            if "timed out" in error_text or "timeout" in error_text:
                raise TimedOutRowError(str(e))
            raise


def _compact_parse_snapshot(parse: Dict[str, Any]) -> Dict[str, Any]:
    """Return a small, stable dict suitable for storing as JSON on the row."""
    return {
        "parsed_type": parse.get("parsed_type"),
        "parsed_amount": parse.get("parsed_amount"),
        "parsed_payment_method": parse.get("parsed_payment_method"),
        "parsed_cash_direction": parse.get("parsed_cash_direction"),
        "parsed_category": parse.get("parsed_category"),
        "confidence": parse.get("confidence"),
        "parsed_notes": parse.get("parsed_notes"),
    }


def _parses_disagree_on_amount(rule_amount: Any, ai_amount: Any) -> bool:
    if rule_amount is None or ai_amount is None:
        return False
    try:
        ra = float(rule_amount)
        aa = float(ai_amount)
    except (TypeError, ValueError):
        return False
    tolerance = max(1.0, 0.01 * max(abs(ra), abs(aa)))
    return abs(ra - aa) > tolerance


def reconcile_parses(
    rule_parsed: Dict[str, Any] | None,
    ai_parsed: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge the rule-based parse and the AI parse into a single result.

    - If rules didn't match, return the AI result unchanged.
    - If rules matched and agree with AI on key fields, return a merged result
      with elevated confidence and ``needs_review=False``.
    - If rules matched and disagree on at least one key field, return the AI
      result (AI has image context) but flag ``needs_review=True`` and
      attach a ``_parse_disagreement`` metadata blob so the worker can
      persist both parses for the reviewer.
    """
    if rule_parsed is None:
        return ai_parsed

    rule_type = (rule_parsed.get("parsed_type") or "").lower()
    ai_type = (ai_parsed.get("parsed_type") or "").lower()
    rule_pm = (rule_parsed.get("parsed_payment_method") or "").lower()
    ai_pm = (ai_parsed.get("parsed_payment_method") or "").lower()
    rule_cd = (rule_parsed.get("parsed_cash_direction") or "").lower()
    ai_cd = (ai_parsed.get("parsed_cash_direction") or "").lower()

    disagreement_fields: list[str] = []
    if rule_type and ai_type and rule_type != ai_type:
        disagreement_fields.append("parsed_type")
    if _parses_disagree_on_amount(rule_parsed.get("parsed_amount"), ai_parsed.get("parsed_amount")):
        disagreement_fields.append("parsed_amount")
    if (
        rule_pm and ai_pm
        and rule_pm != "unknown" and ai_pm != "unknown"
        and rule_pm != ai_pm
    ):
        disagreement_fields.append("parsed_payment_method")
    if (
        rule_type == "trade" and ai_type == "trade"
        and rule_cd and ai_cd
        and rule_cd != "unknown" and ai_cd != "unknown"
        and rule_cd != ai_cd
    ):
        disagreement_fields.append("parsed_cash_direction")

    if not disagreement_fields:
        # Agreement — AI has richer context (images, items), so use it as
        # the base and fold in rule fields where AI returned "unknown".
        merged = dict(ai_parsed)
        if (merged.get("parsed_payment_method") or "").lower() in {"", "unknown"} and rule_pm and rule_pm != "unknown":
            merged["parsed_payment_method"] = rule_parsed.get("parsed_payment_method")
        if (merged.get("parsed_cash_direction") or "").lower() in {"", "unknown"} and rule_cd and rule_cd != "unknown":
            merged["parsed_cash_direction"] = rule_parsed.get("parsed_cash_direction")

        base_conf = float(merged.get("confidence") or 0.85)
        merged["confidence"] = min(0.99, max(base_conf, 0.95))
        merged["needs_review"] = False

        existing_notes = (merged.get("parsed_notes") or "").strip()
        agreement_note = "rules+ai agreement"
        if agreement_note not in existing_notes.lower():
            merged["parsed_notes"] = (
                f"{existing_notes} | {agreement_note}" if existing_notes else agreement_note
            )
        merged["_parse_agreement"] = True
        return merged

    # Disagreement — prefer AI (stronger judgment with images), but flag the
    # row for human review and record both parses for context.
    merged = dict(ai_parsed)
    merged["needs_review"] = True

    base_conf = float(merged.get("confidence") or 0.70)
    merged["confidence"] = min(base_conf, 0.80)

    disagree_summary = f"rule/ai disagreement on: {', '.join(disagreement_fields)}"
    existing_notes = (merged.get("parsed_notes") or "").strip()
    merged["parsed_notes"] = (
        f"{disagree_summary} | {existing_notes}" if existing_notes else disagree_summary
    )
    merged["_parse_disagreement"] = {
        "rule": _compact_parse_snapshot(rule_parsed),
        "ai": _compact_parse_snapshot(ai_parsed),
        "fields": disagreement_fields,
    }
    return merged


async def parse_message(content: str, attachment_urls: list[str], author_name: str = "", channel_name: str = "") -> Dict[str, Any]:
    image_urls = choose_image_urls(
        attachment_urls,
        use_first_image_only=True,
        max_images=2,
    )

    channel_financial = parse_channel_financial_message(
        content or "",
        channel_name=channel_name,
        image_urls=image_urls,
    )
    if channel_financial is not None:
        if _financial_parse_needs_image_amount(channel_financial, image_urls):
            try:
                image_financial = await parse_financial_image_with_ai_async(
                    author_name=author_name,
                    message_text=content or "",
                    image_urls=image_urls,
                    channel_name=channel_name,
                )
                return _merge_financial_image_parse(channel_financial, image_financial)
            except TimedOutRowError as e:
                logger.warning(
                    "parser.parse_message: financial image parse timed out, using rule review fallback: %s",
                    e,
                )
            except Exception as e:
                logger.warning(
                    "parser.parse_message: financial image parse failed, using rule review fallback: %s",
                    e,
                    exc_info=True,
                )
        return channel_financial

    non_transaction = detect_non_transaction_message(content or "", image_urls=image_urls)
    if non_transaction:
        return non_transaction

    exact_correction = get_exact_correction_match(content or "")
    if exact_correction:
        return exact_correction

    learned_rule_match, learned_rule_event = get_learned_rule_match(content or "")
    if learned_rule_match:
        return learned_rule_match

    # Dual-path parsing: always try rules AND AI, then reconcile.
    # Rules run synchronously (fast). AI runs in the background while we
    # already have the rule answer in hand, so the extra wall-clock cost is
    # bounded to the AI call itself — no worse than the old AI-only path.
    rule_parsed = parse_by_rules(content or "", channel_name=channel_name)

    try:
        ai_parsed = await parse_deal_with_ai_async(
            author_name=author_name,
            message_text=content or "",
            image_urls=image_urls,
            channel_name=channel_name,
        )
    except TimedOutRowError:
        # If AI times out but rules captured a result, use rules rather
        # than failing the row outright.
        if rule_parsed is not None:
            if learned_rule_event:
                rule_parsed["_learned_rule_event"] = learned_rule_event
            return rule_parsed
        raise
    except Exception as e:
        logger.warning(
            "parser.parse_message: AI parse failed, trying rule fallback: %s",
            e,
            exc_info=True,
        )
        if rule_parsed is not None:
            if learned_rule_event:
                rule_parsed["_learned_rule_event"] = learned_rule_event
            return rule_parsed
        raise

    merged = reconcile_parses(rule_parsed, ai_parsed)
    if learned_rule_event:
        merged["_learned_rule_event"] = learned_rule_event
    return merged
