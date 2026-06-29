from __future__ import annotations

import json
import re
from dataclasses import dataclass
from email import policy
from email.parser import HeaderParser
from typing import Any, Iterable, Optional


SORTSWIFT_EXPECTED_MAILBOX = "no-reply@mail.sortswift.com"
SORTSWIFT_ORGANIZATIONAL_DOMAIN = "sortswift.com"

MAX_FROM_HEADER_BYTES = 1024
MAX_AUTHENTICATION_RESULTS_HEADERS = 8
MAX_AUTHENTICATION_RESULT_BYTES = 16 * 1024
MAX_AUTHENTICATION_RESULTS_AGGREGATE_BYTES = 64 * 1024
MAX_AUTHENTICATION_RESULT_SEGMENTS = 32
MAX_AUTHENTICATION_TOKENS_PER_SEGMENT = 32
MAX_AUTHENTICATION_COMMENT_DEPTH = 8

SOURCE_TRUST_REASON_CODES = frozenset(
    {
        "source_not_evaluated",
        "trusted_explicit",
        "trusted_dmarc_aligned",
        "trusted_dkim_aligned",
        "trusted_spf_aligned",
        "untrusted_explicit",
        "from_missing",
        "from_malformed",
        "from_unexpected",
        "auth_missing",
        "auth_malformed",
        "auth_no_google_receiver",
        "auth_mixed_receivers",
        "auth_ambiguous_google_receiver",
        "auth_ambiguous_results",
        "auth_no_aligned_pass",
    }
)
VERIFIED_SOURCE_TRUST_REASONS = frozenset(
    {
        "trusted_dmarc_aligned",
        "trusted_dkim_aligned",
        "trusted_spf_aligned",
    }
)
VERIFIED_SOURCE_UNTRUST_REASONS = frozenset(
    {
        "from_missing",
        "from_malformed",
        "from_unexpected",
        "auth_missing",
        "auth_malformed",
        "auth_no_google_receiver",
        "auth_mixed_receivers",
        "auth_ambiguous_google_receiver",
        "auth_ambiguous_results",
        "auth_no_aligned_pass",
    }
)

_AUTHENTICATION_METHOD_PROPERTIES = {
    "arc": frozenset(),
    "dmarc": frozenset({"header.from"}),
    "dkim": frozenset({"header.i", "header.d", "header.s", "header.b"}),
    "spf": frozenset({"smtp.mailfrom", "smtp.helo"}),
}
_AUTHENTICATION_METHOD_RESULTS = {
    "arc": frozenset({"none", "pass", "fail"}),
    "dmarc": frozenset({"none", "pass", "fail", "temperror", "permerror"}),
    "dkim": frozenset({"none", "pass", "fail", "policy", "neutral", "temperror", "permerror"}),
    "spf": frozenset({"none", "pass", "fail", "neutral", "softfail", "temperror", "permerror"}),
}
_AUTHENTICATION_IDENTITY_KINDS = {
    "header.from": "domain",
    "header.i": "dkim_identity",
    "header.d": "domain",
    "smtp.mailfrom": "addr_spec",
    "smtp.helo": "domain",
}
_AUTHENTICATION_WSP = " \t"


@dataclass(frozen=True, slots=True)
class SourceTrustDecision:
    trusted: bool
    reason: str
    verified: bool = True


def _decision(trusted: bool, reason: str, *, verified: bool = True) -> SourceTrustDecision:
    return SourceTrustDecision(trusted=trusted, reason=reason, verified=verified)


def _unknown_decision() -> SourceTrustDecision:
    return _decision(False, "source_not_evaluated", verified=False)


def _bounded_utf8_length(value: str, limit: int) -> Optional[int]:
    total = 0
    for character in value:
        codepoint = ord(character)
        if codepoint <= 0x7F:
            total += 1
        elif codepoint <= 0x7FF:
            total += 2
        elif codepoint <= 0xFFFF:
            total += 3
        else:
            total += 4
        if total > limit:
            return None
    return total


def strict_single_mailbox(sender: str) -> Optional[str]:
    if not isinstance(sender, str) or _bounded_utf8_length(sender, MAX_FROM_HEADER_BYTES) is None:
        return None
    value = sender.strip()
    if not value or any(character in value for character in ("\r", "\n", "\x00")):
        return None
    quoted = False
    escaped = False
    angle_depth = 0
    comment_depth = 0
    for character in value:
        if escaped:
            escaped = False
            continue
        if character == "\\" and (quoted or comment_depth):
            escaped = True
            continue
        if character == '"' and comment_depth == 0:
            quoted = not quoted
            continue
        if quoted:
            continue
        if character == "(":
            comment_depth += 1
            if comment_depth > MAX_AUTHENTICATION_COMMENT_DEPTH:
                return None
            continue
        if character == ")":
            if comment_depth == 0:
                return None
            comment_depth -= 1
            continue
        if comment_depth:
            continue
        if character == "<":
            if angle_depth:
                return None
            angle_depth = 1
            continue
        if character == ">":
            if not angle_depth:
                return None
            angle_depth = 0
            continue
        if not angle_depth and character in {",", ";", ":"}:
            return None
    if quoted or escaped or angle_depth or comment_depth:
        return None
    try:
        parsed = HeaderParser(policy=policy.default).parsestr(f"From: {value}\n\n")["From"]
    except Exception:
        return None
    if parsed is None or parsed.defects or len(parsed.addresses) != 1 or len(parsed.groups) != 1:
        return None
    if parsed.groups[0].display_name is not None:
        return None
    mailbox = (parsed.addresses[0].addr_spec or "").strip().lower()
    return mailbox or None


def safe_source_trust_reason(source_trusted: bool, reason: str) -> str:
    candidate = (reason or "").strip().lower()
    if candidate in SOURCE_TRUST_REASON_CODES:
        if source_trusted and candidate.startswith("trusted_"):
            return candidate
        if not source_trusted and not candidate.startswith("trusted_"):
            return candidate
    return "trusted_explicit" if source_trusted else "untrusted_explicit"


def persisted_source_trust_decision(sender: str, parsed_json: str) -> SourceTrustDecision:
    mailbox = strict_single_mailbox(sender)
    if mailbox is None:
        return _decision(False, "from_malformed")
    if mailbox != SORTSWIFT_EXPECTED_MAILBOX:
        return _decision(False, "from_unexpected")
    try:
        parsed = json.loads(parsed_json or "{}")
    except (TypeError, ValueError):
        return _unknown_decision()
    if not isinstance(parsed, dict) or type(parsed.get("source_trusted")) is not bool:
        return _unknown_decision()
    reason = parsed.get("source_trust_reason")
    if not isinstance(reason, str):
        return _unknown_decision()
    candidate = reason.strip().lower()
    if parsed["source_trusted"] is True:
        if candidate in VERIFIED_SOURCE_TRUST_REASONS:
            return _decision(True, candidate)
        return _unknown_decision()
    if candidate in VERIFIED_SOURCE_UNTRUST_REASONS:
        return _decision(False, candidate)
    return _unknown_decision()


def _replace_authentication_result_comments(value: str) -> Optional[str]:
    output: list[str] = []
    comment_depth = 0
    quoted = False
    quote_escaped = False
    comment_escaped = False
    for character in value:
        if comment_depth:
            if comment_escaped:
                comment_escaped = False
                continue
            if character == "\\":
                comment_escaped = True
                continue
            if character == "(":
                comment_depth += 1
                if comment_depth > MAX_AUTHENTICATION_COMMENT_DEPTH:
                    return None
                continue
            if character == ")":
                comment_depth -= 1
                if comment_depth == 0:
                    output.append(" ")
                continue
            continue
        if quoted:
            output.append(character)
            if quote_escaped:
                quote_escaped = False
            elif character == "\\":
                quote_escaped = True
            elif character == '"':
                quoted = False
            continue
        if character == '"':
            quoted = True
            output.append(character)
            continue
        if character == "(":
            comment_depth = 1
            continue
        if character == ")":
            return None
        output.append(character)
    if comment_depth or quoted or quote_escaped or comment_escaped:
        return None
    return "".join(output)


def _split_authentication_result_segments(value: str) -> Optional[list[str]]:
    segments: list[str] = []
    current: list[str] = []
    quoted = False
    escaped = False
    for character in value:
        if quoted:
            current.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                quoted = False
            continue
        if character == '"':
            quoted = True
            current.append(character)
            continue
        if character == ";":
            segment = "".join(current).strip(_AUTHENTICATION_WSP)
            if not segment:
                return None
            segments.append(segment)
            if len(segments) > MAX_AUTHENTICATION_RESULT_SEGMENTS:
                return None
            current = []
            continue
        current.append(character)
    if quoted or escaped:
        return None
    segment = "".join(current).strip(_AUTHENTICATION_WSP)
    if not segment:
        return None
    segments.append(segment)
    if len(segments) > MAX_AUTHENTICATION_RESULT_SEGMENTS + 1:
        return None
    return segments


def _authentication_result_parts(value: str) -> Optional[tuple[str, list[str]]]:
    if (
        not isinstance(value, str)
        or _bounded_utf8_length(value, MAX_AUTHENTICATION_RESULT_BYTES) is None
        or not value.strip(_AUTHENTICATION_WSP)
    ):
        return None
    if any(
        ord(character) > 126
        or ord(character) == 127
        or (ord(character) < 32 and character != "\t")
        for character in value
    ):
        return None
    without_comments = _replace_authentication_result_comments(value)
    if without_comments is None:
        return None
    segments = _split_authentication_result_segments(without_comments)
    if segments is None or len(segments) < 2:
        return None
    authserv_match = re.fullmatch(
        r"([A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)(?:[ \t]+1)?",
        segments[0],
    )
    if not authserv_match:
        return None
    authserv_id = authserv_match.group(1).lower().rstrip(".")
    if not _domain_is_or_subdomain(authserv_id, "google.com") and not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?",
        authserv_id,
    ):
        return None
    method_segments = segments[1:]
    if len(method_segments) > MAX_AUTHENTICATION_RESULT_SEGMENTS:
        return None
    return authserv_id, method_segments


def _strict_authentication_domain(value: str) -> Optional[str]:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip(_AUTHENTICATION_WSP)
        or len(value) > 253
    ):
        return None
    if any(ord(character) > 127 for character in value):
        return None
    domain = value.lower()
    labels = domain.split(".")
    if any(
        not label
        or len(label) > 63
        or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", label)
        for label in labels
    ):
        return None
    return domain


def _strict_addr_spec_domain(value: str, *, allow_empty_local: bool = False) -> Optional[str]:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip(_AUTHENTICATION_WSP)
        or value.count("@") != 1
    ):
        return None
    local_part, domain_value = value.rsplit("@", 1)
    if not local_part:
        if not allow_empty_local:
            return None
    else:
        atom = r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+"
        if not re.fullmatch(rf"{atom}(?:\.{atom})*", local_part):
            return None
    return _strict_authentication_domain(domain_value)


def _authentication_identity_domain(
    property_name: str,
    value: str,
    *,
    quoted: bool,
) -> Optional[str]:
    if quoted:
        return None
    kind = _AUTHENTICATION_IDENTITY_KINDS.get(property_name)
    if kind == "domain":
        return _strict_authentication_domain(value)
    if kind == "addr_spec":
        return _strict_addr_spec_domain(value)
    if kind == "dkim_identity":
        return _strict_addr_spec_domain(value, allow_empty_local=True)
    return None


def _domain_is_or_subdomain(value: str, expected_domain: str) -> bool:
    domain = _strict_authentication_domain(value)
    expected = (expected_domain or "").strip(_AUTHENTICATION_WSP).lower().rstrip(".")
    return bool(domain and expected and (domain == expected or domain.endswith(f".{expected}")))


def _valid_dkim_selector(value: str) -> bool:
    return _strict_authentication_domain(value) is not None


def _valid_dkim_signature_token(value: str) -> bool:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 1024
        or not re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", value)
    ):
        return False
    padding_length = len(value) - len(value.rstrip("="))
    if padding_length:
        return len(value) % 4 == 0
    return len(value) % 4 != 1


def _lex_authentication_key_values(segment: str) -> Optional[list[tuple[str, str, bool]]]:
    tokens: list[tuple[str, str, bool]] = []
    index = 0
    length = len(segment)
    while index < length:
        while index < length and segment[index] in _AUTHENTICATION_WSP:
            index += 1
        if index >= length:
            break
        key_start = index
        while index < length and segment[index] != "=" and segment[index] not in _AUTHENTICATION_WSP:
            if segment[index] in {'"', "\\"}:
                return None
            index += 1
        if index == key_start or index >= length or segment[index] != "=":
            return None
        key = segment[key_start:index]
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]*", key):
            return None
        index += 1
        if index >= length or segment[index] in _AUTHENTICATION_WSP:
            return None

        quoted = segment[index] == '"'
        if quoted:
            index += 1
            value_characters: list[str] = []
            closed = False
            while index < length:
                character = segment[index]
                if character == "\\":
                    if index + 1 >= length or segment[index + 1] not in {'"', "\\"}:
                        return None
                    value_characters.append(segment[index + 1])
                    index += 2
                    continue
                if character == '"':
                    closed = True
                    index += 1
                    break
                if ord(character) < 32 or ord(character) > 126:
                    return None
                value_characters.append(character)
                index += 1
            if not closed or index < length and segment[index] not in _AUTHENTICATION_WSP:
                return None
            value = "".join(value_characters)
        else:
            value_start = index
            while index < length and segment[index] not in _AUTHENTICATION_WSP:
                if segment[index] in {'"', "\\"}:
                    return None
                index += 1
            value = segment[value_start:index]
            if not re.fullmatch(r"[A-Za-z0-9!#$%&'*+./:=?@^_`{|}~-]+", value):
                return None
        if not value:
            return None
        tokens.append((key.lower(), value, quoted))
        if len(tokens) > MAX_AUTHENTICATION_TOKENS_PER_SEGMENT:
            return None
    return tokens or None


def _parse_authentication_method_segment(
    segment: str,
) -> Optional[tuple[str, str, dict[str, list[tuple[str, bool]]]]]:
    tokens = _lex_authentication_key_values(segment)
    if tokens is None:
        return None
    method, result, result_quoted = tokens[0]
    result = result.lower()
    allowed_results = _AUTHENTICATION_METHOD_RESULTS.get(method)
    if result_quoted or allowed_results is None or result not in allowed_results:
        return None
    allowed_properties = _AUTHENTICATION_METHOD_PROPERTIES[method]
    properties: dict[str, list[tuple[str, bool]]] = {}
    property_seen = False
    reason_seen = False
    for key, value, quoted in tokens[1:]:
        if key != "reason" and key not in allowed_properties:
            return None
        if quoted and key != "reason":
            return None
        if key == "reason":
            if property_seen or reason_seen:
                return None
            if not quoted and not re.fullmatch(r"[A-Za-z0-9!#$%&'*+.^_`{|}~-]+", value):
                return None
            reason_seen = True
        else:
            property_seen = True
            if method == "dkim" and key == "header.s" and not _valid_dkim_selector(value):
                return None
            if method == "dkim" and key == "header.b" and not _valid_dkim_signature_token(value):
                return None
        properties.setdefault(key, []).append((value, quoted))
    return method, result, properties


def _aligned_pass_exists(
    method_segments: list[tuple[str, str, dict[str, list[tuple[str, bool]]]]],
    method: str,
    identity_properties: tuple[str, ...],
) -> bool:
    for segment_method, result, properties in method_segments:
        if segment_method != method or result != "pass":
            continue
        identity_domains = [
            _authentication_identity_domain(property_name, value, quoted=quoted)
            for property_name in identity_properties
            for value, quoted in properties.get(property_name, [])
        ]
        if identity_domains and all(
            domain and _domain_is_or_subdomain(domain, SORTSWIFT_ORGANIZATIONAL_DOMAIN)
            for domain in identity_domains
        ):
            return True
    return False


def _bounded_authentication_results(
    authentication_results: Iterable[str],
) -> Optional[list[str]]:
    if isinstance(authentication_results, (str, bytes)):
        return None
    values: list[str] = []
    aggregate_bytes = 0
    try:
        iterator = iter(authentication_results)
    except TypeError:
        return None
    for value in iterator:
        if len(values) >= MAX_AUTHENTICATION_RESULTS_HEADERS or not isinstance(value, str):
            return None
        value_bytes = _bounded_utf8_length(value, MAX_AUTHENTICATION_RESULT_BYTES)
        if value_bytes is None:
            return None
        aggregate_bytes += value_bytes
        if aggregate_bytes > MAX_AUTHENTICATION_RESULTS_AGGREGATE_BYTES:
            return None
        values.append(value)
    return values


def evaluate_sortswift_source_authentication(
    sender: str,
    authentication_results: Iterable[str],
) -> SourceTrustDecision:
    if not isinstance(sender, str):
        return _decision(False, "from_missing")
    if _bounded_utf8_length(sender, MAX_FROM_HEADER_BYTES) is None:
        return _decision(False, "from_malformed")
    if not sender.strip(_AUTHENTICATION_WSP):
        return _decision(False, "from_missing")
    mailbox = strict_single_mailbox(sender)
    if mailbox is None:
        return _decision(False, "from_malformed")
    if mailbox != SORTSWIFT_EXPECTED_MAILBOX:
        return _decision(False, "from_unexpected")
    bounded_results = _bounded_authentication_results(authentication_results)
    if bounded_results is None:
        return _decision(False, "auth_malformed")
    if not bounded_results:
        return _decision(False, "auth_missing")

    parsed_results: list[tuple[str, list[str]]] = []
    for value in bounded_results:
        parsed = _authentication_result_parts(value)
        if parsed is None:
            return _decision(False, "auth_malformed")
        parsed_results.append(parsed)
    google_results = []
    non_google_results = []
    for authserv_id, segments in parsed_results:
        if _domain_is_or_subdomain(authserv_id, "google.com"):
            google_results.append(segments)
        else:
            non_google_results.append(segments)
    if google_results and non_google_results:
        return _decision(False, "auth_mixed_receivers")
    if not google_results:
        return _decision(False, "auth_no_google_receiver")
    if len(google_results) != 1:
        return _decision(False, "auth_ambiguous_google_receiver")

    method_segments: list[tuple[str, str, dict[str, list[tuple[str, bool]]]]] = []
    for segment in google_results[0]:
        parsed_segment = _parse_authentication_method_segment(segment)
        if parsed_segment is None:
            return _decision(False, "auth_malformed")
        method_segments.append(parsed_segment)

    required_pass_properties = {
        "dmarc": ("header.from",),
        "dkim": ("header.i", "header.d"),
        "spf": ("smtp.mailfrom", "smtp.helo"),
    }
    seen_methods: set[str] = set()
    for method, _result, _properties in method_segments:
        if method in seen_methods and method != "dkim":
            return _decision(False, "auth_ambiguous_results")
        seen_methods.add(method)
    for method, result, properties in method_segments:
        if any(len(values) != 1 for values in properties.values()):
            return _decision(False, "auth_ambiguous_results")
        if method == "spf":
            identity_count = sum(
                property_name in properties
                for property_name in ("smtp.mailfrom", "smtp.helo")
            )
            if identity_count == 0:
                return _decision(False, "auth_malformed")
            if identity_count > 1:
                return _decision(False, "auth_ambiguous_results")
        for property_name, values in properties.items():
            if property_name not in _AUTHENTICATION_IDENTITY_KINDS:
                continue
            value, quoted = values[0]
            domain = _authentication_identity_domain(property_name, value, quoted=quoted)
            if domain is None:
                return _decision(False, "auth_malformed")
        if method == "dkim" and "header.i" in properties and "header.d" in properties:
            i_value, i_quoted = properties["header.i"][0]
            d_value, d_quoted = properties["header.d"][0]
            i_domain = _authentication_identity_domain("header.i", i_value, quoted=i_quoted)
            d_domain = _authentication_identity_domain("header.d", d_value, quoted=d_quoted)
            if not i_domain or not d_domain or not _domain_is_or_subdomain(i_domain, d_domain):
                return _decision(False, "auth_ambiguous_results")
        if result == "pass" and method in required_pass_properties:
            required_identity_domains = [
                _authentication_identity_domain(property_name, value, quoted=quoted)
                for property_name in required_pass_properties[method]
                for value, quoted in properties.get(property_name, [])
            ]
            if not required_identity_domains:
                return _decision(False, "auth_malformed")
            alignment = [
                bool(domain and _domain_is_or_subdomain(domain, SORTSWIFT_ORGANIZATIONAL_DOMAIN))
                for domain in required_identity_domains
            ]
            if any(alignment) and not all(alignment):
                return _decision(False, "auth_ambiguous_results")

    if _aligned_pass_exists(method_segments, "dmarc", ("header.from",)):
        return _decision(True, "trusted_dmarc_aligned")
    if _aligned_pass_exists(method_segments, "dkim", ("header.i", "header.d")):
        return _decision(True, "trusted_dkim_aligned")
    if _aligned_pass_exists(method_segments, "spf", ("smtp.mailfrom",)):
        return _decision(True, "trusted_spf_aligned")
    return _decision(False, "auth_no_aligned_pass")


def source_authentication_from_headers(headers: Any) -> tuple[str, SourceTrustDecision]:
    if not isinstance(headers, list):
        return "", _decision(False, "from_missing")
    sender = ""
    from_count = 0
    authentication_results: list[str] = []
    aggregate_authentication_bytes = 0
    for header in headers:
        if not isinstance(header, dict):
            continue
        raw_name = header.get("name")
        if not isinstance(raw_name, str) or len(raw_name) > 32:
            continue
        name = raw_name.lower()
        if name not in {"from", "authentication-results"}:
            continue
        value = header.get("value")
        if not isinstance(value, str):
            reason = "from_malformed" if name == "from" else "auth_malformed"
            return sender, _decision(False, reason)
        if name == "from":
            from_count += 1
            if from_count > 1 or _bounded_utf8_length(value, MAX_FROM_HEADER_BYTES) is None:
                return sender, _decision(False, "from_malformed")
            sender = value
            continue
        if len(authentication_results) >= MAX_AUTHENTICATION_RESULTS_HEADERS:
            return sender, _decision(False, "auth_malformed")
        value_bytes = _bounded_utf8_length(value, MAX_AUTHENTICATION_RESULT_BYTES)
        if value_bytes is None:
            return sender, _decision(False, "auth_malformed")
        aggregate_authentication_bytes += value_bytes
        if aggregate_authentication_bytes > MAX_AUTHENTICATION_RESULTS_AGGREGATE_BYTES:
            return sender, _decision(False, "auth_malformed")
        authentication_results.append(value)
    if from_count == 0:
        return "", _decision(False, "from_missing")
    return sender, evaluate_sortswift_source_authentication(sender, authentication_results)
