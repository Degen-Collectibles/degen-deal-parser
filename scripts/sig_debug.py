"""
Capture the next TikTok webhook body + headers to a file for signature debugging.
Run on Machine B: .venv/Scripts/python.exe scripts/sig_debug.py
Reads from a captured file if it exists, otherwise waits for the webhook handler to create it.
"""
import os
import sys
import json
import hmac
import hashlib

CAPTURE_FILE = "C:/Users/Degen/degen-deal-parser/logs/webhook_capture.json"
ENV_FILE = "C:/Users/Degen/degen-deal-parser/.env"

def load_env():
    if os.path.exists(ENV_FILE):
        for line in open(ENV_FILE):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def try_all_methods(raw_body: bytes, secret: str, received_sig: str, path: str, payload_ts: str):
    secret_bytes = secret.encode("utf-8")
    body_text = raw_body.decode("utf-8", errors="ignore")
    received_lower = received_sig.lower().strip()

    candidates = []

    # HMAC candidates
    candidates.append(("hmac(secret, body)", hmac.new(secret_bytes, raw_body, hashlib.sha256).hexdigest()))
    candidates.append(("hmac(secret, body_text)", hmac.new(secret_bytes, body_text.encode("utf-8"), hashlib.sha256).hexdigest()))
    candidates.append(("hmac(secret, s+body+s)", hmac.new(secret_bytes, f"{secret}{body_text}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
    candidates.append(("hmac(secret, s+path+body+s)", hmac.new(secret_bytes, f"{secret}{path}{body_text}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
    if payload_ts:
        candidates.append(("hmac(secret, ts.body)", hmac.new(secret_bytes, f"{payload_ts}.{body_text}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("hmac(secret, ts+body)", hmac.new(secret_bytes, f"{payload_ts}{body_text}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("hmac(secret, s+path+ts+body+s)", hmac.new(secret_bytes, f"{secret}{path}{payload_ts}{body_text}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("hmac(secret, path+timestamp+ts+body)", hmac.new(secret_bytes, f"{path}timestamp{payload_ts}{body_text}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("hmac(secret, s+path+timestamp+ts+body+s)", hmac.new(secret_bytes, f"{secret}{path}timestamp{payload_ts}{body_text}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))

    # Plain SHA256 candidates
    candidates.append(("sha256(s+body)", hashlib.sha256(f"{secret}{body_text}".encode("utf-8")).hexdigest()))
    candidates.append(("sha256(body+s)", hashlib.sha256(f"{body_text}{secret}".encode("utf-8")).hexdigest()))
    candidates.append(("sha256(s+body+s)", hashlib.sha256(f"{secret}{body_text}{secret}".encode("utf-8")).hexdigest()))
    candidates.append(("sha256(s+path+body+s)", hashlib.sha256(f"{secret}{path}{body_text}{secret}".encode("utf-8")).hexdigest()))
    if payload_ts:
        candidates.append(("sha256(s+path+ts+body+s)", hashlib.sha256(f"{secret}{path}{payload_ts}{body_text}{secret}".encode("utf-8")).hexdigest()))
        candidates.append(("sha256(s+path+timestamp+ts+body+s)", hashlib.sha256(f"{secret}{path}timestamp{payload_ts}{body_text}{secret}".encode("utf-8")).hexdigest()))

    # TikTok API-style: sorted key-value params
    # For webhooks, there are no params, so it's just path + body
    candidates.append(("sha256(s+path+body+s) [raw bytes]", hashlib.sha256((secret + path).encode("utf-8") + raw_body + secret.encode("utf-8")).hexdigest()))
    candidates.append(("hmac(secret, s+path+body+s) [raw bytes]", hmac.new(secret_bytes, (secret + path).encode("utf-8") + raw_body + secret.encode("utf-8"), hashlib.sha256).hexdigest()))

    # Try with body as canonical JSON (sorted keys, no spaces)
    try:
        parsed = json.loads(body_text)
        canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        candidates.append(("hmac(secret, canonical_body)", hmac.new(secret_bytes, canonical.encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("sha256(s+canonical+s)", hashlib.sha256(f"{secret}{canonical}{secret}".encode("utf-8")).hexdigest()))
        candidates.append(("hmac(secret, s+canonical+s)", hmac.new(secret_bytes, f"{secret}{canonical}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("hmac(secret, s+path+canonical+s)", hmac.new(secret_bytes, f"{secret}{path}{canonical}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("sha256(s+path+canonical+s)", hashlib.sha256(f"{secret}{path}{canonical}{secret}".encode("utf-8")).hexdigest()))
    except json.JSONDecodeError:
        pass

    print(f"\nReceived sig: {received_lower}")
    print(f"Secret len:   {len(secret)} chars")
    print(f"Body len:     {len(raw_body)} bytes")
    print(f"Path:         {path}")
    print(f"Payload ts:   {payload_ts}")
    print(f"\n{'Method':<55} {'Digest (first 32 chars)':<40} {'Match'}")
    print("-" * 105)

    for label, digest in candidates:
        match = "YES <<<" if hmac.compare_digest(received_lower, digest.lower()) else ""
        print(f"{label:<55} {digest[:32]:<40} {match}")

    return any(hmac.compare_digest(received_lower, d.lower()) for _, d in candidates)


def main():
    load_env()
    secret = os.environ.get("TIKTOK_APP_SECRET", "").strip()
    if not secret:
        print("ERROR: TIKTOK_APP_SECRET not found in environment")
        sys.exit(1)
    print(f"Secret loaded: {len(secret)} chars, starts with: {secret[:3]}***")

    if os.path.exists(CAPTURE_FILE):
        with open(CAPTURE_FILE) as f:
            capture = json.load(f)
        raw_body = capture["raw_body"].encode("utf-8")
        received_sig = capture["received_signature"]
        path = capture.get("request_path", "/webhooks/tiktok/orders")
        payload_ts = capture.get("payload_timestamp", "")
        headers = capture.get("headers", {})
        print(f"\nLoaded captured webhook from {CAPTURE_FILE}")
        print(f"Headers: {json.dumps(headers, indent=2)}")
        found = try_all_methods(raw_body, secret, received_sig, path, payload_ts)
        if not found:
            print("\n*** NO MATCH FOUND with any method ***")
        else:
            print("\n*** MATCH FOUND! ***")
    else:
        print(f"\nNo capture file found at {CAPTURE_FILE}")
        print("The webhook handler will create it on the next webhook.")


if __name__ == "__main__":
    main()
