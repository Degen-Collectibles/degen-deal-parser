"""
TikTok webhook signature debugger.
Run on Machine B: .venv/Scripts/python.exe scripts/sig_debug.py
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

def try_all_methods(raw_body: bytes, secret: str, received_sig: str, path: str,
                    payload_ts: str, header_ts: str = None):
    secret_bytes = secret.encode("utf-8")
    body_text = raw_body.decode("utf-8", errors="ignore")
    received_lower = received_sig.lower().strip()

    all_timestamps = []
    for ts in (header_ts, payload_ts):
        t = (ts or "").strip()
        if t and t not in all_timestamps:
            all_timestamps.append(t)

    candidates = []

    # --- HMAC candidates (no timestamp) ---
    candidates.append(("hmac(secret, body)", hmac.new(secret_bytes, raw_body, hashlib.sha256).hexdigest()))
    candidates.append(("hmac(secret, path+body)", hmac.new(secret_bytes, (path + body_text).encode("utf-8"), hashlib.sha256).hexdigest()))
    candidates.append(("hmac(secret, path+body) [raw]", hmac.new(secret_bytes, path.encode("utf-8") + raw_body, hashlib.sha256).hexdigest()))
    candidates.append(("hmac(secret, s+body+s)", hmac.new(secret_bytes, f"{secret}{body_text}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
    candidates.append(("hmac(secret, s+path+body+s)", hmac.new(secret_bytes, f"{secret}{path}{body_text}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
    candidates.append(("sha256(s+body)", hashlib.sha256(f"{secret}{body_text}".encode("utf-8")).hexdigest()))
    candidates.append(("sha256(body+s)", hashlib.sha256(f"{body_text}{secret}".encode("utf-8")).hexdigest()))
    candidates.append(("sha256(s+body+s)", hashlib.sha256(f"{secret}{body_text}{secret}".encode("utf-8")).hexdigest()))
    candidates.append(("sha256(s+path+body+s)", hashlib.sha256(f"{secret}{path}{body_text}{secret}".encode("utf-8")).hexdigest()))
    candidates.append(("sha256(s+path+body+s) [raw]", hashlib.sha256(secret.encode("utf-8") + path.encode("utf-8") + raw_body + secret.encode("utf-8")).hexdigest()))
    candidates.append(("sha256(path+body+s)", hashlib.sha256(f"{path}{body_text}{secret}".encode("utf-8")).hexdigest()))

    # --- Timestamp-based candidates ---
    for ts in all_timestamps:
        ts_label = f"hdr_ts={ts}" if ts == header_ts else f"pl_ts={ts}"

        # Official TikTok docs: HMAC-SHA256(client_secret, timestamp + "." + body)
        candidates.append((f"hmac(secret, ts.body) [{ts_label}]",
            hmac.new(secret_bytes, f"{ts}.{body_text}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append((f"hmac(secret, ts.body) [raw,{ts_label}]",
            hmac.new(secret_bytes, (ts + ".").encode("utf-8") + raw_body, hashlib.sha256).hexdigest()))
        candidates.append((f"hmac(secret, ts+body) [{ts_label}]",
            hmac.new(secret_bytes, f"{ts}{body_text}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append((f"hmac(secret, path+ts+body) [{ts_label}]",
            hmac.new(secret_bytes, f"{path}{ts}{body_text}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append((f"hmac(secret, s+path+ts+body+s) [{ts_label}]",
            hmac.new(secret_bytes, f"{secret}{path}{ts}{body_text}{secret}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append((f"sha256(s+path+ts+body+s) [{ts_label}]",
            hashlib.sha256(f"{secret}{path}{ts}{body_text}{secret}".encode("utf-8")).hexdigest()))
        candidates.append((f"sha256(ts.body) [{ts_label}]",
            hashlib.sha256(f"{ts}.{body_text}".encode("utf-8")).hexdigest()))
        candidates.append((f"sha256(s+ts.body) [{ts_label}]",
            hashlib.sha256(f"{secret}{ts}.{body_text}".encode("utf-8")).hexdigest()))

    # --- Canonical JSON candidates ---
    try:
        parsed = json.loads(body_text)
        canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        candidates.append(("hmac(secret, canonical)", hmac.new(secret_bytes, canonical.encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("hmac(secret, path+canonical)", hmac.new(secret_bytes, (path + canonical).encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("sha256(s+canonical+s)", hashlib.sha256(f"{secret}{canonical}{secret}".encode("utf-8")).hexdigest()))
        candidates.append(("sha256(s+path+canonical+s)", hashlib.sha256(f"{secret}{path}{canonical}{secret}".encode("utf-8")).hexdigest()))
        for ts in all_timestamps:
            ts_label = f"hdr_ts" if ts == header_ts else f"pl_ts"
            candidates.append((f"hmac(secret, ts.canonical) [{ts_label}]",
                hmac.new(secret_bytes, f"{ts}.{canonical}".encode("utf-8"), hashlib.sha256).hexdigest()))
    except json.JSONDecodeError:
        pass

    # --- Try with app_key as HMAC key (in case TikTok uses app_key not app_secret) ---
    app_key = os.environ.get("TIKTOK_APP_KEY", "").strip()
    if app_key and app_key != secret:
        ak_bytes = app_key.encode("utf-8")
        candidates.append(("hmac(app_key, body)", hmac.new(ak_bytes, raw_body, hashlib.sha256).hexdigest()))
        for ts in all_timestamps:
            ts_label = f"hdr_ts" if ts == header_ts else f"pl_ts"
            candidates.append((f"hmac(app_key, ts.body) [{ts_label}]",
                hmac.new(ak_bytes, f"{ts}.{body_text}".encode("utf-8"), hashlib.sha256).hexdigest()))
        candidates.append(("sha256(app_key+body+app_key)", hashlib.sha256(f"{app_key}{body_text}{app_key}".encode("utf-8")).hexdigest()))

    print(f"\nReceived sig: {received_lower}")
    print(f"Secret len:   {len(secret)} chars")
    if app_key:
        print(f"App key len:  {len(app_key)} chars")
    print(f"Body len:     {len(raw_body)} bytes")
    print(f"Path:         {path}")
    print(f"Header ts:    {header_ts or '(none)'}")
    print(f"Payload ts:   {payload_ts or '(none)'}")
    print(f"\n{'Method':<60} {'Digest (first 32 chars)':<40} {'Match'}")
    print("-" * 110)

    for label, digest in candidates:
        match = "YES <<<" if hmac.compare_digest(received_lower, digest.lower()) else ""
        print(f"{label:<60} {digest[:32]:<40} {match}")

    return any(hmac.compare_digest(received_lower, d.lower()) for _, d in candidates)


def main():
    load_env()
    secret = os.environ.get("TIKTOK_APP_SECRET", "").strip()
    if not secret:
        print("ERROR: TIKTOK_APP_SECRET not found in environment")
        sys.exit(1)
    print(f"Secret loaded: {len(secret)} chars, starts with: {secret[:3]}***")

    if not os.path.exists(CAPTURE_FILE):
        print(f"\nNo capture file found at {CAPTURE_FILE}")
        print("The webhook handler will create it on the next webhook.")
        return

    with open(CAPTURE_FILE) as f:
        capture = json.load(f)
    raw_body = capture["raw_body"].encode("utf-8")
    received_sig = capture.get("received_signature") or capture.get("parsed_header_signature") or ""
    path = capture.get("request_path", "/webhooks/tiktok/orders")
    payload_ts = capture.get("payload_timestamp", "")
    header_ts = capture.get("parsed_header_timestamp")
    all_headers = capture.get("all_headers") or capture.get("headers") or {}

    print(f"\nLoaded captured webhook from {CAPTURE_FILE}")
    print(f"\nAll headers ({len(all_headers)}):")
    for k, v in sorted(all_headers.items()):
        print(f"  {k}: {v}")

    found = try_all_methods(raw_body, secret, received_sig, path, payload_ts, header_ts)
    if not found:
        print("\n*** NO MATCH FOUND with any method ***")
        print("\nPossible causes:")
        print("  1. TikTok Shop uses a different secret than TIKTOK_APP_SECRET for webhooks")
        print("  2. The signing algorithm uses the full callback URL, not just the path")
        print("  3. The `client_secret` in TikTok's docs differs from `app_secret`")
    else:
        print("\n*** MATCH FOUND! ***")


if __name__ == "__main__":
    main()
