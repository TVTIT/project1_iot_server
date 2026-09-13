#!/usr/bin/env python3
import hmac
import hashlib
import base64
import json
import time
import sys

def base64url_encode(input_bytes):
    return base64.urlsafe_b64encode(input_bytes).decode('utf-8').rstrip('=')

def create_jwt(payload, secret):
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = base64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))
    payload_b64 = base64url_encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))
    signing_input = f"{header_b64}.{payload_b64}".encode('utf-8')
    signature = hmac.new(secret.encode('utf-8'), signing_input, hashlib.sha256).digest()
    sig_b64 = base64url_encode(signature)
    return f"{header_b64}.{payload_b64}.{sig_b64}"

def main():
    secret = sys.argv[1] if len(sys.argv) > 1 else "super-secret-jwt-token-with-at-least-32-characters-long"
    
    # anon key payload
    now = int(time.time())
    exp = now + 10 * 365 * 24 * 3600 # 10 years
    anon_payload = {
        "role": "anon",
        "iss": "supabase",
        "iat": now,
        "exp": exp
    }
    
    # service_role key payload
    service_payload = {
        "role": "service_role",
        "iss": "supabase",
        "iat": now,
        "exp": exp
    }
    
    anon_token = create_jwt(anon_payload, secret)
    service_token = create_jwt(service_payload, secret)
    
    print(f"ANON_KEY={anon_token}")
    print(f"SERVICE_ROLE_KEY={service_token}")

if __name__ == "__main__":
    main()
