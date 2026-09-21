#!/usr/bin/env python3
import hmac
import hashlib
import base64
import json
import time
import sys
import os
from pathlib import Path

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

def get_secret_from_env():
    # 1. CLI argument
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1].strip()

    # 2. Environment variable
    env_secret = os.environ.get("JWT_SECRET")
    if env_secret and env_secret.strip():
        return env_secret.strip()

    # 3. Read from .env file in project root
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.is_file():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("JWT_SECRET=") and not line.startswith("#"):
                    val = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if val:
                        return val

    return "super-secret-jwt-token-with-at-least-32-characters-long"

def main():
    secret = get_secret_from_env()
    
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
