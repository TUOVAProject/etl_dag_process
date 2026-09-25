from __future__ import annotations
import time
import requests
from typing import Optional
from erp_pipeline.config.config import USERNAME, PASSWORD, LOGIN_URL, AUTHVALUE

LOGINHEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache"
}
LOGINBODY = {
    "Username": USERNAME,
    "Password": PASSWORD,
    "ForceReLogin": True
}

def login_and_get_token(login_url: Optional[str] = None) -> dict:
    login_url = login_url or LOGIN_URL
    url = f"{login_url}/login"
    response = requests.post(url, headers=LOGINHEADERS, json=LOGINBODY, verify=False)
    response.raise_for_status()

    token = response.headers.get(AUTHVALUE)
    if not token:
        raise RuntimeError("Login response did not contain Auth-header")

    return {"token": token, "expires_at": time.time() + 3300}