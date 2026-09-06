import requests
from config.config import LOGIN_URL, LOGINBODY, LOGINHEADERS
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
TOKEN_FILE = Path("/tmp/monitor_api_token.txt") #TODO : Vaihda

def _login_and_get_token():
    url = f"{LOGIN_URL}/login"

    response = requests.post(url, 
                             headers=LOGINHEADERS,
                             json=LOGINBODY,verify=False)

    response.raise_for_status()
    token = response.headers.get("X-Monitor-SessionId")
    if not token:
        raise RuntimeError("Login response did not contain Auth-header")
    TOKEN_FILE.write_text(token)
    return token

def get_token():
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text().strip()
    return _login_and_get_token()

def force_refresh_token():
    return _login_and_get_token()