"""
pybinolla/http_client.py
=========================
HTTP layer: TLS-fingerprint-safe browser session, REST login, cookie management.

Wire-verified login flow (live-tested):
    POST https://binolla.com/api/auth/login
         {"email": "...", "password": "..."}
    → 200 {"token": {"accessToken": "<JWT>"}, "user": {"id": 619481, ...}}

No CSRF token is involved — the endpoint is plain JSON REST.
"""

from __future__ import annotations

import ssl
import json
import logging
import threading
from pathlib import Path
from typing import Optional

import certifi
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import BinollaAuthError

logger = logging.getLogger(__name__)

_CERT_PATH = certifi.where()

_RETRY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
)

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) "
    "Gecko/20100101 Firefox/119.0"
)
_session_lock = threading.Lock()


class _TLSAdapter(HTTPAdapter):
    """Forces cipher suites that mimic a real Firefox TLS fingerprint."""

    _CIPHERS = (
        "ECDHE-ECDSA-AES128-GCM-SHA256:"
        "ECDHE-RSA-AES128-GCM-SHA256:"
        "ECDHE-ECDSA-AES256-GCM-SHA384:"
        "ECDHE-RSA-AES256-GCM-SHA384:"
        "ECDHE-ECDSA-CHACHA20-POLY1305:"
        "ECDHE-RSA-CHACHA20-POLY1305"
    )

    def __init__(self, **kwargs):
        self._ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        self._ssl_ctx.set_ciphers(self._CIPHERS)
        self._ssl_ctx.set_ecdh_curve("prime256v1")
        self._ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        self._ssl_ctx.maximum_version = ssl.TLSVersion.TLSv1_3
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_ctx
        return super().init_poolmanager(*args, **kwargs)


class BrowserSession(Session):
    """Requests session that looks like a real Firefox browser."""

    def __init__(self, user_agent: Optional[str] = None):
        super().__init__()
        self.user_agent = user_agent or DEFAULT_UA
        self.headers.update({
            "User-Agent":      self.user_agent,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT":             "1",
        })
        self.mount("https://", _TLSAdapter(max_retries=_RETRY))

    def get_cookies_str(self) -> str:
        return "; ".join(f"{c.name}={c.value}" for c in self.cookies)


BINOLLA_HOST = "binolla.com"


class BinollaLogin(BrowserSession):
    """
    Handles HTTP-based authentication against binolla.com.

    Flow (verified against the live platform)
    ----------------------------------------
    1. GET  /          → prime cookies (Cloudflare friendly session)
    2. POST /api/auth/login  {"email","password"}
    3. Extract token.accessToken + user.id

    Attributes set after a successful login():
        ssid        → JWT access token (WebSocket auth)
        uid         → numeric user id (e.g. 619481)
        user_data   → full user object from the login response
        cookies_str → raw cookie string
    """

    def __init__(self, host: str = BINOLLA_HOST, lang: str = "en"):
        super().__init__()
        self.host      = host
        self.lang      = lang
        self.base_url  = f"https://{host}"
        self.response  = None
        self.ssid: Optional[str] = None
        self.uid: Optional[int]  = None
        self.user_data: dict     = {}
        self.cookies_str: str    = ""

    def _get(self, url: str, **kwargs) -> requests.Response:
        self.response = self.get(url, **kwargs)
        return self.response

    # ------------------------------------------------------------------
    def login(self, email: str, password: str) -> tuple[bool, str]:
        """
        Full login flow. Returns (success, message).

        On success sets ssid / uid / user_data / cookies_str.
        """
        # 1. prime the session with cookies (avoid CF cold-post blocks)
        try:
            self._get(self.base_url, timeout=20)
        except Exception as exc:
            logger.debug("Homepage priming failed (non-fatal): %s", exc)

        # 2. submit credentials to the REST API
        self.response = self.request(
            "POST",
            f"{self.base_url}/api/auth/login",
            json={"email": email, "password": password},
            headers={
                "Content-Type": "application/json",
                "Accept":       "application/json, text/plain, */*",
                "Referer":      f"{self.base_url}/sign-in",
            },
            timeout=20,
        )

        if self.response.status_code == 403:
            return False, "HTTP 403 — request blocked (Cloudflare/TLS fingerprint)."

        if self.response.status_code != 200:
            try:
                err = self.response.json()
                msg = err.get("message") or err.get("error") or str(err)[:200]
            except Exception:
                msg = self.response.text[:200]
            return False, f"Login failed (HTTP {self.response.status_code}): {msg}"

        try:
            data = self.response.json()
        except json.JSONDecodeError:
            return False, "Login endpoint did not return JSON."

        token_obj = data.get("token", {})
        token = token_obj.get("accessToken") if isinstance(token_obj, dict) else None
        if not token:
            return False, "Login OK but token.accessToken missing."

        user = data.get("user", {})
        self.ssid       = token
        self.uid        = int(user.get("id") or 0) or None
        self.user_data  = user
        self.cookies_str = self.get_cookies_str()

        logger.info("Login OK — user %s", self.uid)
        return True, "Login successful."


# ---------------------------------------------------------------------------
# Session file helpers (mirrors pyquotex's load_session / save_session)
# ---------------------------------------------------------------------------
_SESSION_FILE = Path("binolla_session.json")


def load_session(email: str) -> dict:
    with _session_lock:
        if _SESSION_FILE.exists():
            try:
                data = json.loads(_SESSION_FILE.read_text())
                if email in data:
                    return data[email]
            except json.JSONDecodeError:
                pass
        return {"cookies": None, "token": None, "uid": None, "user_agent": DEFAULT_UA}


def save_session(email: str, session: dict) -> None:
    with _session_lock:
        all_sessions: dict = {}
        if _SESSION_FILE.exists():
            try:
                all_sessions = json.loads(_SESSION_FILE.read_text())
            except json.JSONDecodeError:
                pass
        all_sessions[email] = session
        _SESSION_FILE.write_text(json.dumps(all_sessions, indent=2))


def delete_session() -> None:
    if _SESSION_FILE.exists():
        try:
            _SESSION_FILE.unlink()
        except Exception:
            pass
