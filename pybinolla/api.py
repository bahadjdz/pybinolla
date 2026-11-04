"""
pybinolla/api.py
=================
BinollaAPI — low-level core combining HTTP auth + WebSocket transport.
Mirrors pyquotex's QuotexAPI class.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from .connection import ConnectionState, wait_for_first_event
from .enums import AccountType
from .event_registry import EventRegistry
from .exceptions import BinollaConnectionError
from .http_client import BinollaLogin, load_session, save_session
from .websocket_client import BinollaWebSocket, BINOLLA_WSS

logger = logging.getLogger(__name__)

_request_counter = itertools.count(int(time.time() * 1000))


class BinollaAPI:
    """
    Orchestrates:
      • HTTP login  → SSID / cookies
      • WebSocket   → real-time data + trade execution
      • Rate limiting
      • Event registry for async responses
    """

    def __init__(self,
                 host:          str = "binolla.com",
                 username:      str = "",
                 password:      str = "",
                 lang:          str = "en",
                 proxies:       Optional[dict] = None):
        self.host          = host
        self.username      = username
        self.password      = password
        self.lang          = lang
        self.proxies       = proxies
        self.https_url     = f"https://{host}"
        self.wss_url       = BINOLLA_WSS

        self.state         = ConnectionState()
        self.event_registry= EventRegistry()
        self.session_data: dict = {}

        self.account_type  = AccountType.DEMO
        self.auth_uaid: Optional[int] = None
        self.account_balance: Optional[dict] = None
        self.candle_v2_data: Dict[str, Any]  = {}
        self.realtime_candles: Dict[str, Any] = {}
        self.realtime_price: dict             = defaultdict(list)
        self.quote_periods: dict              = defaultdict(set)
        self.orders_store: dict               = {}
        self.assets_raw: Any       = None
        self.settings_raw: Any     = None

        self.current_asset:  Optional[str] = None
        self.current_period: Optional[int] = None

        self._ws_client: Optional[BinollaWebSocket] = None
        self._ws_thread  = None
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self.last_message_at: float = time.time()

        # Rate-limit state
        self._ws_send_times: List[float] = []
        self._ws_min_interval: float     = 0.015
        self._ws_max_per_window: int     = 100
        self._ws_window: float           = 1.0
        self._throttle: float            = 1.0
        self._last_send_at: float        = 0.0

    # ------------------------------------------------------------------
    # Rate limiting (identical to pyquotex)
    # ------------------------------------------------------------------
    def _apply_rate_limit(self) -> None:
        now = time.time()
        mi  = self._ws_min_interval * self._throttle
        if self._last_send_at > 0:
            elapsed = now - self._last_send_at
            if elapsed < mi:
                time.sleep(mi - elapsed)
                now = time.time()
        win    = self._ws_window * self._throttle
        cutoff = now - win
        self._ws_send_times = [t for t in self._ws_send_times if t >= cutoff]
        if len(self._ws_send_times) >= self._ws_max_per_window:
            sleep_for = (self._ws_send_times[0] + win) - now
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._ws_send_times.append(time.time())
        self._last_send_at = time.time()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    async def authenticate(self) -> tuple[bool, str]:
        """Run full HTTP login and store the session."""
        login = BinollaLogin(host=self.host, lang=self.lang)
        ok, msg = login.login(self.username, self.password)
        if ok:
            self.session_data = {
                "cookies":    login.cookies_str,
                "token":      login.ssid,
                "uid":        login.uid,
                "user_data":  login.user_data,
                "user_agent": login.user_agent,
            }
            self.state.ssid = login.ssid
            save_session(self.username, self.session_data)
        return ok, msg

    # ------------------------------------------------------------------
    # WebSocket connection
    # ------------------------------------------------------------------
    async def start_websocket(self) -> tuple[bool, str]:
        self.state.init_events()
        self.state.reset_events()
        try:
            self.state._loop = asyncio.get_running_loop()
            self._async_loop = self.state._loop
        except RuntimeError:
            pass

        if not self.state.ssid:
            ok, msg = await self.authenticate()
            if not ok:
                return False, msg

        self.state.init_events()
        self._ws_client = BinollaWebSocket(self, self.state, self.event_registry)
        self._ws_thread  = self._ws_client.start(self._async_loop)

        # Wait for connection open or failure
        evt_conn = self.state.ws_connected_event or asyncio.Event()
        evt_rej  = self.state.auth_rejected_event or asyncio.Event()
        evt_err  = self.state.ws_error_event or asyncio.Event()
        evt_cls  = self.state.ws_closed_event or asyncio.Event()

        try:
            idx = await wait_for_first_event(evt_conn, evt_rej, evt_err, evt_cls, timeout=12.0)
        except asyncio.TimeoutError:
            return False, "Timeout waiting for WebSocket to open."

        if idx == 0:
            authed = await self.send_ssid()
            if not authed:
                return False, "Authorization rejected — check credentials."
            return True, "Authenticated."
        if idx == 1:
            self.state.ssid = None
            return False, "Token rejected by server."
        if idx == 2:
            return False, self.state.websocket_error_reason or "WebSocket error."
        return False, "WebSocket closed before auth."

    async def send_ssid(self, timeout: float = 10.0) -> bool:
        """
        Wait for (or force) WebSocket authorization.

        The auth frame is already emitted automatically as soon as the
        engine.io namespace connects (see ``BinollaWebSocket.handle_raw``),
        so normally we only need to await its acceptance. When the auto
        frame was rejected, authenticate again over HTTP and push a new
        authorization frame with the fresh token.
        """
        if not self._ws_client:
            return False
        acc = (self.state.auth_accepted_event or asyncio.Event())
        rej = (self.state.auth_rejected_event or asyncio.Event())
        idx = await wait_for_first_event(acc, rej, timeout=timeout)
        if idx == 0:
            return True

        # rejected → refresh token over HTTP and retry once
        ok, _msg = await self.authenticate()
        if not ok or not self.state.ssid:
            return False
        self.state.init_events()
        self.state.reset_events()
        self._ws_client.send_auth()
        try:
            idx = await wait_for_first_event(acc, rej, timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return idx == 0

    async def connect(self, account_type: AccountType = AccountType.DEMO) -> tuple[bool, str]:
        self.account_type = account_type
        ok, reason = await self.start_websocket()
        if not ok:
            return ok, reason
        authed = await self.send_ssid()
        if not authed:
            ok2, msg2 = await self.authenticate()
            if ok2 and self.state.ssid:
                await self.send_ssid()
        return ok, reason

    async def close(self) -> None:
        if self._ws_client:
            self._ws_client.close()
            await asyncio.sleep(1)
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # WebSocket send wrappers
    # ------------------------------------------------------------------
    def send_request(self, data: str, is_heartbeat: bool = False) -> None:
        """Push a pre-rendered engine.io frame through the socket."""
        if self._ws_client:
            self._ws_client._raw_send(data)

    def subscribe_asset(self, asset: str, period: int) -> None:
        self.current_asset  = asset
        self.current_period = period
        self.realtime_price[asset] = []
        self.realtime_candles[asset] = {}
        self.quote_periods[asset].add(int(period))
        if self._ws_client:
            self._ws_client.subscribe_asset(asset, int(period))

    def request_history(self, asset: str, index: int,
                         fetch_time: int, offset: int, period: int) -> None:
        if self._ws_client:
            self._ws_client.request_history(asset, index, fetch_time, offset, period)

    def change_account(self, account: AccountType) -> None:
        """Switch demo / real account on the socket."""
        self.account_type = account
        if self._ws_client:
            self._ws_client.change_account(account)

    def buy(self, asset: str, direction: str, amount: float,
            period: int) -> int:
        """Place a trade. Returns a local order_id reference."""
        if not self._ws_client:
            raise BinollaConnectionError("WebSocket is not started.")
        return self._ws_client.send_buy(
            asset, direction, amount, period
        )

