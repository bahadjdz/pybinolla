"""
pybinolla/websocket_client.py
==============================
Low-level Socket.IO v4 (Engine.IO) client for Binolla — wire-verified.

Endpoint
--------
    wss://ws3.binolla.com/socket.io/?EIO=4&transport=websocket

   (ws2.binolla.com/ws speaks native JSON arrays and accepts authorization,
    but the quote/candle stream flows through ws3 Socket.IO — exactly what
    the binolla.com/trading web app opens.)

Wire protocol (verified from live frames + platform JS)
-------------------------------------------------------
    Engine.IO handshake : server → 0{"sid":...,"pingInterval":25000,
                                       "pingTimeout":20000}
                          client → 40
                          server → 40{"sid":...}
    Event frame         : 42["event", payload]
    Attachment          : 451-["event",{"_placeholder":true,"num":0}]
                          → next frame (text or binary) carries the payload
    Heartbeat           : server "2" (ping) → client "3" (pong)
    Authorization       : 42["authorization", {"token": <JWT>,
                                               "userAccountType": 0|1,
                                               "uaid": 1|<uid>}]
                          → 42["s_authorization"]

    After auth the server automatically pushes:
        s_assets/list, s_settings/list, s_balances/list, s_history/last

Requests
--------
    Quote/candle subscription :
        42["asset/list/change", [{"asset": "EURUSD_otc", "period": 60}]]
    Historical candles        :
        42["history/region", {"asset":..., "index":<req id>, "time":<now>,
                              "offset":<seconds>, "period":60}]
        → 42/451-["s_history/region", {"asset","index","period","history":[...]}]
          candle row = [time, open, close, high, low, tick, lastTick]
    Live quotes (~3 / second) :
        42["s_quotes/list", [["EURUSD_otc", 1793750472.338, 1.11673, 0], ...]]
          row = [asset, time, price, direction]
    Account change            :
        42["account/change", {"userAccountType": 0|1, "uaid": ...}]
    Trade (verified)          :
        42["orders/open", {"asset": "EURUSD_otc", "amount": 1,
                           "cmd": 0,          # 0 = buy/call, 1 = sell/put
                           "time": 1793754660}] # expiry (unix seconds)
        → 42["s_orders/open", {"deal": {"uuid", "uid", "isDemo", …}}]
        → at expiry: 42["s_orders/close", {"profit", "deals"}]
    demo userAccountType is 1 (real is 0) — verified from the live terminal.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import ssl
import threading
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

import certifi
import websocket
from .connection import ConnectionState, _signal_event
from .enums import AccountType, AuthStatus, WebsocketStatus
from .event_registry import EventRegistry
from .models import Candle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoint / protocol constants (wire-verified)
# ---------------------------------------------------------------------------
BINOLLA_WSS = "wss://ws3.binolla.com/socket.io/?EIO=4&transport=websocket"

EIO_OPEN        = "0"     # server open packet
EIO_CONNECT     = "40"    # client namespace connect / server ack
EIO_EVENT       = "42"    # socket.io event
EIO_EVENT_W_ATT = "451-"  # event with 1 binary attachment (placeholder)
EIO_PING        = "2"
EIO_PONG        = "3"

SSL_OPTS = {
    "check_hostname": True,
    "cert_reqs":      ssl.CERT_REQUIRED,
    "ca_certs":       certifi.where(),
}

_request_counter = itertools.count(int(time.time() * 1000))


def new_request_id() -> int:
    return next(_request_counter)


class BinollaWebSocket:
    """
    Runs the WebSocket inside a daemon thread with automatic reconnect.

    Public API (called by BinollaAPI):
        start(loop)          → spawns the reader thread
        send_auth()          → sends the authorization frame
        subscribe_asset(...)  → asset/list/change
        request_history(...)  → history/region
        change_account(...)
        send_buy(...)
        close()
    """

    def __init__(self,
                 api:            "Any",
                 state:          ConnectionState,
                 event_registry: EventRegistry):
        self.api      = api
        self.state    = state
        self.registry = event_registry

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_lock = threading.Lock()

        self.closed_by_user = False
        self._auth_sent     = False
        self._pending_event: Optional[str] = None
        self._reconnect_delay = 1.0
        # pending orders/open correlation (rid ↔ deal uuid)
        self.trade_uuid_to_rid: Dict[str, int] = {}
        self._pending_buy: Optional[dict] = None
        # correlation map: uuid of an orders/open request → local request id
        self._trade_uuid_index: Dict[str, int] = self.trade_uuid_to_rid  # alias

        # candle storage -------------------------------------------------
        # closed candles : {asset: {candle_time: [open, high, low, close, vol, last_tick]}}
        self.candles: Dict[str, Dict[int, List]] = defaultdict(dict)
        # live (open) candle : {asset: {...}}
        self.live_candle: Dict[str, Dict[str, Any]] = {}

        headers = {
            "User-Agent": api.session_data.get("user_agent") or "Mozilla/5.0",
            "Origin":     f"https://{api.host}",
        }
        cookies = api.session_data.get("cookies") or ""
        self._base_headers = headers
        self._base_cookies = cookies

        self._recreate_ws()

    # ------------------------------------------------------------------
    # WebSocket lifecycle
    # ------------------------------------------------------------------
    def _recreate_ws(self) -> None:
        with self._ws_lock:
            self._ws = websocket.WebSocketApp(
                BINOLLA_WSS,
                header=self._base_headers,
                cookie=self._base_cookies,
                on_open    = self._on_open,
                on_message = self._on_message,
                on_error   = self._on_error,
                on_close   = self._on_close,
            )

    def start(self, loop: Optional[asyncio.AbstractEventLoop]) -> threading.Thread:
        """Spawn the reader thread (with automatic reconnect)."""
        self._loop = loop or self._loop

        def _runner():
            while not self.closed_by_user:
                try:
                    self._ws.run_forever(
                        sslopt=SSL_OPTS,
                        ping_interval=0,          # engine.io heartbeat handled manually (2→3)
                    )
                except Exception as exc:
                    logger.error("[WS] run_forever error: %s", exc)
                if self.closed_by_user:
                    break
                logger.info("[WS] reconnecting in %.1fs …", self._reconnect_delay)
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 30.0)
                self._recreate_ws()
            logger.info("[WS] reader thread stopped.")

        self._thread = threading.Thread(target=_runner, daemon=True,
                                        name="BinollaWSThread")
        self._thread.start()
        return self._thread

    def close(self) -> None:
        self.closed_by_user = True
        with self._ws_lock:
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Callbacks (run in the WS thread)
    # ------------------------------------------------------------------
    def _on_open(self, ws):
        logger.info("[WS] connection open — waiting for engine.io open packet")
        self.state.init_events()
        # keep identical auth state across reconnects for re-auth
        self._auth_sent = False
        self.state.status = WebsocketStatus.CONNECTED
        self.state.signal_connected()

    def _on_message(self, ws, raw: str | bytes):
        try:
            self.handle_raw(raw)
        except Exception as exc:
            logger.exception("[WS] handle_raw failed: %s", exc)

    def _on_error(self, ws, error):
        logger.error("[WS] error: %s", error)
        self.state.status = WebsocketStatus.ERROR
        self.state.websocket_error_reason = str(error)
        self.state.check_websocket_if_error = True

    def _on_close(self, ws, code, msg):
        logger.info("[WS] closed (code=%s)", code)
        self.state.status = WebsocketStatus.DISCONNECTED
        self.state.check_accepted_connection = False
        self._signal(self.state.ws_closed_event)

    # ------------------------------------------------------------------
    # Engine.IO / Socket.IO frame handling
    # ------------------------------------------------------------------
    def handle_raw(self, raw: str | bytes) -> None:
        text = raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else raw

        if text == EIO_PING:                       # engine.io ping
            self._raw_send(EIO_PONG)
            return

        if text.startswith(EIO_OPEN):              # 0{...} → connect namespace
            logger.info("[WS] engine.io open — connecting namespace")
            self._raw_send(EIO_CONNECT)
            return

        if text.startswith(EIO_CONNECT):           # 40 / 40{"sid":...}
            if not self._auth_sent:
                self._auth_sent = True
                self.send_auth()
            return

        if text.startswith(EIO_EVENT_W_ATT):       # 451-[...] placeholder
            try:
                meta = json.loads(text[len(EIO_EVENT_W_ATT):])
                self._pending_event = meta[0] if isinstance(meta, list) else None
            except json.JSONDecodeError:
                self._pending_event = None
            return

        if text.startswith(EIO_EVENT):             # 42[...]
            try:
                parsed = json.loads(text[len(EIO_EVENT):])
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list) and parsed:
                self._dispatch(parsed[0], parsed[1] if len(parsed) > 1 else None)
            self._pending_event = None
            return

        # otherwise → attachment payload for a pending 451- event
        if self._pending_event:
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = text
            self._dispatch(self._pending_event, payload)
            self._pending_event = None

    # ------------------------------------------------------------------
    # Outgoing frames
    # ------------------------------------------------------------------
    def _raw_send(self, text: str) -> None:
        with self._ws_lock:
            if not self._ws:
                return
            try:
                self._ws.send(text)
            except Exception as exc:
                logger.debug("[WS] send failed: %s", exc)

    def emit(self, event: str, payload=None) -> None:
        frame = [event] + ([payload] if payload is not None else [])
        self._raw_send(EIO_EVENT + json.dumps(frame, separators=(",", ":")))

    def send_auth(self, timeout: float = 0.0) -> None:
        """Send the authorization frame (account-type aware).

        Wire-verified: userAccountType 1 = demo, 0 = real.
        """
        account = int(self.api.account_type or AccountType.DEMO)
        uid = int(self.api.session_data.get("uid") or 0)
        if account == int(AccountType.REAL) and uid:
            uaid = uid                 # real → uid
        else:
            uaid = 1                   # demo → 1 (verified from live frames)
        uaid = getattr(self.api, "auth_uaid", None) or uaid
        payload = {
            "token":           self.state.ssid or self.api.session_data.get("token") or "",
            "userAccountType": account,
            "uaid":            uaid,
        }
        self.emit("authorization", payload)
        self.state.auth_status = AuthStatus.AUTHENTICATING

    def subscribe_asset(self, asset: str, period: int) -> None:
        """Start the live quote stream for an asset on a timeframe."""
        self.emit("asset/list/change", [{"asset": asset, "period": int(period)}])

    def request_history(self, asset: str, index: int,
                        fetch_time: int, offset: int, period: int) -> None:
        """Request historical candles (server echoes ``index`` in response)."""
        self.emit("history/region", {
            "asset":  asset,
            "index":  int(index),
            "time":   int(fetch_time),
            "offset": int(offset),
            "period": int(period),
        })

    def change_account(self, account: AccountType) -> None:
        account = int(account)
        uid = int(self.api.session_data.get("uid") or 0)
        uaid = uid if account == int(AccountType.REAL) and uid else (
            self.api.auth_uaid or 1)
        self.emit("account/change", {
            "userAccountType": account,
            "uaid":            uaid,
        })

    def send_buy(self, asset: str, direction: str, amount: float,
                 period: int) -> int:
        """
        Place a By-Time digital-option trade.

        Exact wire payload captured from the real platform browser:
            42["orders/open", {"asset": "EURUSD_otc", "amount": 1,
                                "cmd": 0, "time": 1793754660}]
        cmd: 0 = high / buy (call), 1 = low / sell (put)
        time: expiry as a unix timestamp (seconds)
        Returns a local request id to correlate with s_orders(/open|/close).
        """
        idx = new_request_id()
        is_up = str(direction).lower() in ("call", "higher", "high", "up", "buy")
        expiry = int(time.time()) + int(period)
        payload = {
            "asset":  asset,
            "amount": float(amount),
            "cmd":    0 if is_up else 1,   # 0 = ACHETER/buy, 1 = VENDRE/sell
            "time":   expiry,
        }
        self._pending_buy = {"rid": idx, "asset": asset,
                             "cmd": payload["cmd"], "time": expiry,
                             "at": time.time()}
        self.emit("orders/open", payload)
        return idx

    # ------------------------------------------------------------------
    # Event dispatch (runs in WS thread)
    # ------------------------------------------------------------------
    def _resolve_rid(self, deal: dict, data: dict) -> int:
        """Attach a server event to our pending orders/open request."""
        for key in ("requestId", "uuid"):
            v = data.get(key)
            if v is not None:
                if v in self.trade_uuid_to_rid.values():
                    return int(v)
                if str(v) in self.trade_uuid_to_rid:
                    return self.trade_uuid_to_rid[str(v)]
        for key in ("requestId", "uuid"):
            v = deal.get(key) if isinstance(deal, dict) else None
            if v is not None:
                if str(v) in self.trade_uuid_to_rid:
                    return self.trade_uuid_to_rid[str(v)]
        # timeout-based fallback: the freshest pending buy
        pend = self._pending_buy
        if pend and time.time() - pend.get("at", 0) < 30:
            return int(pend.get("rid") or 0)
        return 0

    def _add_candle_row(self, asset: str, row: list) -> Optional[Candle]:
        """Store a candle row [time, open, close, high, low, tick, lastTick]."""
        try:
            if len(row) < 5:
                return None
            t  = int(float(row[0]))
            o  = float(row[1])
            c  = float(row[2])
            h  = float(row[3])
            l  = float(row[4])
            vol = int(float(row[5])) if len(row) > 5 and row[5] is not None else 0
            last = row[6] if len(row) > 6 and row[6] is not None else t
            store = self.candles[asset]
            existing = store.get(t)
            if existing:
                existing[1] = max(existing[1], h)
                existing[2] = min(existing[2], l)
                existing[3] = c
                existing[4] = max(existing[4], vol)
            else:
                store[t] = [o, h, l, c, vol, last]
            return Candle(time=t, open=o, high=h, low=l, close=c, volume=vol)
        except Exception:
            return None

    def _update_live_quotes(self, rows) -> None:
        """Update per-asset live OHLC aggregation from s_quotes/list rows."""
        if not isinstance(rows, list):
            return
        for q in rows:
            try:
                asset, ts, price = q[0], float(q[1]), float(q[2])
            except Exception:
                continue
            rates = self.api.quote_periods.get(asset) or {60}
            for period in rates:
                bucket = int(ts) // period * period
                current = self.live_candle.get(asset)
                if current is not None and current["time"] != bucket:
                    self._close_live_candle(asset, period)
                    current = self.live_candle.get(asset)
                if current is None or current["time"] != bucket:
                    self.live_candle[asset] = {
                        "time": bucket, "open": price, "high": price,
                        "low": price, "close": price, "volume": 1, "last_tick": ts,
                    }
                else:
                    current["high"]  = max(current["high"], price)
                    current["low"]   = min(current["low"], price)
                    current["close"] = price
                    current["volume"] += 1
                    current["last_tick"] = ts
                # expose through the high-level api surface
                self.api.realtime_candles[asset] = dict(self.live_candle[asset])
                self.api.realtime_price[asset].append(price)

    def _close_live_candle(self, asset: str, period: int) -> None:
        current = self.live_candle.get(asset)
        if not current:
            return
        store = self.candles[asset]
        t = int(current["time"])
        row = [current["open"], current["high"], current["low"],
               current["close"], current["volume"], current["last_tick"]]
        existing = store.get(t)
        if existing:
            existing[1] = max(existing[1], row[1])
            existing[2] = min(existing[2], row[2])
            existing[4] = max(existing[4], row[4])
            existing[5] = row[5]
        else:
            store[t] = row
        candle = Candle(time=t, open=row[0], high=row[1], low=row[2],
                        close=row[3], volume=int(row[4]))
        self._fire(f"candle_closed_{asset}", candle)
        self.live_candle[asset] = None

    # -- registry bridge -------------------------------------------------
    def _signal(self, event: Optional[asyncio.Event]) -> None:
        _signal_event(event, self._loop)

    def _fire(self, key: str, data: Any) -> None:
        """Set a key on the async event registry from the WS thread."""
        if self._loop is None or self._loop.is_closed():
            return
        try:
            asyncio.run_coroutine_threadsafe(self.registry.set(key, data), self._loop)
        except Exception:
            pass

    # -- main dispatcher -------------------------------------------------
    def _dispatch(self, event: str, payload: Any) -> None:
        api = self.api
        logger.debug("[WS] ← %s %s", event, str(payload)[:80])

        if event == "s_authorization":
            logger.info("[WS] authenticated")
            self.state.auth_status = AuthStatus.AUTHENTICATED
            self.state.check_accepted_connection = True
            self.state.check_rejected_connection = False
            self.state.signal_auth_accepted()

        elif event == "f_authorization":
            data = payload if isinstance(payload, dict) else {"raw": payload}
            logger.error("[WS] authorization rejected: %s", data)
            self.state.auth_status = AuthStatus.FAILED
            self.state.check_rejected_connection = True
            self.state.signal_auth_rejected()
            self._fire("auth_failed", data)

        elif event == "s_history/region":
            if not isinstance(payload, dict):
                return
            asset  = payload.get("asset") or api.current_asset or ""
            index  = payload.get("index")
            rows   = payload.get("candles") or payload.get("history") or []
            candles = [c for c in (self._add_candle_row(asset, r) for r in rows)
                       if c is not None]
            api.candle_v2_data[asset] = payload
            self._fire(f"candles_ready_{asset}", candles)
            if index is not None:
                self._fire(f"candles_ready_{asset}_{index}", candles)

        elif event == "s_history/last":
            if not isinstance(payload, dict):
                return
            asset = payload.get("asset") or api.current_asset or ""
            rows  = payload.get("candles") or []
            for r in rows:
                self._add_candle_row(asset, r)

        elif event == "s_quotes/list":
            self._update_live_quotes(payload)

        elif event == "s_assets/list":
            api.assets_raw = payload
            self._fire("assets_ready", payload)

        elif event == "s_balances/list":
            api.account_balance = payload if isinstance(payload, dict) else {}
            self._fire("balance_ready", api.account_balance)

        elif event == "s_settings/list":
            api.settings_raw = payload

        elif event == "s_orders/open":
            data = payload if isinstance(payload, dict) else {"raw": payload}
            deal = data.get("deal") if isinstance(data.get("deal"), dict) else data
            uuid = deal.get("uuid") or ""
            rid  = self._resolve_rid(deal, data)
            if uuid and rid:
                self.trade_uuid_to_rid[uuid] = rid
            api.orders_store[rid or uuid or "last_open"] = deal
            if rid:
                self._fire(f"trade_open_{rid}", deal)

        elif event == "s_orders/close":
            data = payload if isinstance(payload, dict) else {"raw": payload}
            deals = data.get("deals") or [data.get("deal") or data]
            for deal in deals:
                if not isinstance(deal, dict):
                    continue
                uuid = deal.get("uuid") or ""
                rid  = self.trade_uuid_to_rid.get(uuid) or self._resolve_rid(deal, data)
                api.orders_store[rid or uuid or "last_close"] = deal
                if rid:
                    self._fire(f"trade_result_{rid}", deal)

        elif event == "s_balance/update":
            if isinstance(payload, dict):
                bal = self.api.account_balance or {}
                if payload.get("isDemo"):
                    bal["demoBalance"] = payload.get("balance", bal.get("demoBalance", 0))
                else:
                    bal["liveBalance"] = payload.get("balance", bal.get("liveBalance", 0))
                self.api.account_balance = bal
                self._fire("balance_ready", bal)
                self._fire("balance_update", bal)

        elif event == "f_orders/open":
            data = payload if isinstance(payload, dict) else {"raw": payload}
            err = data.get("error")
            rid = self._resolve_rid(data, data)
            api.orders_store[rid or "last_failed"] = {"accepted": data,
                                                      "error": err}
            if rid:
                self._fire(f"trade_result_{rid}", {"error": err,
                                                   "rejected": True})

        elif event.startswith("f_") and event != "f_authorization":
            logger.warning("[WS] server failed event %s: %s", event, str(payload)[:120])

        # s_connection / s_version / s_asset/change ... → informational only
