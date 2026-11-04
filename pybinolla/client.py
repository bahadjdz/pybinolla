"""
pybinolla/client.py
====================
Binolla — high-level public API.
This is the class users import and interact with.
"""

from __future__ import annotations

import asyncio
import calendar
import itertools
import time
import logging
from typing import Any, Callable, List, Optional

from .api import BinollaAPI, _request_counter
from .candles import calculate_candles, parse_raw_candles, merge_candles
from .connection import wait_until
from .enums import AccountType, Direction, Period
from .exceptions import (
    BinollaConnectionError,
    BinollaTimeoutError,
    BinollaProFeatureError,
)
from .http_client import load_session
from .models import Balance, Candle, Order, OrderResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
FETCH_CHUNK_SIZE  = 200   # candles per history batch
FETCH_BATCH_DELAY = 0.10  # seconds between batches
FETCH_MAX_WORKERS = 4     # parallel history workers


class Binolla:
    """
    High-level async client for Binolla.

    Example
    -------
    .. code-block:: python

        import asyncio
        from pybinolla import Binolla, Direction, Period

        async def main():
            client = Binolla(email="you@example.com", password="secret")
            ok, reason = await client.connect()
            if not ok:
                print("Connection failed:", reason)
                return

            candles = await client.get_candles("EURUSD_otc", count=100,
                                               period=Period.M1)
            print(candles[-1])

            order_id = await client.buy("EURUSD_otc", Direction.CALL,
                                        amount=1.0, period=Period.M1)
            result = await client.wait_for_result(order_id, timeout=90)
            print(result.outcome, result.profit)

            await client.close()

        asyncio.run(main())
    """

    def __init__(self,
                 email:     str = "",
                 password:  str = "",
                 host:      str = "binolla.com",
                 lang:      str = "en",
                 proxies:   Optional[dict] = None):
        self.email    = email
        self.password = password
        self.host     = host
        self.lang     = lang
        self.proxies  = proxies

        self._api: Optional[BinollaAPI] = None
        self._account_type = AccountType.DEMO
        self._session_data = load_session(email)

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------
    async def connect(self,
                      account: AccountType = AccountType.DEMO
                      ) -> tuple[bool, str]:
        """
        Authenticate and open the WebSocket connection.

        Parameters
        ----------
        account : AccountType
            ``AccountType.DEMO`` (default) or ``AccountType.REAL``.

        Returns
        -------
        (success, reason_string)
        """
        if account == AccountType.REAL:
            raise BinollaProFeatureError(
                "Real account trading is exclusively available in pybinolla-pro.\n"
                "The Free Community Edition supports DEMO account trading only.\n"
                "To upgrade to Pro, contact on Telegram: https://t.me/llldevl or visit https://github.com/bahadjdz/pybinolla"
            )
        self._account_type = account
        self._api = BinollaAPI(
            host     = self.host,
            username = self.email,
            password = self.password,
            lang     = self.lang,
            proxies  = self.proxies,
        )
        self._api.session_data = self._session_data
        self._api.state.ssid   = self._session_data.get("token")
        self._api._async_loop  = asyncio.get_running_loop()

        if not self._session_data.get("token"):
            ok, msg = await self._api.authenticate()
            if not ok:
                return False, msg

        ok, reason = await self._api.connect(account)
        if not ok:
            # Stale/expired saved token → fresh login and one reconnect retry
            logger.info("[Binolla] reconnect retry after: %s", reason)
            self._session_data = {}
            ok, msg = await self._api.authenticate()
            if not ok:
                return False, msg
            self._api.session_data = self._session_data
            self._api.state.ssid   = self._api.session_data.get("token")
            await self._api.close()
            await asyncio.sleep(1.5)
            self._api._async_loop = asyncio.get_running_loop()
            ok, reason = await self._api.connect(account)
        return ok, reason

    async def close(self) -> None:
        """Gracefully close the WebSocket connection."""
        if self._api:
            await self._api.close()

    async def is_connected(self) -> bool:
        """Return True if the WebSocket session is authenticated and alive."""
        if self._api is None:
            return False
        try:
            await wait_until(
                lambda: self._api.state.check_accepted_connection,
                timeout=2.0,
            )
            return True
        except asyncio.TimeoutError:
            return self._api.state.check_accepted_connection

    # ------------------------------------------------------------------
    # Account
    # ------------------------------------------------------------------
    async def change_account(self, account: AccountType) -> None:
        """Switch between demo and real balance."""
        if account == AccountType.REAL:
            raise BinollaProFeatureError(
                "Real account trading is exclusively available in pybinolla-pro.\n"
                "The Free Community Edition supports DEMO account trading only.\n"
                "To upgrade to Pro, contact on Telegram: https://t.me/llldevl or visit https://github.com/bahadjdz/pybinolla"
            )
        if self._api:
            self._account_type = account
            self._api.change_account(account)

    async def get_balance(self) -> Optional[Balance]:
        """Return the latest account balance snapshot."""
        if self._api is None:
            return None
        raw = self._api.account_balance
        if raw is None:
            return None
        return Balance(
            demo_balance = float(raw.get("demoBalance", 0)),
            live_balance = float(raw.get("liveBalance", 0)),
            currency     = raw.get("currency", "USD"),
            account_type = int(self._account_type),
        )

    # ------------------------------------------------------------------
    # Candle data
    # ------------------------------------------------------------------
    async def start_stream(self, asset: str,
                           period: int = Period.M1) -> None:
        """Subscribe to real-time candle / tick updates for an asset."""
        if self._api:
            self._api.subscribe_asset(asset, period)

    async def get_realtime_candle(self, asset: str) -> Optional[Any]:
        """Return the latest raw real-time candle data for an asset."""
        if self._api:
            return self._api.realtime_candles.get(asset)
        return None

    async def get_candles(self,
                          asset:   str,
                          count:   int = 100,
                          period:  int = Period.M1,
                          timeout: float = 30.0
                          ) -> List[Candle]:
        """
        Fetch a batch of historical candles.

        Parameters
        ----------
        asset   : symbol, e.g. ``"EURUSD_otc"``
        count   : how many candles you need (approximate)
        period  : candle period in seconds (use ``Period.*`` constants)
        timeout : seconds to wait for the server response
        """
        if self._api is None:
            return []
        end_time = int(time.time())
        offset   = period * count
        index    = calendar.timegm(time.gmtime())

        await self._api.event_registry.clear(f"candles_ready_{asset}")
        await self.start_stream(asset, period)
        self._api.request_history(asset, index, end_time, offset, period)

        raw = await self._api.event_registry.wait(
            f"candles_ready_{asset}", timeout=timeout
        )
        if raw is None:
            return []
        # dispatch already returns parsed Candle objects for a list payload
        if isinstance(raw, list):
            return [c for c in raw if isinstance(c, Candle)]
        return parse_raw_candles(raw)

    async def get_historical_candles(self,
                                     asset:             str,
                                     amount_of_seconds: int,
                                     period:            int = Period.M1,
                                     timeout:           float = 30.0,
                                     max_workers:       int = FETCH_MAX_WORKERS,
                                     progress_callback: Optional[Callable] = None
                                     ) -> List[Candle]:
        """
        Fetch a large historical range using parallel workers.

        Parameters
        ----------
        asset             : e.g. "EURUSD_otc"
        amount_of_seconds : total lookback window (e.g. 86400 = 1 day)
        period            : candle period in seconds
        timeout           : per-batch wait timeout
        max_workers       : parallel batch workers (default 4)
        progress_callback : optional fn(progress, total, count, label)
        """
        chunk_s      = period * FETCH_CHUNK_SIZE
        current_time = int(time.time())
        target_start = current_time - amount_of_seconds
        block_size   = amount_of_seconds // max(max_workers, 1)
        semaphore    = asyncio.Semaphore(max_workers)
        all_candles: dict = {}

        async def worker(start_t: int, end_t: int, wid: int):
            worker_data: dict = {}
            async with semaphore:
                oldest = start_t
                fails  = 0
                while oldest > end_t:
                    if not self._api or not self._api.state.check_accepted_connection:
                        break
                    idx   = next(_request_counter)
                    key   = f"candles_ready_{asset}_{idx}"
                    await self._api.event_registry.clear(key)
                    self._api.request_history(asset, idx, oldest, chunk_s, period)
                    raw = await self._api.event_registry.wait(key, timeout=timeout)
                    batch: List[Candle]
                    if isinstance(raw, list):
                        batch = [c for c in raw if isinstance(c, Candle)]
                    elif isinstance(raw, dict):
                        batch = parse_raw_candles(raw)
                    else:
                        batch = []
                    if not batch:
                        oldest -= chunk_s
                        fails  += 1
                        if fails >= 3:
                            break
                        await asyncio.sleep(FETCH_BATCH_DELAY * 2)
                        continue
                    fails   = 0
                    in_range = [c for c in batch if end_t <= c.time <= start_t]
                    if not in_range:
                        oldest -= chunk_s
                        continue
                    for c in in_range:
                        worker_data[c.time] = c
                    new_oldest = min(c.time for c in in_range)
                    if progress_callback:
                        progress_callback(
                            start_t - new_oldest,
                            start_t - end_t,
                            len(worker_data),
                            f"Worker-{wid}",
                        )
                    oldest = new_oldest if new_oldest < oldest else oldest - chunk_s
                    await asyncio.sleep(FETCH_BATCH_DELAY)
            return list(worker_data.values())

        await self.start_stream(asset, period)
        tasks = []
        for i in range(max_workers):
            s = current_time - (i * block_size)
            e = max(target_start, s - block_size)
            tasks.append(worker(s, e, i))

        results = await asyncio.gather(*tasks)
        for batch in results:
            for c in batch:
                all_candles[c.time] = c
        return sorted(all_candles.values(), key=lambda c: c.time)

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------
    async def buy(self,
                  asset:     str,
                  direction: Direction,
                  amount:    float,
                  period:    int = Period.M1
                  ) -> int:
        """
        Place a digital options trade.

        Parameters
        ----------
        asset     : symbol, e.g. ``"EURUSD_otc"``
        direction : ``Direction.CALL`` (up) or ``Direction.PUT`` (down)
        amount    : stake in account currency
        period    : expiry duration in seconds

        Returns
        -------
        order_id : local reference for checking the result
        """
        if self._api is None:
            raise BinollaConnectionError("Not connected. Call connect() first.")
        if self._account_type == AccountType.REAL:
            raise BinollaProFeatureError(
                "Real account trading is exclusively available in pybinolla-pro.\n"
                "The Free Community Edition supports DEMO account trading only.\n"
                "To upgrade to Pro, contact on Telegram: https://t.me/llldevl or visit https://github.com/bahadjdz/pybinolla"
            )
        return self._api.buy(
            asset     = asset,
            direction = direction.value,
            amount    = amount,
            period    = int(period),
        )

    async def wait_for_result(self,
                              order_id: int,
                              timeout:  float = 180.0
                              ) -> Optional[OrderResult]:
        """
        Wait asynchronously for the settlement result of a placed order.

        Parameters
        ----------
        order_id : the int returned by :meth:`buy`
        timeout : maximum seconds to wait (expiry included)

        Returns
        -------
        OrderResult or None if the result did not arrive in time.
        """
        if self._api is None:
            return None
        raw = await self._api.event_registry.wait(
            f"trade_result_{order_id}", timeout=timeout
        )
        if raw is None:
            return None
        if not isinstance(raw, dict) or raw.get("rejected"):
            return OrderResult(
                order_id    = order_id,
                profit      = 0.0,
                win         = False,
                close_price = 0.0,
                close_time  = int(time.time()),
            )

        def _f(*keys, default=0.0):
            for k in keys:
                if raw.get(k) is not None:
                    try:
                        return float(raw[k])
                    except (TypeError, ValueError):
                        pass
            return default

        def _t(*keys, default=None):
            for k in keys:
                v = raw.get(k)
                if v is not None:
                    if isinstance(v, (int, float)):
                        return int(v)
                    try:
                        return int(float(str(v).replace("Z", "")) )
                    except ValueError:
                        pass
            return default or int(time.time())

        profit      = _f("profit", "pnl", "amount2")
        close_price = _f("closePrice", "close_price", "close")
        close_time  = _t("closeTimeUTC", "closeTime", "close_time", "timestamp")
        if close_time < 10_000_000:
            close_time = int(time.time())
        return OrderResult(
            order_id    = order_id,
            profit      = profit,
            win         = profit > 0,
            close_price = close_price,
            close_time  = close_time,
        )

