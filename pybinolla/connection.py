"""
pybinolla/connection.py
========================
WebSocket state machine + event primitives.
Mirrors pyquotex's ConnectionState + event-driven wait helpers.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .enums import WebsocketStatus, AuthStatus

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thread-safe event bridge helper
# ---------------------------------------------------------------------------
def _signal_event(event: Optional[asyncio.Event],
                  loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """Schedule event.set() from a non-async (websocket callback) thread."""
    if event is None or loop is None:
        return
    try:
        if not loop.is_closed() and loop.is_running():
            loop.call_soon_threadsafe(event.set)
    except RuntimeError:
        pass


# ---------------------------------------------------------------------------
# Wait helpers (identical pattern to pyquotex)
# ---------------------------------------------------------------------------
async def wait_until(predicate, *, timeout: float = 10.0,
                     poll_interval: float = 0.05) -> None:
    """Spin until predicate() is True or timeout expires."""
    async def _loop():
        while not predicate():
            await asyncio.sleep(poll_interval)
    await asyncio.wait_for(_loop(), timeout=timeout)


async def wait_for_first_event(*events: asyncio.Event,
                                timeout: float = 10.0) -> int:
    """
    Await whichever asyncio.Event fires first.
    Returns the zero-based index of the winning event.
    Raises asyncio.TimeoutError if none fire within timeout.
    """
    if not events:
        raise ValueError("At least one event is required.")
    tasks = [asyncio.ensure_future(e.wait()) for e in events]
    try:
        done, pending = await asyncio.wait(
            tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        for i, t in enumerate(tasks):
            if t in done and not t.cancelled():
                return i
        raise asyncio.TimeoutError()
    except asyncio.CancelledError:
        for t in tasks:
            if not t.done():
                t.cancel()
        raise


# ---------------------------------------------------------------------------
# Connection state holder
# ---------------------------------------------------------------------------
class ConnectionState:
    """Mutable connection state shared between the WebSocket thread and asyncio."""

    def __init__(self):
        self.status:      WebsocketStatus = WebsocketStatus.DISCONNECTED
        self.auth_status: AuthStatus      = AuthStatus.NOT_AUTHENTICATED
        self.ssid:        Optional[str]   = None

        # Legacy boolean flags (kept for compatibility)
        self.check_accepted_connection: bool = False
        self.check_rejected_connection: bool = False
        self.check_websocket_if_error:  bool = False
        self.websocket_error_reason:    Optional[str] = None

        # Mutual exclusion flags (same as pyquotex)
        self.ssl_Mutual_exclusion:       bool = False
        self.ssl_Mutual_exclusion_write: bool = False

        # asyncio events (initialised in init_events)
        self.ws_connected_event:   Optional[asyncio.Event] = None
        self.ws_closed_event:      Optional[asyncio.Event] = None
        self.auth_accepted_event:  Optional[asyncio.Event] = None
        self.auth_rejected_event:  Optional[asyncio.Event] = None
        self.ws_error_event:       Optional[asyncio.Event] = None

        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    def init_events(self) -> None:
        """
        Create the asyncio Events once. Safe to call from the WS thread:
        there is no running loop there, so a previously-captured loop is
        preserved instead of being overwritten with None.
        """
        for attr in ("ws_connected_event", "ws_closed_event",
                     "auth_accepted_event", "auth_rejected_event",
                     "ws_error_event"):
            if getattr(self, attr) is None:
                setattr(self, attr, asyncio.Event())
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            # called from the WS thread — keep the caller's loop
            pass

    def reset_events(self) -> None:
        for attr in ("ws_connected_event", "ws_closed_event",
                     "auth_accepted_event", "auth_rejected_event",
                     "ws_error_event"):
            ev = getattr(self, attr)
            if ev is not None:
                ev.clear()

    # ------------------------------------------------------------------
    def signal_connected(self)     -> None: _signal_event(self.ws_connected_event,  self._loop)
    def signal_closed(self)        -> None: _signal_event(self.ws_closed_event,      self._loop)
    def signal_auth_accepted(self) -> None: _signal_event(self.auth_accepted_event,  self._loop)
    def signal_auth_rejected(self) -> None: _signal_event(self.auth_rejected_event,  self._loop)
    def signal_error(self)         -> None: _signal_event(self.ws_error_event,        self._loop)

