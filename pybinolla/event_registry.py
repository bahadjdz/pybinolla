"""
pybinolla/event_registry.py
============================
Async key-value event store that lets the WebSocket message handler
resolve awaiting coroutines by name (e.g. "candles_ready_EURUSD").
Identical pattern to pyquotex's EventRegistry.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional


class EventRegistry:
    """Thread-safe, async event bus keyed by string identifiers."""

    def __init__(self):
        self._events: Dict[str, asyncio.Event] = {}
        self._data:   Dict[str, Any]           = {}
        self._lock  = asyncio.Lock()

    async def _ensure(self, key: str) -> asyncio.Event:
        async with self._lock:
            if key not in self._events:
                self._events[key] = asyncio.Event()
            return self._events[key]

    async def set(self, key: str, data: Any = None) -> None:
        """Signal the event and store its associated data."""
        async with self._lock:
            if key not in self._events:
                self._events[key] = asyncio.Event()
            self._data[key] = data
            self._events[key].set()

    async def wait(self, key: str, timeout: float = 30.0) -> Optional[Any]:
        """Block until the event is set or timeout expires. Returns stored data."""
        event = await self._ensure(key)
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return self._data.get(key)
        except asyncio.TimeoutError:
            return None

    async def clear(self, key: str) -> None:
        """Reset the event so it can be awaited again."""
        async with self._lock:
            if key in self._events:
                self._events[key].clear()
            self._data.pop(key, None)

