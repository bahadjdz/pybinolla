"""
pybinolla/models.py
====================
Data-class models representing the core domain objects returned by the API.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Candle:
    """A single OHLCV candlestick."""
    time:     int    # Unix timestamp (seconds, aligned to period)
    open:     float
    high:     float
    low:      float
    close:    float
    volume:   int = 0

    @property
    def datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(self.time, tz=timezone.utc)

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open

    def to_dict(self) -> dict:
        return {
            "time":     self.time,
            "datetime": self.datetime_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "open":     self.open,
            "high":     self.high,
            "low":      self.low,
            "close":    self.close,
            "volume":   self.volume,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Candle":
        return cls(
            time   = int(d.get("time", d.get("timestamp", 0))),
            open   = float(d.get("open",  0)),
            high   = float(d.get("high",  d.get("max", 0))),
            low    = float(d.get("low",   d.get("min", 0))),
            close  = float(d.get("close", 0)),
            volume = int(d.get("volume",  d.get("ticks", 0))),
        )

    @classmethod
    def from_list(cls, row: list) -> "Candle":
        """Parse a raw list [time, open, close, high, low, ...]."""
        return cls(
            time  = int(float(row[0])),
            open  = float(row[1]),
            close = float(row[2]),
            high  = float(row[3]),
            low   = float(row[4]),
            volume= int(row[5]) if len(row) > 5 else 0,
        )


@dataclass
class Balance:
    """Account balance snapshot.

    ``account_type`` follows the wire value: 0 = demo, 1 = real.
    """
    demo_balance: float
    live_balance: float
    currency:     str = "USD"
    account_type: int = 0   # 0 = demo, 1 = real

    @property
    def active(self) -> float:
        return self.demo_balance if self.account_type == 0 else self.live_balance


@dataclass
class Asset:
    """A tradeable asset on Binolla."""
    symbol:      str          # e.g. "EURUSD_otc"
    name:        str = ""     # Display name
    payout:      float = 0.0  # e.g. 0.92 → 92 %
    is_open:     bool = True
    is_otc:      bool = False
    min_amount:  float = 1.0
    max_amount:  float = 20000.0

    def __post_init__(self):
        if self.symbol.upper().endswith("_OTC"):
            self.is_otc = True
        if not self.name:
            self.name = self.symbol


@dataclass
class Order:
    """A placed trade / digital option order."""
    order_id:   str
    asset:      str
    direction:  str      # "call" | "put"
    amount:     float
    period:     int      # seconds
    open_time:  int      # Unix timestamp
    open_price: float
    is_demo:    bool = True
    expiry_time: int = 0

    @property
    def datetime_open(self) -> datetime:
        return datetime.fromtimestamp(self.open_time, tz=timezone.utc)


@dataclass
class OrderResult:
    """Final settlement result of a placed order."""
    order_id:    str
    profit:      float   # positive = win, negative = loss
    win:         bool
    close_price: float
    close_time:  int

    @property
    def outcome(self) -> str:
        return "WIN" if self.win else "LOSS"

