"""
pybinolla - Unofficial Python API for Binolla Trading Platform
==============================================================
Free Community Edition.
A fully async, event-driven client library for Binolla (binolla.com).

License : MIT
"""

from .client import Binolla
from .models import Candle, Balance, Order, OrderResult, Asset
from .enums import Direction, AccountType, Period
from .exceptions import (
    BinollaError,
    BinollaAuthError,
    BinollaConnectionError,
    BinollaTimeoutError,
    BinollaAPIError,
    BinollaProFeatureError,
)

__version__ = "1.0.0-free"
__all__ = [
    "Binolla",
    "Candle",
    "Balance",
    "Order",
    "OrderResult",
    "Asset",
    "Direction",
    "AccountType",
    "Period",
    "BinollaError",
    "BinollaAuthError",
    "BinollaConnectionError",
    "BinollaTimeoutError",
    "BinollaAPIError",
    "BinollaProFeatureError",
]
