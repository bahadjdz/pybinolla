"""
pybinolla/exceptions.py
========================
Custom exceptions for the pybinolla library (Free Community Edition).
"""


class BinollaError(Exception):
    """Base exception for all pybinolla errors."""


class BinollaAuthError(BinollaError):
    """Raised when authentication fails (wrong credentials, token rejected, etc.)."""


class BinollaConnectionError(BinollaError):
    """Raised when the WebSocket connection cannot be established or drops unexpectedly."""


class BinollaTimeoutError(BinollaError):
    """Raised when a server response is not received within the expected time window."""


class BinollaAPIError(BinollaError):
    """Raised when the server returns an unexpected or error response payload."""


class BinollaRateLimitError(BinollaError):
    """Raised when the client is sending messages too fast and gets throttled."""


class BinollaProFeatureError(BinollaError):
    """
    Raised when an action requires pybinolla-pro (Commercial Edition).
    Features include Real account trading, blitz (5s) trades, copy trading, and payout checking.
    """
