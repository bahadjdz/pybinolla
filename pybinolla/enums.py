"""
pybinolla/enums.py
==================
Enumerations used throughout the library.

Wire-verified values (from live platform frames):
    userAccountType: 0 → demo/training account
    userAccountType: 1 → real account
"""

from enum import IntEnum, Enum


class Direction(str, Enum):
    """Trade direction."""
    CALL = "call"   # Price goes UP
    PUT  = "put"    # Price goes DOWN
    HIGHER = "call"
    LOWER  = "put"


class AccountType(IntEnum):
    """
    Account mode — value matches the wire field ``userAccountType``.

    Wire-verified from live frames (browser + python):
        1 → demo account
        0 → real account
    """
    DEMO = 1
    REAL = 0


class Period(IntEnum):
    """Candle / trade period in seconds."""
    S5   = 5
    S10  = 10
    S15  = 15
    S30  = 30
    M1   = 60
    M2   = 120
    M3   = 180
    M5   = 300
    M10  = 600
    M15  = 900
    M30  = 1800
    H1   = 3600
    H4   = 14400
    D1   = 86400


class WebsocketStatus(IntEnum):
    DISCONNECTED = 0
    CONNECTED    = 1
    CONNECTING   = 2
    ERROR        = -1


class AuthStatus(IntEnum):
    NOT_AUTHENTICATED = 0
    AUTHENTICATING    = 1
    AUTHENTICATED     = 2
    FAILED            = -1
