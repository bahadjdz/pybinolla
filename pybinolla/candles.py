"""
pybinolla/candles.py
=====================
Candle aggregation helpers.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from .models import Candle


# ---------------------------------------------------------------------------
def group_by_period(ticks: List[Any], period: int) -> Dict[int, list]:
    """Group raw [timestamp, price, ...] ticks into period-aligned buckets."""
    grouped: Dict[int, list] = defaultdict(list)
    for tick in ticks:
        ts   = int(tick[0])
        slot = ts // period
        grouped[slot].append(tick)
    return dict(grouped)


def calculate_candles(ticks: List[Any], period: int) -> List[Candle]:
    """Convert raw tick list into OHLCV candles aligned to the given period."""
    if not isinstance(ticks, list) or not ticks:
        return []
    grouped = group_by_period(ticks, period)
    result  = []
    for slot, slot_ticks in sorted(grouped.items()):
        prices = [float(t[1]) for t in slot_ticks]
        result.append(Candle(
            time   = slot * period,
            open   = prices[0],
            high   = max(prices),
            low    = min(prices),
            close  = prices[-1],
            volume = len(prices),
        ))
    # Drop the last (possibly incomplete) candle
    return result[:-1] if len(result) > 1 else result


def parse_raw_candles(raw_data: Optional[dict]) -> List[Candle]:
    """Parse a server history response dict into a list of Candle objects."""
    if not raw_data or not isinstance(raw_data, dict):
        return []
    rows = (
        raw_data.get("candles") or
        raw_data.get("data")    or
        raw_data.get("history") or
        []
    )
    out = []
    for row in rows:
        try:
            if isinstance(row, list) and len(row) >= 5:
                out.append(Candle.from_list(row))
            elif isinstance(row, dict) and "time" in row:
                out.append(Candle.from_dict(row))
        except Exception:
            continue
    return out


def merge_candles(lists: List[List[Candle]]) -> List[Candle]:
    """Merge multiple candle lists, deduplicate by time, sort ascending."""
    seen: Dict[int, Candle] = {}
    for lst in lists:
        for c in lst:
            seen[c.time] = c
    return sorted(seen.values(), key=lambda c: c.time)

