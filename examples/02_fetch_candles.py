"""
Example 02: Fetch Historical Candles
=====================================
Demonstrates how to fetch historical M1 candles for an OTC asset
(e.g., EURUSD_otc) and inspect Open/High/Low/Close data.
"""

import asyncio
from pybinolla import Binolla, Period

EMAIL = "YOUR_EMAIL@example.com"
PASSWORD = "YOUR_PASSWORD"
ASSET = "EURUSD_otc"


async def main():
    client = Binolla(email=EMAIL, password=PASSWORD)

    ok, reason = await client.connect()
    if not ok:
        print(f"Connection failed: {reason}")
        return

    print(f"Fetching 100 historical M1 candles for {ASSET}...")
    candles = await client.get_candles(ASSET, count=100, period=Period.M1, timeout=30)

    print(f"Received {len(candles)} candles.")
    if candles:
        first = candles[0]
        last = candles[-1]
        print(f"Oldest Candle: {first.datetime_utc} | Open: {first.open:.5f} | Close: {first.close:.5f}")
        print(f"Newest Candle: {last.datetime_utc} | Open: {last.open:.5f} | Close: {last.close:.5f}")

        print("\nLast 5 Candles:")
        for c in candles[-5:]:
            print(f"  {c.datetime_utc}  O: {c.open:.5f}  H: {c.high:.5f}  L: {c.low:.5f}  C: {c.close:.5f}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())

