"""
Example 03: Real-Time Price Streaming
======================================
Demonstrates how to subscribe to live quotes for an asset and track
tick updates and open candle updates in real time.
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

    print(f"Subscribing to live price stream for {ASSET} (M1 period)...")
    await client.start_stream(ASSET, period=Period.M1)

    print("Listening to price updates for 15 seconds. Press Ctrl+C to stop.\n")
    for second in range(1, 16):
        await asyncio.sleep(1.0)
        candle = await client.get_realtime_candle(ASSET)
        if candle and candle.get("close"):
            print(
                f"[{second:02d}s] {ASSET} -> "
                f"Open: {candle['open']:.5f} | "
                f"High: {candle['high']:.5f} | "
                f"Low: {candle['low']:.5f} | "
                f"Close: {candle['close']:.5f} | "
                f"Ticks: {candle.get('volume', 0)}"
            )
        else:
            print(f"[{second:02d}s] Waiting for first tick...")

    await client.close()
    print("\nStream closed.")


if __name__ == "__main__":
    asyncio.run(main())

