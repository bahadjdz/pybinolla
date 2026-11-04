"""
Example 04: Demo Account Trading
=================================
Demonstrates how to place a CALL or PUT trade on the DEMO account,
track the order ID, and wait for the trade settlement result.
"""

import asyncio
from pybinolla import Binolla, Direction, Period

EMAIL = "YOUR_EMAIL@example.com"
PASSWORD = "YOUR_PASSWORD"
ASSET = "EURUSD_otc"
AMOUNT = 1.0  # 1 USD stake on Demo


async def main():
    client = Binolla(email=EMAIL, password=PASSWORD)

    ok, reason = await client.connect()
    if not ok:
        print(f"Connection failed: {reason}")
        return

    await asyncio.sleep(1.0)
    bal_before = await client.get_balance()
    if bal_before:
        print(f"Demo Balance Before: ${bal_before.demo_balance:.2f}")

    # Start live price stream for settlement accuracy
    await client.start_stream(ASSET, Period.M1)

    print(f"\nPlacing DEMO Trade: {ASSET} | Direction: CALL | Amount: ${AMOUNT:.2f} | Period: M1")
    order_id = await client.buy(
        asset=ASSET,
        direction=Direction.CALL,
        amount=AMOUNT,
        period=Period.M1,
    )
    print(f"Order submitted! Order Reference: {order_id}")

    print("Waiting for settlement (expiry)...")
    result = await client.wait_for_result(order_id, timeout=120)

    if result:
        outcome = "WIN 🎉" if result.win else "LOSS ❌"
        print(f"\nTrade Result: {outcome}")
        print(f"Profit/Loss : ${result.profit:+.2f}")
        print(f"Close Price : {result.close_price:.5f}")
    else:
        print("Result wait timed out.")

    bal_after = await client.get_balance()
    if bal_after:
        print(f"\nDemo Balance After: ${bal_after.demo_balance:.2f}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())

